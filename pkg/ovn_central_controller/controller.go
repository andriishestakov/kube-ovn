// controller.go: orchestration of ovn-central startup + runtime loop.
//
// Lifecycle:
//   Run() -> parseConfig -> preflight -> bringUp -> runtimeLoop
//   - preflight: read on-disk state, wipe orphan/kicked/stale-stub DBs.
//   - bringUp:   start ovsdb-server (existing DB resumes natively); on
//                cluster-failure to elect leader, take the bootstrap lease
//                and either reconvert (sole survivor) or create-cluster
//                (true fresh bootstrap), based on what kubectl says about
//                peer state.
//   - runtimeLoop: single ticker dispatching watchdog (no-leader -> fatal)
//                  + kicker (leader-only) + header backup + annotation sync.
package ovn_central_controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// errRetry is returned by bringUp / recover when this attempt didn't fail
// fatally but there's nothing useful for us to do right now (e.g. someone
// else holds the bootstrap lease, or peers are still spinning up). Run()
// catches it, sleeps briefly, and re-enters preflight + bringUp without
// going through a kubelet restart cycle.
var errRetry = errors.New("retry preflight/bringUp")

// lifecycleState tracks two facts about THIS container's lifetime that
// can't be derived from disk alone:
//   - dbInherited: is the current on-disk DB inherited from a prior
//     container (started true when DB existed at boot; cleared by wipe
//     or initStub which creates a fresh DB in this container)?
//     Differentiates "we have data from a prior life" (statusStale) from
//     "we just initStub-joined a fresh stub" (statusJoining).
//   - wasActive: did we ever observe cluster_member + known leader in
//     this container? Differentiates "had data, lost leader" (leader_lost)
//     from "have data but never confirmed sync" (stale). Sticky in-memory
//     for this container's life: a brief leader-unknown blip should leave
//     us at leader_lost, not silently downgrade to stale.
//
// Single instance in pkg-level `lifecycle` is mutated by the few code
// paths that know about each transition (Run() init, wipeDB, initStub
// fresh-create, computeStatus).
type lifecycleState struct {
	dbInherited bool
	wasActive   bool
}

var lifecycle lifecycleState

// computeStatus derives our current status from on-disk state, ovsdb
// runtime state, and lifecycle flags.
func computeStatus(cfg *Config) status {
	nbExists := readDBState(nbDB(cfg)).exists
	sbExists := readDBState(sbDB(cfg)).exists
	if !nbExists && !sbExists {
		return ""
	}
	nbCS, errNB := readClusterStatus(nbDB(cfg))
	sbCS, errSB := readClusterStatus(sbDB(cfg))
	if errNB == nil && errSB == nil &&
		nbCS.status == "cluster member" && sbCS.status == "cluster member" &&
		isKnownLeader(nbCS.leader) && isKnownLeader(sbCS.leader) {
		lifecycle.wasActive = true
		return statusActive
	}
	if lifecycle.wasActive {
		return statusLeaderLost
	}
	if lifecycle.dbInherited {
		return statusStale
	}
	return statusJoining
}

// Config carries env-var-driven knobs. Fields default-init to zero value;
// parseConfig fills them.
type Config struct {
	PodName, PodNamespace, PodIP string
	DBClusterAddr                string   // raft listen addr (= PodIP)
	DBAddr                       string   // single db addr passed to --db-{nb,sb}-addr
	DBAddresses                  []string // listen addrs for db client port (dual-stack capable)
	EnableSSL                    bool
	NBPort, SBPort               int
	NBClusterPort, SBClusterPort int
	OVNNorthdNThreads            int
	OVNNorthdProbeInterval       int    // ms
	ProbeInterval                int    // ms (NB/SB inactivity_probe)
	OVNVersionCompatibility      string // optional compat tag
	OVNDir                       string // /etc/ovn
	OVNRunDir                    string // /var/run/ovn
	EnableCompact                bool   // periodic ovsdb-server/compact on leader
	DebugWrapper                 string // --ovn-northd-wrapper / --ovsdb-{nb,sb}-wrapper, e.g. valgrind
	TickInterval                 time.Duration
	StaggerTimeout               time.Duration // wait for cluster member+leader
	NoLeaderTimeout              time.Duration // watchdog threshold
	DeadMemberTimeout            time.Duration // kicker threshold
	BackupInterval               time.Duration
	CompactInterval              time.Duration
}

// SSLOptions returns the -p/-c/-C arg list for ovn-nbctl / ovn-sbctl /
// ovsdb-client invocations when SSL is enabled, empty otherwise.
func (c *Config) SSLOptions() []string {
	if !c.EnableSSL {
		return nil
	}
	return []string{
		"-p", "/var/run/tls/key",
		"-c", "/var/run/tls/cert",
		"-C", "/var/run/tls/cacert",
	}
}

func parseConfig() (*Config, error) {
	c := &Config{
		PodName:                 getenv("POD_NAME", ""),
		PodNamespace:            getenv("POD_NAMESPACE", "kube-system"),
		PodIP:                   getenv("POD_IP", ""),
		EnableSSL:               getenv("ENABLE_SSL", "false") == "true",
		NBPort:                  atoi(getenv("NB_PORT", "6641")),
		SBPort:                  atoi(getenv("SB_PORT", "6642")),
		NBClusterPort:           atoi(getenv("NB_CLUSTER_PORT", "6643")),
		SBClusterPort:           atoi(getenv("SB_CLUSTER_PORT", "6644")),
		OVNNorthdNThreads:       atoiDefault(getenv("OVN_NORTHD_N_THREADS", ""), 1),
		OVNNorthdProbeInterval:  atoiDefault(getenv("OVN_NORTHD_PROBE_INTERVAL", ""), 5000),
		ProbeInterval:           atoiDefault(getenv("PROBE_INTERVAL", ""), 180000),
		OVNVersionCompatibility: getenv("OVN_VERSION_COMPATIBILITY", ""),
		OVNDir:                  getenv("OVN_DIR", "/etc/ovn"),
		OVNRunDir:               getenv("OVN_RUN_DIR", "/var/run/ovn"),
		EnableCompact:           getenv("ENABLE_COMPACT", "false") == "true",
		DebugWrapper:            getenv("DEBUG_WRAPPER", ""),
		TickInterval:            5 * time.Second,
		NoLeaderTimeout:         30 * time.Second,
		DeadMemberTimeout:       120 * time.Second,
		BackupInterval:          60 * time.Second,
		CompactInterval:         5 * time.Minute, // matches old leader-checker default cadence
	}
	if c.PodName == "" || c.PodIP == "" {
		return nil, fmt.Errorf("POD_NAME and POD_IP env vars are required")
	}
	c.DBClusterAddr = c.PodIP

	// ENABLE_BIND_LOCAL_IP=true makes ovsdb-server bind only this pod's
	// addresses (instead of ::). For dual-stack POD_IPS may be multiple
	// comma-separated values; --db-{nb,sb}-addr takes one (we use the
	// primary PodIP), Local_Config/listen takes them all.
	if getenv("ENABLE_BIND_LOCAL_IP", "false") == "true" {
		c.DBAddr = c.PodIP
		c.DBAddresses = splitCSV(getenv("POD_IPS", c.PodIP))
		if len(c.DBAddresses) == 0 {
			c.DBAddresses = []string{c.PodIP}
		}
	} else {
		c.DBAddr = "::"
		c.DBAddresses = []string{"::"}
	}
	c.StaggerTimeout = time.Duration(atoiDefault(getenv("DYNAMIC_JOIN_TIMEOUT", ""), 60)) * time.Second
	return c, nil
}

// Run is the binary entry point. Returns only on fatal error -- otherwise
// blocks forever in the runtime loop. Retries preflight + bringUp in-process
// when bringUp signals errRetry (someone else is bootstrapping, peers not
// ready yet) so we don't churn through kubelet restart cycles.
func Run() error {
	cfg, err := parseConfig()
	if err != nil {
		return err
	}
	kc, err := newKubeClient()
	if err != nil {
		return err
	}
	ctx := context.Background()

	// Snapshot whether DB files were present at container startup. The
	// dbInherited flag is cleared by wipeDB and by initStub on fresh
	// create, so it stays true only while the on-disk DB carries over
	// from a prior container's life.
	lifecycle.dbInherited = readDBState(nbDB(cfg)).exists ||
		readDBState(sbDB(cfg)).exists

	// Publish initial status so peers see a fresh, accurate annotation
	// even if we crash before reaching runtimeLoop. Empty cid is fine
	// at this point; refreshAnnotations re-reads disk every call.
	if err := refreshAnnotations(ctx, cfg, kc, computeStatus(cfg)); err != nil {
		klog.Warningf("initial annotation publish: %v", err)
	}

	const retryDelay = 5 * time.Second
	for attempt := 1; ; attempt++ {
		if err := preflight(ctx, cfg, kc); err != nil {
			return fmt.Errorf("preflight (attempt %d): %w", attempt, err)
		}
		err := bringUp(ctx, cfg, kc)
		if err == nil {
			err = runtimeLoop(ctx, cfg, kc)
			if err == nil {
				return nil // ctx cancelled cleanly
			}
		}
		if !errors.Is(err, errRetry) {
			return fmt.Errorf("attempt %d: %w", attempt, err)
		}
		klog.Infof("attempt %d: %v; sleeping %s before retry", attempt, err, retryDelay)
		stopNBSBOvsdb()
		time.Sleep(retryDelay)
	}
}

// preflight inspects each DB and wipes ones we can't recover from in-place.
// Also sweeps leftover temp files from a crashed reconvert. Then publishes
// our (post-wipe) state into pod annotations.
func preflight(ctx context.Context, cfg *Config, kc kubernetes.Interface) error {
	for _, d := range []dbInfo{nbDB(cfg), sbDB(cfg)} {
		// Sweep reconvert temp files. Atomic-rename design means the
		// real db file is always either the pre-reconvert original or
		// the post-reconvert new one -- never missing -- so these
		// leftovers are always safe to drop.
		_ = os.Remove(d.dbFile + ".sa-tmp")
		_ = os.Remove(d.dbFile + ".new")

		st := readDBState(d)
		switch {
		case !st.exists:
			// nothing to validate
		case st.kicked:
			klog.Warningf("%s: kicked from cluster, wiping (%s)", d.short, d.dbFile)
			if err := wipeDB(d); err != nil {
				return err
			}
			lifecycle.dbInherited = false
		case st.notJoined && dbAge(d) >= 120*time.Second:
			klog.Warningf("%s: stale join-stub (>120s old), wiping", d.short)
			if err := wipeDB(d); err != nil {
				return err
			}
			lifecycle.dbInherited = false
		}
	}
	return refreshAnnotations(ctx, cfg, kc, computeStatus(cfg))
}

// bringUp tries to make this pod a healthy cluster member. Strategy:
//
//  1. Compose the peer list from kubectl (master-labeled IPs minus self).
//  2. For each DB without a file on disk, create a stub: rejoin-cluster
//     (preserve SID, no AddServer needed) if hdr exists, else join-cluster,
//     or create-cluster when no remotes (single-master deployment).
//  3. Recreate Local_Config DB so ovsdb-server listens on the right ports.
//  4. Start ovsdb-server; wait staggerTimeout for both DBs to reach
//     `Status: cluster member` with a known leader.
//  5. On success: start northd, refresh annotations, return.
//  6. On failure: take the bootstrap lease and either reconvert (we are
//     the sole survivor with committed data) or create-cluster fresh (no
//     peer has data). If we shouldn't be the one to act, exit and let
//     kubelet retry.
func bringUp(ctx context.Context, cfg *Config, kc kubernetes.Interface) error {
	peers, err := pickPeerIPs(ctx, kc, cfg.PodNamespace, cfg.PodIP)
	if err != nil {
		return fmt.Errorf("pickPeerIPs: %w", err)
	}
	klog.Infof("peer IPs from kubectl: %v", peers)

	bootstrap := false
	for _, d := range []dbInfo{nbDB(cfg), sbDB(cfg)} {
		// Local_Config carries listen addrs (db client port). For
		// ENABLE_BIND_LOCAL_IP=true with multiple POD_IPS we listen on
		// each (dual-stack); otherwise just on ::.
		listenAddrs := make([]string, 0, len(cfg.DBAddresses))
		for _, a := range cfg.DBAddresses {
			listenAddrs = append(listenAddrs, listenAddr(a, dbPort(d, cfg), cfg.EnableSSL))
		}
		if err := writeLocalConfigDB(d, listenAddrs); err != nil {
			return fmt.Errorf("writeLocalConfigDB %s: %w", d.short, err)
		}
		if _, errStat := os.Stat(d.dbFile); errStat == nil {
			continue
		}
		remoteAddrs := make([]string, 0, len(peers))
		for _, p := range peers {
			remoteAddrs = append(remoteAddrs, connAddr(p, d.clusterPort, cfg.EnableSSL))
		}
		boot, err := initStub(d, connAddr(cfg.DBClusterAddr, d.clusterPort, cfg.EnableSSL), remoteAddrs)
		if err != nil {
			return fmt.Errorf("initStub %s: %w", d.short, err)
		}
		// Fresh stub created in this container -- not "inherited" data.
		lifecycle.dbInherited = false
		if boot {
			bootstrap = true
		}
	}

	if err := startNBSBOvsdb(cfg, peers); err != nil {
		return err
	}
	postOvsdbStart(cfg)
	if err := refreshAnnotations(ctx, cfg, kc, computeStatus(cfg)); err != nil {
		klog.Warningf("annotation refresh after init: %v", err)
	}

	if err := waitForLeader(cfg, cfg.StaggerTimeout); err == nil {
		klog.Infof("cluster ready, starting northd (bootstrap=%v)", bootstrap)
		if err := refreshAnnotations(ctx, cfg, kc, computeStatus(cfg)); err != nil {
			klog.Warningf("annotation refresh post-ready: %v", err)
		}
		return startNorthd(cfg, bootstrap, peers)
	}

	klog.Warningf("waitForLeader timed out, entering recovery decision")
	stopNBSBOvsdb()
	return recover(ctx, cfg, kc, peers)
}

// recover is the lease-protected branch of bringUp. We only get here when
// the natural startup couldn't elect a leader within the stagger window.
//
// All decision-making and execution happens INSIDE the bootstrap lease.
// Two pods with stale snapshots can no longer independently decide "I'm
// the sole survivor" -- whichever one acquires the lease first sees a
// committed view, and the loser sees the freshly-published cid annotation
// and takes the wipe-rejoin path instead of reconverting in parallel.
//
// The lease is held all the way through start-ovsdb / wait-for-leader /
// start-northd / final annotation refresh, so the next pod to grab the
// lease only ever observes a stable, published cluster state.
func recover(ctx context.Context, cfg *Config, kc kubernetes.Interface, peers []string) error {
	klog.Infof("acquiring bootstrap lease for recovery decision")
	return withBootstrapLease(ctx, kc, cfg.PodNamespace, cfg.PodName,
		func(c context.Context) error {
			return recoverUnderLease(c, cfg, kc, peers)
		})
}

func recoverUnderLease(ctx context.Context, cfg *Config, kc kubernetes.Interface, peers []string) error {
	allPods, err := listPeers(ctx, kc, cfg.PodNamespace)
	if err != nil {
		return err
	}
	others := otherPeers(allPods, cfg.PodIP)
	mySt := computeStatus(cfg)

	// Tier hierarchy (higher = more authoritative data):
	//   active == recovering > leader_lost > stale > joining > "" (no data)
	//
	// We're inside the bootstrap lease, so this is the only pod making a
	// recovery decision right now. We act if no peer is at a strictly
	// higher tier; otherwise defer (errRetry). cid is not part of the
	// decision: any active peer is by definition the authoritative
	// cluster we should rejoin.

	// (1) Some peer is active AND ready: a working cluster exists. Our
	// local DB references the pre-failure raft topology and can't
	// re-integrate via raft alone, so wipe and let the next bringUp
	// create a fresh stub (initStub join-cluster) that AddServer's into
	// the active cluster and receives a snapshot from the leader.
	//
	// Ready=true is the truthful signal: kubelet's readiness probe runs
	// `cluster_member + known leader` against the peer's ovsdb, so a
	// peer with stale `active` annotation but failing probe (hung or
	// just-degraded) won't satisfy this check.
	if anyPeerActiveAndReady(others) {
		klog.Warningf("recover: peer active+ready; wiping local DB for fresh join")
		for _, d := range []dbInfo{nbDB(cfg), sbDB(cfg)} {
			if err := wipeDB(d); err != nil {
				return fmt.Errorf("wipe %s: %w", d.short, err)
			}
		}
		lifecycle.dbInherited = false
		return errRetry
	}

	// (2) We were active in this container's lifetime: highest tier among
	// non-active candidates. Reconvert.
	if mySt == statusLeaderLost {
		klog.Infof("recover: leader_lost; reconverting")
		return executeUnderLease(ctx, cfg, kc, "reconvert", peers, reconvertFn(cfg))
	}

	// (3) A peer is leader_lost (had data, recently active): they're
	// higher tier than us. Wait for them to act.
	if anyPeerStatus(others, statusLeaderLost) {
		klog.Infof("recover: peer leader_lost; deferring")
		return errRetry
	}

	// (4) We have data from a prior container (stale): no peer beats us,
	// reconvert.
	if mySt == statusStale {
		klog.Infof("recover: stale; reconverting")
		return executeUnderLease(ctx, cfg, kc, "reconvert", peers, reconvertFn(cfg))
	}

	// (5) A peer has stale data (preserved across restart): defer.
	if anyPeerStatus(others, statusStale) {
		klog.Infof("recover: peer stale; deferring")
		return errRetry
	}

	// (6) Nobody has authoritative data (no active+ready, no leader_lost,
	// no stale peer; we're either empty or stub-only). Bootstrap fresh.
	// The lease serializes: only one pod creates the new cluster, others
	// will see our active annotation on their next recover() and wipe +
	// rejoin via case (1).
	if mySt == statusJoining {
		klog.Infof("recover: stub-only and no peer has data; bootstrapping fresh cluster")
	} else {
		klog.Infof("recover: no peer has cluster data; bootstrapping fresh")
	}
	return executeUnderLease(ctx, cfg, kc, "bootstrap", peers, func(c context.Context) error {
		for _, d := range []dbInfo{nbDB(cfg), sbDB(cfg)} {
			_ = wipeDB(d)
			if _, err := initStub(d,
				connAddr(cfg.DBClusterAddr, d.clusterPort, cfg.EnableSSL), nil); err != nil {
				return err
			}
		}
		// Fresh cluster created in this container.
		lifecycle.dbInherited = false
		return nil
	})
}

// reconvertFn returns the closure that runs cluster-to-standalone +
// create-cluster (without --cid) for both NB and SB DBs, preserving data.
func reconvertFn(cfg *Config) func(context.Context) error {
	return func(c context.Context) error {
		for _, d := range []dbInfo{nbDB(cfg), sbDB(cfg)} {
			if err := reconvert(d, connAddr(cfg.DBClusterAddr, d.clusterPort, cfg.EnableSSL),
				nil); err != nil {
				return err
			}
		}
		return nil
	}
}

// executeUnderLease performs the destructive op (fn), restarts ovsdb,
// waits for leader, refreshes annotations and starts northd -- all while
// the caller still holds the bootstrap lease. Any other pod that wakes
// up to do recovery will block on the lease and only proceed after our
// cluster is fully published, eliminating the stale-snapshot race.
//
// ctx is the lease context: it is cancelled if leaderelection loses
// the lease. We check between irreversible steps and bail out rather
// than continue without the lease, since publishing/serving a fresh
// reconvert without the lease could race with another pod acting on
// a stale snapshot.
func executeUnderLease(ctx context.Context, cfg *Config, kc kubernetes.Interface,
	op string, peers []string, fn func(context.Context) error) error {

	// Publish recovering for operator visibility -- not consulted by
	// peers' decision logic (they only trust active+ready), but useful
	// when watching `kubectl get pods` annotations during a recovery.
	if err := refreshAnnotations(ctx, cfg, kc, statusRecovering); err != nil {
		klog.Warningf("annotation publish recovering: %v", err)
	}
	if err := fn(ctx); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("lease lost after %s, aborting before serving: %w", op, err)
	}
	if err := startNBSBOvsdb(cfg, peers); err != nil {
		return err
	}
	if err := waitForLeader(cfg, cfg.StaggerTimeout); err != nil {
		return fmt.Errorf("waitForLeader after %s: %w", op, err)
	}
	// Cluster is up with us as leader. Publish active so peers' next
	// recover() observes us as the new authoritative cluster.
	if err := refreshAnnotations(ctx, cfg, kc, computeStatus(cfg)); err != nil {
		klog.Warningf("annotation refresh post-recovery: %v", err)
	}
	postOvsdbStart(cfg)
	return startNorthd(cfg, op == "bootstrap", peers)
}

// runtimeLoop ticks until ctx is cancelled or the watchdog decides the
// cluster needs in-process recovery (errRetry). On errRetry, Run()
// catches it, sleeps, and re-enters preflight + bringUp + recover --
// no kubelet restart, lifecycle.wasActive stays true so we keep
// statusLeaderLost through the recovery.
func runtimeLoop(ctx context.Context, cfg *Config, kc kubernetes.Interface) error {
	state := newRuntimeState()
	tk := time.NewTicker(cfg.TickInterval)
	defer tk.Stop()
	klog.Infof("entering runtime loop (tick=%s)", cfg.TickInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tk.C:
			if err := runtimeTick(ctx, cfg, kc, state); err != nil {
				return err
			}
		}
	}
}

// runtimeState holds per-DB watchdog/kicker memory across ticks. Single-
// goroutine access; no synchronization needed.
type runtimeState struct {
	noLeaderSince  map[string]time.Time // db -> first time we observed Leader=unknown
	unhealthySince map[string]time.Time // <db>:<sid> -> first time observed unhealthy
	lastBackup     time.Time
	lastCompact    time.Time
	lastStatus     status // last published lifecycle status, for change-detection
}

func newRuntimeState() *runtimeState {
	return &runtimeState{
		noLeaderSince:  map[string]time.Time{},
		unhealthySince: map[string]time.Time{},
	}
}

func runtimeTick(ctx context.Context, cfg *Config, kc kubernetes.Interface, st *runtimeState) error {
	nbLeader, sbLeader := false, false
	noLeaderTimedOut := false
	for _, d := range []dbInfo{nbDB(cfg), sbDB(cfg)} {
		cs, err := readClusterStatus(d)
		if err != nil {
			klog.Warningf("cluster/status %s: %v", d.short, err)
			continue
		}
		// Watchdog: prolonged no-leader is the sole quorum-loss signal we
		// can read locally (raft only elects/keeps a leader with majority).
		if isKnownLeader(cs.leader) {
			delete(st.noLeaderSince, d.short)
		} else {
			if t, ok := st.noLeaderSince[d.short]; !ok {
				st.noLeaderSince[d.short] = time.Now()
				klog.Warningf("%s reports no leader (will check %s for recovery)", d.short, cfg.NoLeaderTimeout)
			} else if time.Since(t) >= cfg.NoLeaderTimeout {
				noLeaderTimedOut = true
			}
		}
		if cs.role == "leader" {
			kickStaleMembers(d, cs, cfg.DeadMemberTimeout, st)
			if d.short == "nb" {
				nbLeader = true
			} else {
				sbLeader = true
			}
		}
	}

	// Pod labels signal which pod is the NB / SB / northd leader so the
	// kube-ovn-controller's Service selectors route to the right one.
	// The northd-leader label is keyed on LOCAL ovn-northd state because
	// only the pod whose own northd holds the SB lock should be in the
	// Service endpoint.
	if err := patchLeaderLabels(ctx, kc, cfg.PodNamespace, cfg.PodName,
		nbLeader, sbLeader, localNorthdActive()); err != nil {
		klog.Warningf("patchLeaderLabels: %v", err)
	}

	// SB leader steals the ovn-northd lock only if NO pod anywhere has
	// an active northd (nobody is in the Service endpoint set). The
	// previous holder probably died without releasing, so blasting the
	// lock lets a standby take over. We must NOT steal when an active
	// northd already exists -- doing so just kicks it out and creates
	// a churn loop.
	if sbLeader && !anyNorthdActive(ctx, kc, cfg.PodNamespace) {
		klog.Warningf("no active northd anywhere, stealing lock")
		if err := stealLock(cfg); err != nil {
			klog.Errorf("stealLock: %v", err)
		}
	}

	if time.Since(st.lastBackup) >= cfg.BackupInterval {
		for _, d := range []dbInfo{nbDB(cfg), sbDB(cfg)} {
			if err := backupHeader(d); err != nil {
				klog.Warningf("backupHeader %s: %v", d.short, err)
			}
		}
		st.lastBackup = time.Now()
	}
	// Publish status only on transitions (e.g. active -> leader_lost).
	// Hung pods are caught via Pod.Ready (readiness probe), not via
	// stale annotations.
	cur := computeStatus(cfg)
	if cur != st.lastStatus {
		if err := refreshAnnotations(ctx, cfg, kc, cur); err != nil {
			klog.Warningf("refreshAnnotations: %v", err)
		}
		st.lastStatus = cur
	}
	if cfg.EnableCompact && time.Since(st.lastCompact) >= cfg.CompactInterval {
		// Run on whoever's the current leader of each DB; compact is a
		// raft-replicated snapshot operation that other members follow.
		if nbLeader {
			if err := compactDB(nbDB(cfg)); err != nil {
				klog.Warningf("compactDB nb: %v", err)
			}
		}
		if sbLeader {
			if err := compactDB(sbDB(cfg)); err != nil {
				klog.Warningf("compactDB sb: %v", err)
			}
		}
		st.lastCompact = time.Now()
	}

	// No-leader watchdog: if either DB has been leaderless past the
	// timeout, decide whether to wait (some peer claims to be active --
	// they may heal the cluster or simply haven't refreshed their stale
	// annotation yet) or run recovery in-process right here.
	if noLeaderTimedOut {
		allPods, err := listPeers(ctx, kc, cfg.PodNamespace)
		if err != nil {
			klog.Warningf("listPeers in no-leader path: %v; will retry next tick", err)
			return nil
		}
		others := otherPeers(allPods, cfg.PodIP)
		if anyPeerActiveAndReady(others) {
			klog.Warningf("no leader; a peer is active+ready, deferring (will recheck next tick)")
			return nil
		}
		// Run recovery inline -- no point bubbling through Run()'s
		// retry sleep + bringUp's waitForLeader timeout when we
		// already know the cluster needs reconvert/bootstrap. recover()
		// stops ovsdb itself before destructive ops.
		klog.Warningf("no leader and no active+ready peer; running recovery in-process")
		stopNBSBOvsdb()
		peers, err := pickPeerIPs(ctx, kc, cfg.PodNamespace, cfg.PodIP)
		if err != nil {
			klog.Warningf("pickPeerIPs in no-leader path: %v", err)
			return errRetry
		}
		if err := recover(ctx, cfg, kc, peers); err != nil {
			// errRetry from recover means it deferred or wiped; let
			// Run() handle the retry cycle (it will rebuild bringUp
			// fresh). Other errors are fatal.
			return err
		}
		// Successful reconvert/bootstrap: ovsdb is back up, we're the
		// new leader. Reset the watchdog timer and continue ticking.
		delete(st.noLeaderSince, "nb")
		delete(st.noLeaderSince, "sb")
	}
	return nil
}

// kickStaleMembers walks the cluster/status server list. For peers other
// than self that have been silent past dead-timeout, sends cluster/kick.
// "No last_msg" is treated as needing a grace period (we may have just
// restarted; give peers a chance to phone home before we kick them).
func kickStaleMembers(d dbInfo, cs clusterStatus, dead time.Duration, st *runtimeState) {
	now := time.Now()
	seen := map[string]struct{}{}
	for _, s := range cs.servers {
		if s.self {
			continue
		}
		key := d.short + ":" + s.sid
		seen[s.sid] = struct{}{}
		var unhealthy bool
		var reason string
		var kickNow bool
		if s.lastMsgMillis < 0 {
			unhealthy = true
			reason = "no last_contact recorded"
		} else if time.Duration(s.lastMsgMillis)*time.Millisecond > dead {
			unhealthy = true
			reason = fmt.Sprintf("last contact %ds ago", s.lastMsgMillis/1000)
			kickNow = true
		}
		if !unhealthy {
			delete(st.unhealthySince, key)
			continue
		}
		if kickNow {
			klog.Infof("kicking %s member %s: %s", d.short, s.sid, reason)
			if err := kickMember(d, s.sid); err != nil {
				klog.Errorf("kick %s/%s: %v", d.short, s.sid, err)
				continue
			}
			delete(st.unhealthySince, key)
			continue
		}
		// no last_msg path: track first-observed; kick after grace.
		if t, ok := st.unhealthySince[key]; !ok {
			st.unhealthySince[key] = now
			klog.Warningf("%s member %s observed unhealthy: %s (will kick after %s)",
				d.short, s.sid, reason, dead)
		} else if now.Sub(t) >= dead {
			klog.Infof("kicking %s member %s: %s, unhealthy for %.0fs",
				d.short, s.sid, reason, now.Sub(t).Seconds())
			if err := kickMember(d, s.sid); err != nil {
				klog.Errorf("kick %s/%s: %v", d.short, s.sid, err)
				continue
			}
			delete(st.unhealthySince, key)
		}
	}
	// Prune state for members no longer in cluster (already gone).
	for key := range st.unhealthySince {
		if !startsWith(key, d.short+":") {
			continue
		}
		sid := key[len(d.short)+1:]
		if _, ok := seen[sid]; !ok {
			delete(st.unhealthySince, key)
		}
	}
}

// refreshAnnotations publishes our current lifecycle status to the pod
// annotation. Idempotent.
func refreshAnnotations(ctx context.Context, cfg *Config, kc kubernetes.Interface, st status) error {
	return publishStatus(ctx, kc, cfg.PodNamespace, cfg.PodName, st)
}

// pickPeerIPs returns the peer-pod IPs we use as raft remotes for new
// join-stubs. Source: kubectl pods labelled app=ovn-central, minus self.
// pickPeerIPs returns peer IPs to use as raft remotes for join/rejoin
// stubs. We include peers that aren't fully Ready yet (their pod just
// spawned with an IP) since ovsdb-server will retry until at least one
// remote responds; we exclude Terminating peers (their address is
// about to disappear) and peers without a Pod IP.
func pickPeerIPs(ctx context.Context, kc kubernetes.Interface, ns, selfIP string) ([]string, error) {
	all, err := listPeers(ctx, kc, ns)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(all))
	for _, p := range all {
		if p.ip == "" || p.ip == selfIP || p.terminating {
			continue
		}
		out = append(out, p.ip)
	}
	return out, nil
}

func dbPort(d dbInfo, cfg *Config) int {
	if d.short == "nb" {
		return cfg.NBPort
	}
	return cfg.SBPort
}

// trivial helpers

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func atoi(s string) int          { v, _ := strconv.Atoi(s); return v }
func splitCSV(s string) []string {
	out := []string{}
	for p := range strings.SplitSeq(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
func atoiDefault(s string, d int) int {
	if v, err := strconv.Atoi(s); err == nil {
		return v
	}
	return d
}
func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
