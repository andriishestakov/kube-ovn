// ovsdb.go: thin wrappers around ovsdb-tool / ovs-appctl / ovn-ctl.
// All on-disk and runtime DB inspection lives here. No k8s, no orchestration.
package ovn_central_controller

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

// dbInfo describes one of the two raft databases (NB or SB) we manage in lockstep.
type dbInfo struct {
	short      string // "nb" / "sb"
	name       string // OVN_Northbound / OVN_Southbound
	schema     string // ovs schema file
	dbFile     string // /etc/ovn/ovnnb_db.db
	hdrFile    string // /etc/ovn/ovnnb_db.hdr
	ctlSocket  string // /var/run/ovn/ovnnb_db.ctl
	port       int    // db client port (6641 / 6642)
	clusterPort int   // raft cluster port (6643 / 6644)
}

func nbDB(cfg *Config) dbInfo {
	return dbInfo{
		short:       "nb",
		name:        "OVN_Northbound",
		schema:      "/usr/share/ovn/ovn-nb.ovsschema",
		dbFile:      filepath.Join(cfg.OVNDir, "ovnnb_db.db"),
		hdrFile:     filepath.Join(cfg.OVNDir, "ovnnb_db.hdr"),
		ctlSocket:   filepath.Join(cfg.OVNRunDir, "ovnnb_db.ctl"),
		port:        cfg.NBPort,
		clusterPort: cfg.NBClusterPort,
	}
}

func sbDB(cfg *Config) dbInfo {
	return dbInfo{
		short:       "sb",
		name:        "OVN_Southbound",
		schema:      "/usr/share/ovn/ovn-sb.ovsschema",
		dbFile:      filepath.Join(cfg.OVNDir, "ovnsb_db.db"),
		hdrFile:     filepath.Join(cfg.OVNDir, "ovnsb_db.hdr"),
		ctlSocket:   filepath.Join(cfg.OVNRunDir, "ovnsb_db.ctl"),
		port:        cfg.SBPort,
		clusterPort: cfg.SBClusterPort,
	}
}

// dbState is the on-disk classification of one db file.
type dbState struct {
	exists    bool
	kicked    bool // check-cluster reports server left/not in list
	notJoined bool // join-stub that hasn't synced
}

// readDBState inspects on-disk state non-destructively.
func readDBState(d dbInfo) dbState {
	s := dbState{}
	if _, err := os.Stat(d.dbFile); err != nil {
		return s
	}
	s.exists = true

	if msg := runMerged("ovsdb-tool", "check-cluster", d.dbFile); msg != "" {
		// "left the cluster" (we issued cluster/leave) or "not found in
		// server list" (leader kicked us): same outcome -- ovsdb-server
		// won't start, must wipe.
		if regexp.MustCompile(`shows that the server left the cluster|server [0-9a-f]+ not found in server list`).MatchString(msg) {
			s.kicked = true
		} else if strings.Contains(msg, "has not joined the cluster") {
			s.notJoined = true
		}
	}
	return s
}

// dbAge returns the time since the db file was created, or 0 if missing.
func dbAge(d dbInfo) time.Duration {
	st, err := os.Stat(d.dbFile)
	if err != nil {
		return 0
	}
	return time.Since(st.ModTime())
}

// wipeDB removes db_file and hdr_file. Idempotent.
func wipeDB(d dbInfo) error {
	if err := os.Remove(d.dbFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove %s: %w", d.dbFile, err)
	}
	_ = os.Remove(d.hdrFile)
	return nil
}

// initStub creates a fresh db file: create-cluster (if no remotes),
// rejoin-cluster (if hdr present, preserves SID, no AddServer needed),
// or plain join-cluster otherwise.
func initStub(d dbInfo, localAddr string, remotes []string) (bootstrapped bool, err error) {
	if len(remotes) == 0 {
		err = run("ovsdb-tool", "create-cluster", d.dbFile, d.schema, localAddr)
		return true, err
	}
	args := []string{}
	if _, errStat := os.Stat(d.hdrFile); errStat == nil {
		args = append(args, "rejoin-cluster", d.dbFile, d.hdrFile, localAddr)
		args = append(args, remotes...)
		if err := run("ovsdb-tool", args...); err == nil {
			return false, nil
		}
		// rejoin failed (corrupt header etc.) -- wipe and try plain join below.
		_ = os.Remove(d.dbFile)
		_ = os.Remove(d.hdrFile)
	}
	args = []string{"join-cluster", d.dbFile, d.name, localAddr}
	args = append(args, remotes...)
	return false, run("ovsdb-tool", args...)
}

// reconvert: cluster->standalone->cluster preserving CID. Used by the
// sole survivor after permanent quorum loss to recreate a working 1-node
// cluster while keeping data and cluster id intact.
// reconvert writes the new cluster DB atomically: build alongside the
// original (cluster -> sa -> cluster.new), then rename(cluster.new,
// cluster). At every crash point either the original DB or both are
// readable; we never have a missing cluster file with data trapped in
// the standalone tmp. Leftover .sa-tmp / .new files are swept by
// preflight at startup.
func reconvert(d dbInfo, localAddr string, remotes []string) error {
	if !readDBState(d).exists {
		return fmt.Errorf("reconvert: %s missing", d.dbFile)
	}
	sa := d.dbFile + ".sa-tmp"
	newDB := d.dbFile + ".new"
	_ = os.Remove(sa)
	_ = os.Remove(newDB)
	if err := run("ovsdb-tool", "cluster-to-standalone", sa, d.dbFile); err != nil {
		_ = os.Remove(sa)
		return fmt.Errorf("cluster-to-standalone %s: %w", d.dbFile, err)
	}
	args := []string{"create-cluster", newDB, sa, localAddr}
	args = append(args, remotes...)
	if err := run("ovsdb-tool", args...); err != nil {
		_ = os.Remove(sa)
		_ = os.Remove(newDB)
		return fmt.Errorf("create-cluster: %w", err)
	}
	if err := os.Rename(newDB, d.dbFile); err != nil {
		_ = os.Remove(sa)
		_ = os.Remove(newDB)
		return fmt.Errorf("rename %s -> %s: %w", newDB, d.dbFile, err)
	}
	_ = os.Remove(sa)
	_ = os.Remove(d.hdrFile)
	return nil
}

// startNBSBOvsdb starts both ovsdb-server processes via ovn-ctl. Returns
// after ovn-ctl returns (which itself waits for the unix sockets to be
// ready). Caller is responsible for waiting for the cluster to elect a
// leader (see waitForLeader).
func startNBSBOvsdb(cfg *Config, peers []string) error {
	args := buildOvnCtlArgs(cfg, peers)
	for _, sub := range []string{"start_nb_ovsdb", "start_sb_ovsdb"} {
		full := append([]string{}, args...)
		full = append(full, sub, "--")
		// db:Local_Config remote registers the db client port (6641/6642)
		// listener. Mirrors what the legacy bash flow does.
		full = append(full,
			"--remote=db:Local_Config,Config,connections",
			filepath.Join(cfg.OVNDir, fmt.Sprintf("ovn%s_local_config.db", strings.TrimPrefix(sub, "start_")[:2])))
		if err := run("/usr/share/ovn/scripts/ovn-ctl", full...); err != nil {
			return fmt.Errorf("%s failed: %w", sub, err)
		}
	}
	return nil
}

func stopNBSBOvsdb() {
	_ = run("/usr/share/ovn/scripts/ovn-ctl", "stop_nb_ovsdb")
	_ = run("/usr/share/ovn/scripts/ovn-ctl", "stop_sb_ovsdb")
	_ = run("/usr/share/ovn/scripts/ovn-ctl", "stop_northd")
}

// startNorthd launches ovn-northd. Optionally writes NB_Global / SB_Global
// option overrides if `bootstrap` (we just created a 1-node cluster).
func startNorthd(cfg *Config, bootstrap bool, peers []string) error {
	args := buildOvnCtlArgs(cfg, peers)
	args = append(args,
		"--ovn-manage-ovsdb=no",
		fmt.Sprintf("--ovn-northd-n-threads=%d", cfg.OVNNorthdNThreads),
		"start_northd",
	)
	if err := run("/usr/share/ovn/scripts/ovn-ctl", args...); err != nil {
		return fmt.Errorf("start_northd: %w", err)
	}
	if !bootstrap {
		return nil
	}
	// On a fresh 1-node cluster, write NB/SB Global tunables. These are
	// raft-replicated to followers when they later join. Pass SSL options
	// when ENABLE_SSL=true so ovn-{nb,sb}ctl can talk to the (TLS) DB.
	probe := strconv.Itoa(cfg.ProbeInterval)
	northdProbe := strconv.Itoa(cfg.OVNNorthdProbeInterval)
	ssl := cfg.SSLOptions()
	mk := func(bin string, table string, kv string) []string {
		args := append([]string{"--no-leader-only"}, ssl...)
		args = append(args, "set", table, ".", kv)
		return append([]string{bin}, args...)
	}
	cmds := [][]string{
		mk("ovn-nbctl", "NB_Global", "options:inactivity_probe="+probe),
		mk("ovn-sbctl", "SB_Global", "options:inactivity_probe="+probe),
		mk("ovn-nbctl", "NB_Global", "options:northd_probe_interval="+northdProbe),
		mk("ovn-nbctl", "NB_Global", "options:use_logical_dp_groups=true"),
	}
	for _, c := range cmds {
		if err := run(c[0], c[1:]...); err != nil {
			return fmt.Errorf("%v: %w", c, err)
		}
	}
	return nil
}

// postOvsdbStart enables memory-trim-on-compaction (lets the kernel
// reclaim RSS after periodic compactions, prevents long-term bloat) and
// locks down DB file permissions. Best-effort: failures are logged.
func postOvsdbStart(cfg *Config) {
	for _, d := range []dbInfo{nbDB(cfg), sbDB(cfg)} {
		if err := run("ovn-appctl", "-t", d.ctlSocket, "ovsdb-server/memory-trim-on-compaction", "on"); err != nil {
			klog.Warningf("memory-trim-on-compaction %s: %v", d.short, err)
		}
	}
	matches, _ := filepath.Glob(filepath.Join(cfg.OVNDir, "*"))
	for _, p := range matches {
		_ = os.Chmod(p, 0o600)
	}
}

// compactDB asks ovsdb-server to write a new snapshot now (truncates the
// raft log to that snapshot). Cheap when there's little to compact, so we
// can call it on every leader from the runtime loop with a long interval.
func compactDB(d dbInfo) error {
	return run("ovn-appctl", "-t", d.ctlSocket, "ovsdb-server/compact")
}

// isNorthdActive returns true iff the local ovn-northd reports active.
// localNorthdActive returns true iff THIS pod's ovn-northd is currently
// holding the SB lock and writing. Used to set the ovn-northd-leader
// label on our pod (which drives the ovn-northd Service endpoint).
func localNorthdActive() bool {
	out, err := exec.Command("ovn-appctl", "-t", "ovn-northd", "status").CombinedOutput()
	if err != nil {
		return false
	}
	return strings.Contains(strings.TrimSpace(string(out)), "active")
}

// stealLock forces release of the ovn_northd lock on the local SB
// database. Used when the previous lock holder went down without
// releasing; without this, ovn-northd elsewhere can't take leadership.
func stealLock(cfg *Config) error {
	addr := connAddr(cfg.DBClusterAddr, cfg.SBPort, cfg.EnableSSL)
	args := []string{"-v", "-t", "1"}
	args = append(args, cfg.SSLOptions()...)
	args = append(args, "steal", addr, "ovn_northd")
	return run("ovsdb-client", args...)
}


// clusterStatus is the parsed view of `ovs-appctl ... cluster/status`.
type clusterStatus struct {
	cid       string
	sid       string
	role      string // leader / follower / candidate / "" if unknown
	leader    string // sid hex / "self" / "unknown" / ""
	status    string // cluster member / joining cluster / left cluster / etc.
	term      int
	servers   []serverInfo
}

type serverInfo struct {
	sid           string
	addr          string
	self          bool
	lastMsgMillis int64 // -1 if not present
}

var (
	reLeaderLine   = regexp.MustCompile(`^Leader:\s+(\S+)`)
	reTermLine     = regexp.MustCompile(`^Term:\s+(\d+)`)
	reRoleLine     = regexp.MustCompile(`^Role:\s+(\S+)`)
	reStatusLine   = regexp.MustCompile(`^Status:\s+(.+)$`)
	reCIDLine      = regexp.MustCompile(`^Cluster ID:\s+\S+\s+\(([0-9a-f-]+)\)`)
	reSIDLine      = regexp.MustCompile(`^Server ID:\s+\S+\s+\(([0-9a-f-]+)\)`)
	reServerLine   = regexp.MustCompile(`^\s+([0-9a-f]+)\s+\([0-9a-f-]+\s+at\s+(\S+?)\)(.*)$`)
	reLastMsg = regexp.MustCompile(`last msg (\d+) ms ago`)
)

// readClusterStatus calls ovs-appctl cluster/status against the unix socket.
// Returns a clusterStatus or an error if the socket is unreachable.
func readClusterStatus(d dbInfo) (clusterStatus, error) {
	out, err := exec.Command("ovs-appctl", "-t", d.ctlSocket, "cluster/status", d.name).CombinedOutput()
	if err != nil {
		return clusterStatus{}, fmt.Errorf("cluster/status %s: %w (%s)", d.short, err, out)
	}
	cs := clusterStatus{}
	var selfSID string
	for line := range strings.SplitSeq(string(out), "\n") {
		switch {
		case reLeaderLine.MatchString(line):
			cs.leader = reLeaderLine.FindStringSubmatch(line)[1]
		case reTermLine.MatchString(line):
			cs.term, _ = strconv.Atoi(reTermLine.FindStringSubmatch(line)[1])
		case reRoleLine.MatchString(line):
			cs.role = reRoleLine.FindStringSubmatch(line)[1]
		case reStatusLine.MatchString(line):
			cs.status = strings.TrimSpace(reStatusLine.FindStringSubmatch(line)[1])
		case reCIDLine.MatchString(line):
			cs.cid = reCIDLine.FindStringSubmatch(line)[1]
		case reSIDLine.MatchString(line):
			cs.sid = reSIDLine.FindStringSubmatch(line)[1]
			selfSID = strings.SplitN(cs.sid, "-", 2)[0][:4]
		}
		if m := reServerLine.FindStringSubmatch(line); m != nil {
			si := serverInfo{sid: m[1], addr: m[2], lastMsgMillis: -1}
			if m[1] == selfSID {
				si.self = true
			}
			if lc := reLastMsg.FindStringSubmatch(m[3]); lc != nil {
				ms, _ := strconv.ParseInt(lc[1], 10, 64)
				si.lastMsgMillis = ms
			}
			cs.servers = append(cs.servers, si)
		}
	}
	return cs, nil
}

// waitForLeader polls cluster/status until both DBs report Status: cluster
// member and a known leader, or until timeout. Returns the elapsed time.
func waitForLeader(cfg *Config, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		nb, errNB := readClusterStatus(nbDB(cfg))
		sb, errSB := readClusterStatus(sbDB(cfg))
		if errNB == nil && errSB == nil &&
			nb.status == "cluster member" && sb.status == "cluster member" &&
			isKnownLeader(nb.leader) && isKnownLeader(sb.leader) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out: nb=%q sb=%q", nb.leader, sb.leader)
		}
		time.Sleep(2 * time.Second)
	}
}

func isKnownLeader(s string) bool { return s != "" && s != "unknown" }

// kickMember asks the local (must be leader) ovsdb-server to remove peer.
func kickMember(d dbInfo, peerSID string) error {
	return run("ovs-appctl", "-t", d.ctlSocket, "cluster/kick", d.name, peerSID)
}

// backupHeader writes the local raft header JSON to hdr_file. Used by the
// rejoin-cluster path to recreate a stub with the same SID after a DB loss.
func backupHeader(d dbInfo) error {
	out, err := exec.Command("ovsdb-tool", "db-raft-header", d.dbFile).Output()
	if err != nil {
		return fmt.Errorf("db-raft-header %s: %w", d.dbFile, err)
	}
	tmp := d.hdrFile + ".tmp"
	if err := os.WriteFile(tmp, out, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, d.hdrFile)
}

// writeLocalConfigDB recreates ovn{nb,sb}_local_config.db with the current
// listen addresses for the db client port. ovn-ctl's
// --remote=db:Local_Config,Config,connections reads this on every start.
func writeLocalConfigDB(d dbInfo, listenAddrs []string) error {
	cfgDB := strings.TrimSuffix(d.dbFile, "_db.db") + "_local_config.db"
	_ = os.Remove(cfgDB)
	if err := run("ovsdb-tool", "create", cfgDB, "/usr/share/openvswitch/local-config.ovsschema"); err != nil {
		return err
	}
	// Initial empty Config row.
	if err := run("ovsdb-tool", "transact", cfgDB, `[
		"Local_Config",
		{"op": "insert", "table": "Config", "row": {"connections": ["set", []]}}
	]`); err != nil {
		return err
	}
	for _, addr := range listenAddrs {
		txn := fmt.Sprintf(`[
			"Local_Config",
			{"op": "insert", "table": "Connection", "uuid-name": "n", "row": {"target": %q}},
			{"op": "mutate", "table": "Config", "where": [], "mutations": [["connections", "insert", ["set", [["named-uuid", "n"]]]]]}
		]`, addr)
		if err := run("ovsdb-tool", "transact", cfgDB, txn); err != nil {
			return err
		}
	}
	return nil
}

// buildOvnCtlArgs constructs the common --db-{nb,sb}-... flags for ovn-ctl
// invocations. Using just one peer in remote-addr is fine: ovsdb-server
// learns the rest via the join-stub or already-committed cluster config.
func buildOvnCtlArgs(cfg *Config, peers []string) []string {
	args := []string{}
	if cfg.DebugWrapper != "" {
		args = append(args,
			"--ovn-northd-wrapper="+cfg.DebugWrapper,
			"--ovsdb-nb-wrapper="+cfg.DebugWrapper,
			"--ovsdb-sb-wrapper="+cfg.DebugWrapper,
		)
	}
	args = append(args,
		"--db-cluster-schema-upgrade=no",
		fmt.Sprintf("--db-nb-cluster-local-addr=[%s]", cfg.DBClusterAddr),
		fmt.Sprintf("--db-sb-cluster-local-addr=[%s]", cfg.DBClusterAddr),
		fmt.Sprintf("--db-nb-cluster-local-port=%d", cfg.NBClusterPort),
		fmt.Sprintf("--db-sb-cluster-local-port=%d", cfg.SBClusterPort),
		fmt.Sprintf("--db-nb-addr=[%s]", cfg.DBAddr),
		fmt.Sprintf("--db-sb-addr=[%s]", cfg.DBAddr),
		fmt.Sprintf("--db-nb-port=%d", cfg.NBPort),
		fmt.Sprintf("--db-sb-port=%d", cfg.SBPort),
		"--db-nb-use-remote-in-db=no",
		"--db-sb-use-remote-in-db=no",
	)
	if cfg.EnableSSL {
		args = append(args,
			"--ovn-nb-db-ssl-key=/var/run/tls/key",
			"--ovn-nb-db-ssl-cert=/var/run/tls/cert",
			"--ovn-nb-db-ssl-ca-cert=/var/run/tls/cacert",
			"--ovn-sb-db-ssl-key=/var/run/tls/key",
			"--ovn-sb-db-ssl-cert=/var/run/tls/cert",
			"--ovn-sb-db-ssl-ca-cert=/var/run/tls/cacert",
			"--ovn-northd-ssl-key=/var/run/tls/key",
			"--ovn-northd-ssl-cert=/var/run/tls/cert",
			"--ovn-northd-ssl-ca-cert=/var/run/tls/cacert",
			"--db-nb-cluster-local-proto=ssl",
			"--db-sb-cluster-local-proto=ssl",
			"--db-nb-cluster-remote-proto=ssl",
			"--db-sb-cluster-remote-proto=ssl",
		)
	} else {
		args = append(args,
			"--db-nb-create-insecure-remote=yes",
			"--db-sb-create-insecure-remote=yes",
		)
	}
	if len(peers) > 0 {
		args = append(args,
			fmt.Sprintf("--db-nb-cluster-remote-addr=[%s]", peers[0]),
			fmt.Sprintf("--db-sb-cluster-remote-addr=[%s]", peers[0]),
			fmt.Sprintf("--db-nb-cluster-remote-port=%d", cfg.NBClusterPort),
			fmt.Sprintf("--db-sb-cluster-remote-port=%d", cfg.SBClusterPort),
		)
	}
	allPeers := append([]string{cfg.DBClusterAddr}, peers...)
	nbConn := commaJoin("tcp:[%s]:"+strconv.Itoa(cfg.NBPort), allPeers, cfg.EnableSSL)
	sbConn := commaJoin("tcp:[%s]:"+strconv.Itoa(cfg.SBPort), allPeers, cfg.EnableSSL)
	args = append(args,
		"--ovn-northd-nb-db="+nbConn,
		"--ovn-northd-sb-db="+sbConn,
	)
	return args
}

func commaJoin(format string, peers []string, ssl bool) string {
	if ssl {
		format = strings.Replace(format, "tcp:", "ssl:", 1)
	}
	parts := make([]string, 0, len(peers))
	for _, p := range peers {
		parts = append(parts, fmt.Sprintf(format, p))
	}
	return strings.Join(parts, ",")
}

// listenAddr formats the --remote=p{tcp,ssl}: listener used by ovsdb-server.
func listenAddr(addr string, port int, ssl bool) string {
	proto := "ptcp"
	if ssl {
		proto = "pssl"
	}
	return fmt.Sprintf("%s:%d:[%s]", proto, port, addr)
}

// connAddr formats a raft remote (peer cluster-port) address.
func connAddr(addr string, port int, ssl bool) string {
	proto := "tcp"
	if ssl {
		proto = "ssl"
	}
	return fmt.Sprintf("%s:[%s]:%d", proto, addr, port)
}

// run executes cmd, returns error on non-zero exit. Output streams to
// our stderr for tracing.
func run(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}


// runMerged runs cmd and returns combined stdout+stderr regardless of exit.
func runMerged(name string, args ...string) string {
	out, _ := exec.Command(name, args...).CombinedOutput()
	return string(out)
}
