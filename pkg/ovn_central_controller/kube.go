// kube.go: kubernetes API helpers -- pod listing, annotations, lease.
// No DB knowledge here.
package ovn_central_controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

// Annotation published per pod. The lifecycle status alone is enough
// for peers to make recovery decisions; cid/sid are not exposed because
// they're only meaningful within a single raft-cluster lifetime and we
// don't preserve cid across reconvert anyway.
const (
	annotStatus = "kube-ovn.io/ovn-status"

	bootstrapLeaseName = "ovn-central-bootstrap"
	podLabelSelector   = "app=ovn-central"
)

// Lifecycle status published in the annotation. Computed in this
// container's lifetime, not sticky across container restarts: source of
// truth is what ovsdb currently reports + what we know we've done.
//
// Decision tier (only statusActive+ready triggers wipe-and-rejoin in
// peers; the rest are informational):
//
//	statusActive > statusLeaderLost > statusStale > statusJoining > "" (no DB)
//
// statusRecovering is published purely for operator visibility while
// we hold the bootstrap lease and run a destructive op.
type status string

const (
	statusJoining    status = "joining"     // stub created in this container, never been cluster_member+leader
	statusStale      status = "stale"       // DB existed at startup, never been active in this container
	statusLeaderLost status = "leader_lost" // was active in this container, leader currently unknown
	statusActive     status = "active"      // ovsdb reports cluster_member + known leader right now
	statusRecovering status = "recovering"  // diagnostic-only: under bootstrap lease, mid destructive op
)

func newKubeClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	return kubernetes.NewForConfig(cfg)
}

// peerInfo summarizes the kubectl-visible state of one ovn-central pod.
type peerInfo struct {
	name        string
	ip          string
	status      status
	running     bool
	ready       bool // Pod.Status.Conditions[PodReady] -- only true when
	// the readiness probe (cluster_member + known leader) passes;
	// trustworthy signal that this peer's annotation is current.
	terminating bool // Pod has DeletionTimestamp -- on its way out, may
	// still respond briefly during grace, may be hung. Unreliable for
	// recovery decisions; its annotation is whatever was last published.
}

// listPeers returns all ovn-central pods from kubectl. Caller filters
// further (by IP, status, terminating, etc.).
func listPeers(ctx context.Context, kc kubernetes.Interface, ns string) ([]peerInfo, error) {
	pl, err := kc.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: podLabelSelector})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	out := make([]peerInfo, 0, len(pl.Items))
	for _, p := range pl.Items {
		pi := peerInfo{
			name:        p.Name,
			ip:          p.Status.PodIP,
			running:     p.Status.Phase == corev1.PodRunning,
			status:      status(p.Annotations[annotStatus]),
			terminating: p.DeletionTimestamp != nil,
		}
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodReady {
				pi.ready = c.Status == corev1.ConditionTrue
				break
			}
		}
		out = append(out, pi)
	}
	return out, nil
}

// otherPeers returns peers usable for recovery decisions: not self,
// has a pod IP, currently Running phase, and not Terminating. Pods in
// Pending/Failed/Terminating phases or scheduled-for-deletion are
// excluded -- their state is either absent (no IP) or unreliable
// (annotation may be stale, process may be hung in grace period).
func otherPeers(all []peerInfo, selfIP string) []peerInfo {
	out := make([]peerInfo, 0, len(all))
	for _, p := range all {
		if p.ip == "" || p.ip == selfIP || !p.running || p.terminating {
			continue
		}
		out = append(out, p)
	}
	return out
}

// anyPeerStatus returns true if any peer reports one of the listed
// statuses. Used for "deferring" tiers (leader_lost, stale) where the
// peer might be stuck and we just don't act yet.
func anyPeerStatus(peers []peerInfo, want ...status) bool {
	for _, p := range peers {
		for _, w := range want {
			if p.status == w {
				return true
			}
		}
	}
	return false
}

// anyPeerActiveAndReady is the trustworthy "there is a healthy cluster"
// signal: peer's annotation says active AND kubelet's readiness probe
// currently passes. Stale annotations from hung or just-degraded pods
// don't satisfy this -- the probe would fail.
func anyPeerActiveAndReady(peers []peerInfo) bool {
	for _, p := range peers {
		if p.ready && p.status == statusActive {
			return true
		}
	}
	return false
}

// anyNorthdActive returns true iff some pod in the cluster currently
// hosts the active ovn-northd, observed via the ovn-northd Service's
// EndpointSlices (we only set ovn-northd-leader=true on the active
// pod). On API error returns true to fail-safe: stealing the SB lock
// when an active northd is already running just bounces leadership
// pointlessly.
func anyNorthdActive(ctx context.Context, kc kubernetes.Interface, ns string) bool {
	esl, err := kc.DiscoveryV1().EndpointSlices(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "kubernetes.io/service-name=ovn-northd",
	})
	if err != nil {
		klog.Warningf("list ovn-northd endpointslices: %v", err)
		return true
	}
	for _, es := range esl.Items {
		for _, ep := range es.Endpoints {
			if len(ep.Addresses) == 0 {
				continue
			}
			if ep.Conditions.Ready == nil || *ep.Conditions.Ready {
				return true
			}
		}
	}
	return false
}

// publishStatus patches our own pod with the current lifecycle status.
// Called whenever the state machine transitions (post-init, under lease,
// after wipe, on no-leader, etc.) and periodically from the runtime
// loop. Empty status is sent as null so kubectl removes the annotation.
func publishStatus(ctx context.Context, kc kubernetes.Interface, ns, name string, st status) error {
	val := func(s string) any {
		if s == "" {
			return nil
		}
		return s
	}
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				annotStatus: val(string(st)),
			},
		},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = kc.CoreV1().Pods(ns).Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{})
	return err
}

// withBootstrapLease acquires the bootstrap lease, runs fn while holding
// it, and releases on return. Only one ovn-central pod across the
// cluster can be inside fn at any moment -- this is the invariant that
// prevents concurrent reconvert/create-cluster from forming split-brain.
//
// fn is expected to run synchronously and quickly (creating a new
// 1-node cluster takes seconds, not minutes). If it doesn't return
// before the lease's renew deadline, the lease is lost and another
// pod may take over.
func withBootstrapLease(ctx context.Context, kc kubernetes.Interface, ns, identity string,
	fn func(context.Context) error) error {

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: bootstrapLeaseName, Namespace: ns},
		Client:    kc.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}
	leCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var fnErr error
	leaderelection.RunOrDie(leCtx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		// Long enough to cover reconvert + start ovsdb + waitForLeader
		// (StaggerTimeout) + start northd while still under the lease,
		// with very generous renewal headroom so a brief API hiccup
		// can't lose the lease mid-recovery (would risk split-brain
		// with another pod racing in).
		LeaseDuration:   10 * time.Minute,
		RenewDeadline:   5 * time.Minute,
		RetryPeriod:     5 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(c context.Context) {
				klog.Infof("acquired bootstrap lease, running action")
				fnErr = fn(c)
				cancel() // releases lease
			},
			OnStoppedLeading: func() {
				klog.Infof("released bootstrap lease")
			},
		},
	})
	return fnErr
}

// patchLeaderLabels sets ovn-nb-leader / ovn-sb-leader / ovn-northd-leader
// label values on this pod so the kube-ovn Service selectors route NB/SB
// client traffic and northd liveness to the actual leaders.
func patchLeaderLabels(ctx context.Context, kc kubernetes.Interface, ns, name string,
	nb, sb, northd bool) error {

	patch := map[string]any{
		"metadata": map[string]any{
			"labels": map[string]any{
				"ovn-nb-leader":     fmt.Sprintf("%t", nb),
				"ovn-sb-leader":     fmt.Sprintf("%t", sb),
				"ovn-northd-leader": fmt.Sprintf("%t", northd),
			},
		},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = kc.CoreV1().Pods(ns).Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{})
	return err
}

