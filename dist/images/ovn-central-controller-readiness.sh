#!/usr/bin/env bash
# Readiness probe for ovn-central in DYNAMIC_PEERS mode.
#
# Returns 0 only when BOTH NB and SB ovsdb report:
#   Status: cluster member
#   Leader: <known sid> (not "unknown")
#
# Other pods' ovn-central-controller treats Pod.Ready=true as the
# trustworthy "this peer is in a working cluster" signal, so this probe
# must reflect actual raft health -- not just process liveness.

set -u

check_db() {
    local sock=$1 name=$2 out leader
    if ! out=$(ovs-appctl -t "$sock" cluster/status "$name" 2>/dev/null); then
        return 1
    fi
    grep -q '^Status: cluster member' <<<"$out" || return 1
    leader=$(awk '/^Leader:/ {print $2; exit}' <<<"$out")
    [[ -n "$leader" && "$leader" != "unknown" ]]
}

check_db /var/run/ovn/ovnnb_db.ctl OVN_Northbound || exit 1
check_db /var/run/ovn/ovnsb_db.ctl OVN_Southbound || exit 1
exit 0
