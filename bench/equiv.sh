#!/usr/bin/env bash
# Test two invariants:
#   1. Topology invariance, evix emits the same derivation set regardless of
#      how work is split (1 worker, N workers, remote-only workers, mixed
#      local+remote workers, and daemon warm graph queries).
#   2. Reference equivalence, that set matches nix-eval-jobs on the same input.
#
# A mismatch prints the offending diff and exits non-zero. nix-eval-jobs is
# optional: if it is not on PATH, only invariance (1) is checked.
#
# Usage: bench/equiv.sh [breadth depth]
#   With no args, sweeps a few breadth/depth shapes. With both args, runs just
#   that shape.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/.." && pwd)"
fixture="$here/fixture.nix"
work="$(mktemp -d)"
trap '[ -n "${wpid:-}" ] && kill "$wpid" 2>/dev/null || true; [ -n "${dpid:-}" ] && kill "$dpid" 2>/dev/null || true; rm -rf "$work"' EXIT

sys="$(nix eval --raw --impure --expr builtins.currentSystem)"

echo "building evix (release)..."
(cd "$root" && cargo build -q --release -p evix-cli)
evix="$root/target/release/evix"

# bench harness. Swap to an OS-assigned port if this ever races in CI.
port=$((20000 + RANDOM % 20000))
wpid=""
dpid=""
sock="$work/evix.sock"

# Normalize any NDJSON stream (evix or nix-eval-jobs) to a sorted, unique list
# of drvPaths. evix's attrset/error events carry no drvPath and drop out.
norm() { jq -r 'select(.drvPath) | .drvPath' "$1" | sort -u; }

run_local() {
	# workers, out, breadth, depth
	"$evix" eval --no-daemon --workers "$1" \
		--file "$fixture" --argstr system "$sys" --arg breadth "$3" --arg depth "$4" \
		>"$2"
}

start_worker() {
	"$evix" worker --listen "127.0.0.1:$port" >/dev/null 2>&1 &
	wpid=$!
	for _ in $(seq 1 100); do
		(echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null && break
		sleep 0.05
	done
}

stop_worker() {
	[ -n "$wpid" ] || return 0
	kill "$wpid" 2>/dev/null || true
	wait "$wpid" 2>/dev/null || true
	wpid=""
}

run_remote() {
	# out, breadth, depth
	"$evix" eval --no-daemon --workers 0 \
		--remote "127.0.0.1:$port" "$sys" 4 \
		--file "$fixture" --argstr system "$sys" --arg breadth "$2" --arg depth "$3" \
		>"$1"
}

run_mixed() {
	# out, breadth, depth
	"$evix" eval --no-daemon --workers 4 \
		--remote "127.0.0.1:$port" "$sys" 4 \
		--file "$fixture" --argstr system "$sys" --arg breadth "$2" --arg depth "$3" \
		>"$1"
}

start_daemon() {
	"$evix" daemon --foreground --socket "$sock" >/dev/null 2>&1 &
	dpid=$!
	for _ in $(seq 1 100); do
		[ -S "$sock" ] && break
		sleep 0.05
	done
}

stop_daemon() {
	[ -n "$dpid" ] || return 0
	kill "$dpid" 2>/dev/null || true
	wait "$dpid" 2>/dev/null || true
	dpid=""
}

run_daemon_query() {
	# out, breadth, depth
	"$evix" eval --socket "$sock" --workers 4 \
		--file "$fixture" --argstr system "$sys" --arg breadth "$2" --arg depth "$3" \
		>/dev/null
	"$evix" query --socket "$sock" --workers 4 \
		--file "$fixture" --argstr system "$sys" --arg breadth "$2" --arg depth "$3" \
		>"$1"
}

run_nej() {
	# out, breadth, depth. Fixture is positional for nix-eval-jobs
	nix-eval-jobs --gc-roots-dir "$work/gc" --workers 4 \
		"$fixture" --argstr system "$sys" --arg breadth "$2" --arg depth "$3" \
		>"$1" 2>"$work/nej.err"
}

have_nej=0
command -v nix-eval-jobs >/dev/null && have_nej=1 ||
	echo "WARN: nix-eval-jobs not found; skipping reference equivalence"

fail=0
if [ "$#" -ge 2 ]; then
	specs=("$1 $2")
else
	specs=("2 2" "4 2" "3 3" "6 1")
fi

for spec in "${specs[@]}"; do
	read -r b d <<<"$spec"
	echo "=== fixture breadth=$b depth=$d ($((b ** (d + 1))) derivations) ==="

	run_local 1 "$work/w1" "$b" "$d"
	norm "$work/w1" >"$work/base"
	count="$(grep -c . "$work/base" || true)"
	[ "$count" -eq 0 ] && {
		echo "FAIL: baseline found no derivations"
		fail=1
		continue
	}
	run_local 8 "$work/w8" "$b" "$d"
	start_worker
	run_remote "$work/wr" "$b" "$d"
	run_mixed "$work/wm" "$b" "$d"
	stop_worker
	start_daemon
	run_daemon_query "$work/wq" "$b" "$d"
	stop_daemon

	for variant in w8 wr wm wq; do
		if ! diff "$work/base" <(norm "$work/$variant") >"$work/d.$variant"; then
			echo "FAIL: topology '$variant' diverged from single-worker baseline:"
			cat "$work/d.$variant"
			fail=1
		else
			echo "  ok: topology '$variant' matches single-worker baseline"
		fi
	done

	if [ "$have_nej" -eq 1 ]; then
		run_nej "$work/nej" "$b" "$d"
		if ! diff "$work/base" <(norm "$work/nej") >"$work/d.nej"; then
			echo "FAIL: nix-eval-jobs diverged from evix:"
			cat "$work/d.nej"
			fail=1
		else
			echo "  ok: matches nix-eval-jobs ($count derivations)"
		fi
	fi
done

[ "$fail" -eq 0 ] && echo "ALL EQUIVALENT" || {
	echo "DIVERGENCE DETECTED"
	exit 1
}
