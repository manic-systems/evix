#!/usr/bin/env bash
# Wall-clock benchmark of evix evaluation vs nix-eval-jobs on a fixed fixture,
# using hyperfine. Covers evix local-only evaluation, remote-only distributed
# evaluation, mixed local+remote distributed evaluation, daemon prewarming,
# warm daemon graph queries, and nix-eval-jobs as the reference.
#
# Usage: bench/bench.sh [breadth] [depth]   (defaults: breadth=6 depth=3)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/.." && pwd)"
fixture="$here/fixture.nix"
b="${1:-6}"
d="${2:-3}"
sys="$(nix eval --raw --impure --expr builtins.currentSystem)"
gc="$(mktemp -d)"
sock="$gc/evix.sock"
wpid=""
dpid=""
trap '[ -n "$wpid" ] && kill "$wpid" 2>/dev/null || true; [ -n "$dpid" ] && kill "$dpid" 2>/dev/null || true; rm -rf "$gc"' EXIT

echo "building evix (release)..."
(cd "$root" && cargo build -q --release -p evix-cli)
evix="$root/target/release/evix"
echo "fixture: breadth=$b depth=$d => $((b ** (d + 1))) derivations"

# evix takes the fixture via --file; nix-eval-jobs takes it positionally.
fargs=(--argstr system "$sys" --arg breadth "$b" --arg depth "$d")
args=(--file "$fixture" "${fargs[@]}")
printf -v args_str ' %q' "${args[@]}"
printf -v fargs_str ' %q' "${fargs[@]}"

port=$((20000 + RANDOM % 20000))
"$evix" worker --listen "127.0.0.1:$port" --insecure-tokenless-remote >/dev/null 2>&1 &
wpid=$!
for _ in $(seq 1 100); do
	(echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null && break
	sleep 0.05
done

"$evix" daemon --foreground --socket "$sock" >/dev/null 2>&1 &
dpid=$!
for _ in $(seq 1 100); do
	[ -S "$sock" ] && break
	sleep 0.05
done
"$evix" eval --socket "$sock" --workers 4$args_str >/dev/null

cmds=(
	-n "evix local=1" "$evix eval --no-daemon --workers 1$args_str >/dev/null"
	-n "evix local=4" "$evix eval --no-daemon --workers 4$args_str >/dev/null"
	-n "evix local=8" "$evix eval --no-daemon --workers 8$args_str >/dev/null"
	-n "evix distributed remote=4" "$evix eval --no-daemon --workers 0 --insecure-tokenless-remote --remote 127.0.0.1:$port $sys 4$args_str >/dev/null"
	-n "evix distributed local=4 remote=4" "$evix eval --no-daemon --workers 4 --insecure-tokenless-remote --remote 127.0.0.1:$port $sys 4$args_str >/dev/null"
	-n "evix daemon prewarm local=4" "$evix eval --socket $sock --workers 4$args_str >/dev/null"
	-n "evix daemon warm query full local=4" "$evix query --socket $sock --workers 4$args_str >/dev/null"
	-n "evix daemon warm query n0 local=4" "$evix query --socket $sock --workers 4 --attr-prefix n0$args_str >/dev/null"
)
if command -v nix-eval-jobs >/dev/null; then
	cmds+=(-n "nix-eval-jobs w=4" "nix-eval-jobs --gc-roots-dir $gc --workers 4 $fixture$fargs_str >/dev/null")
else
	echo "WARN: nix-eval-jobs not found; benchmarking evix only"
fi

hyperfine --warmup 1 --runs 5 "${cmds[@]}" --export-markdown "$here/results.md"
echo "wrote $here/results.md"
