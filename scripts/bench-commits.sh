#!/usr/bin/env bash
# Прогон ключевых бенчей на нескольких коммитах (git worktree).
# Требует Redis на localhost:6379. Результаты → stdout и docs/bench-results/<sha>.txt
#
# Usage:
#   ./scripts/bench-commits.sh
#   ./scripts/bench-commits.sh a96f70e 0959fb3 HEAD
#   BENCH_CPU=1 ./scripts/bench-commits.sh 0959fb3 787467d HEAD   # blocked-group CPU benches
#   BENCH_SCAN=1 ./scripts/bench-commits.sh a96f70e HEAD          # partition-scan load (report S1000)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${ROOT}/docs/bench-results"
mkdir -p "$OUT_DIR"

COMMITS=("$@")
if [[ ${#COMMITS[@]} -eq 0 ]]; then
  COMMITS=(
    a96f70e7823454763a31d00c76fb842a7325d963
    0959fb375a2871bc50a7eac7af832e1df6f2ffe8
    HEAD
  )
fi

# Лёгкий набор для сравнения коммитов (FullCycle слишком долгий).
BENCH_REGEXP='BenchmarkPublish$|BenchmarkPublishBatch100|BenchmarkConsume$|BenchmarkConsumePrefetch/prefetch-10'

run_one() {
  local rev="$1"
  local sha
  sha="$(git -C "$ROOT" rev-parse --short "$rev")"
  local full
  full="$(git -C "$ROOT" rev-parse "$rev")"
  local msg
  msg="$(git -C "$ROOT" log -1 --format='%s' "$full")"
  local wt="${TMPDIR:-/tmp}/srq-bench-${sha}"
  local out="${OUT_DIR}/${sha}.txt"
  local regexp="$BENCH_REGEXP"

  echo "=== ${sha} ${msg} ==="
  git -C "$ROOT" worktree prune 2>/dev/null || true
  rm -rf "$wt"
  git -C "$ROOT" worktree add --detach "$wt" "$full" >/dev/null

  # Сценарные бенчи копируем с HEAD только если API уже есть на целевом SHA.
  if [[ "${BENCH_CPU:-}" != "1" && -f "${ROOT}/scenarios_bench_test.go" ]]; then
    if git -C "$wt" grep -q 'CompressionZstd' -- '*.go' 2>/dev/null; then
      cp "${ROOT}/scenarios_bench_test.go" "$wt/"
      regexp="${regexp}|BenchmarkPayload|BenchmarkCompress|BenchmarkOrdered"
    fi
  fi
  # CPU / blocked-group: нужен BlockGroups (с 0959fb3).
  local has_cpu=0
  if [[ -f "${ROOT}/scenarios_cpu_bench_test.go" ]] \
    && git -C "$wt" grep -q 'BlockGroups' -- 'queue.go' 2>/dev/null; then
    cp "${ROOT}/scenarios_cpu_bench_test.go" "$wt/"
    has_cpu=1
    if [[ "${BENCH_CPU:-}" != "1" ]]; then
      regexp="${regexp}|BenchmarkBlocked|BenchmarkParked|BenchmarkIdleReject"
    fi
  fi
  # Только CPU-сценарии → отдельный файл <sha>-cpu.txt (не затирает core).
  if [[ "${BENCH_CPU:-}" == "1" ]]; then
    out="${OUT_DIR}/${sha}-cpu.txt"
    regexp='BenchmarkBlocked|BenchmarkParked|BenchmarkIdleReject'
    if [[ "$has_cpu" != "1" ]]; then
      echo "skip ${sha}: no BlockGroups / cpu scenarios" | tee "$out"
      git -C "$ROOT" worktree remove --force "$wt" >/dev/null
      return 0
    fi
  fi

  # Partition-scan load (отчёт S1000): работает с a96f70e (SMEMBERS+:block).
  if [[ "${BENCH_SCAN:-}" == "1" ]]; then
    out="${OUT_DIR}/${sha}-scan.txt"
    if [[ ! -f "${ROOT}/scenarios_partition_scan_load_test.go" ]]; then
      echo "skip ${sha}: no scenarios_partition_scan_load_test.go" | tee "$out"
      git -C "$ROOT" worktree remove --force "$wt" >/dev/null
      return 0
    fi
    cp "${ROOT}/scenarios_partition_scan_load_test.go" "$wt/"
    {
      echo "# commit ${full}"
      echo "# ${msg}"
      echo "# date $(git -C "$ROOT" log -1 --format='%ci' "$full")"
      echo "# host $(hostname) go $(go env GOVERSION)"
      echo "# load TestPartitionScanLoad_S1000"
      echo "# env SRQ_SCAN_CHATS=${SRQ_SCAN_CHATS:-1000} SRQ_SCAN_DURATION=${SRQ_SCAN_DURATION:-20s} SRQ_SCAN_WORKERS=${SRQ_SCAN_WORKERS:-4}"
      echo
      (cd "$wt" && env \
        SRQ_SCAN_CHATS="${SRQ_SCAN_CHATS:-1000}" \
        SRQ_SCAN_DURATION="${SRQ_SCAN_DURATION:-20s}" \
        SRQ_SCAN_WORKERS="${SRQ_SCAN_WORKERS:-4}" \
        SRQ_SCAN_ACK_RATE="${SRQ_SCAN_ACK_RATE:-15}" \
        SRQ_EXPECT_SCAN_COST="${SRQ_EXPECT_SCAN_COST:-}" \
        go test -v -run='TestPartitionScanLoad_S1000' -count=1 -timeout=10m .)
    } | tee "$out"
    git -C "$ROOT" worktree remove --force "$wt" >/dev/null
    echo "wrote ${out}"
    echo
    return 0
  fi

  {
    echo "# commit ${full}"
    echo "# ${msg}"
    echo "# date $(git -C "$ROOT" log -1 --format='%ci' "$full")"
    echo "# host $(hostname) go $(go env GOVERSION)"
    echo "# bench_regexp ${regexp}"
    echo
    (cd "$wt" && go test -run='^$' -bench="${regexp}" -benchtime=1s -count=3 -timeout=45m .)
  } | tee "$out"

  git -C "$ROOT" worktree remove --force "$wt" >/dev/null
  echo "wrote ${out}"
  echo
}

for c in "${COMMITS[@]}"; do
  run_one "$c"
done

echo "Done. Compare files in ${OUT_DIR}/"
echo "CPU-only matrix: BENCH_CPU=1 ./scripts/bench-commits.sh 0959fb3 787467d HEAD"
echo "Partition-scan load: BENCH_SCAN=1 ./scripts/bench-commits.sh a96f70e HEAD"
