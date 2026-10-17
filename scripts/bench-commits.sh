#!/usr/bin/env bash
# Прогон ключевых бенчей на нескольких коммитах (git worktree).
# Требует Redis на localhost:6379. Результаты → stdout и docs/bench-results/<sha>.txt
#
# Usage:
#   ./scripts/bench-commits.sh
#   ./scripts/bench-commits.sh a96f70e 0959fb3 HEAD
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

  echo "=== ${sha} ${msg} ==="
  rm -rf "$wt"
  git -C "$ROOT" worktree add --detach "$wt" "$full" >/dev/null
  # Сценарные бенчи (compression) есть только на новых коммитах — копируем с HEAD при наличии.
  if [[ -f "${ROOT}/scenarios_bench_test.go" ]]; then
    # Не копируем на старые коммиты: API Compression* может отсутствовать.
    if git -C "$wt" grep -q 'CompressionZstd' -- '*.go' 2>/dev/null; then
      cp "${ROOT}/scenarios_bench_test.go" "$wt/"
      BENCH_REGEXP="${BENCH_REGEXP}|BenchmarkPayload|BenchmarkCompress|BenchmarkOrdered"
    fi
  fi

  {
    echo "# commit ${full}"
    echo "# ${msg}"
    echo "# date $(git -C "$ROOT" log -1 --format='%ci' "$full")"
    echo "# host $(hostname) go $(go env GOVERSION)"
    echo
    (cd "$wt" && go test -run='^$' -bench="${BENCH_REGEXP}" -benchtime=1s -count=3 -timeout=30m .)
  } | tee "$out"

  git -C "$ROOT" worktree remove --force "$wt" >/dev/null
  echo "wrote ${out}"
  echo
}

for c in "${COMMITS[@]}"; do
  run_one "$c"
done

echo "Done. Compare files in ${OUT_DIR}/"
