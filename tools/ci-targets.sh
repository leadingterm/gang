#!/usr/bin/env bash
# Partition newline-delimited Bazel labels into deterministic, one-based shards.
set -euo pipefail
export LC_ALL=C

if (( $# != 2 )) || [[ ! $1 =~ ^[0-9]+$ || ! $2 =~ ^[0-9]+$ ]]; then
  echo "Usage: $0 INDEX COUNT (positive decimal integers, INDEX <= COUNT)" >&2
  exit 2
fi

index=$1
count=$2
# Normalize without arithmetic so leading zeroes and large values are safe.
while [[ ${#index} -gt 1 && $index == 0* ]]; do index=${index#0}; done
while [[ ${#count} -gt 1 && $count == 0* ]]; do count=${count#0}; done
if [[ $index == 0 || $count == 0 ]] ||
  (( ${#index} > ${#count} )) ||
  { (( ${#index} == ${#count} )) && [[ $index > $count ]]; }; then
  echo 'INDEX and COUNT must be positive, and INDEX must not exceed COUNT' >&2
  exit 2
fi

workdir=$(mktemp -d "${TMPDIR:-/tmp}/ci-targets.XXXXXXXX")
trap 'rm -rf "$workdir"' EXIT

# Validate every input label before producing any shard output.
while IFS= read -r label || [[ -n $label ]]; do
  case $label in
    *[[:space:]]*) echo "Invalid target label: $label" >&2; exit 1 ;;
    //*:* | @*:*) printf '%s\n' "$label" ;;
    *) echo "Invalid target label: $label" >&2; exit 1 ;;
  esac
done >"$workdir/targets"

sort -u "$workdir/targets" >"$workdir/sorted"
shard=1
while IFS= read -r label; do
  if [[ $shard == "$index" ]]; then
    printf '%s\n' "$label"
  fi
  if [[ $shard == "$count" ]]; then
    shard=1
  else
    shard=$((shard + 1))
  fi
done <"$workdir/sorted"
