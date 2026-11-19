#!/usr/bin/env bash
# Stage only Bazel's reusable cache entries after its server has stopped.
set -euo pipefail
export LC_ALL=C

fail() { echo "ci-cache: $*" >&2; exit 1; }

# Input paths must not contain traversal, even when they would resolve inside a root.
path_ok() {
  [[ -n $1 && /$1/ != */../* && /$1/ != */./* ]] || fail "invalid path: $1"
}

canonical() {
  local path=$1
  path_ok "$path"
  while [[ $path != / && $path == */ ]]; do path=${path%/}; done
  [[ ! -L $path ]] || fail "symlink root: $path"
  realpath -m -- "$path"
}

separate() {
  local a=$1 b=$2
  [[ $a != / && $b != / ]] || fail 'cache paths must not be filesystem root'
  [[ $a != "$b" && $a != "$b"/* && $b != "$a"/* ]] || fail "overlapping paths: $a and $b"
}

fresh_destination() {
  local dest=$1
  if [[ -e $dest ]]; then
    [[ -d $dest ]] || fail "destination is not a directory: $dest"
    [[ -z $(find "$dest" -mindepth 1 -print -quit) ]] || fail "destination is not empty: $dest"
  else
    mkdir -p -- "$dest"
  fi
  mkdir -p -- "$dest/disk" "$dest/repository/content_addressable"
}

# Sets kind to ac, cas, repo, or dir for a permitted staging path.
classify() {
  local path=$1 type=$2 prefix hash
  kind=
  if [[ $type == d ]]; then
    if [[ $path == disk || $path == repository || $path == repository/content_addressable || $path == repository/content_addressable/sha256 || $path =~ ^repository/content_addressable/sha256/[0-9a-f]{64}$ || $path =~ ^disk/([a-z0-9]+/)?(ac|cas)(/[0-9a-f]{2})?$ || $path =~ ^disk/[a-z0-9]+$ ]]; then
      kind=dir
    fi
  elif [[ $type == f ]]; then
    if [[ $path =~ ^disk/([a-z0-9]+/)?(ac|cas)/([0-9a-f]{2})/([0-9a-f]{64})$ ]]; then
      kind=${BASH_REMATCH[2]}; prefix=${BASH_REMATCH[3]}; hash=${BASH_REMATCH[4]}
      [[ ${hash:0:2} == "$prefix" ]] || kind=
    elif [[ $path =~ ^repository/content_addressable/sha256/[0-9a-f]{64}/(file|id-[^/]+)$ ]]; then
      kind=repo
    fi
  fi
}

# Validate every entry, including directories and special files, before copying.
validate_tree() {
  local root=$1 item rel type
  [[ -d $root && ! -L $root ]] || fail "invalid source directory: $root"
  find "$root" -mindepth 1 -print0 >"$scratch/entries"
  while IFS= read -r -d '' item; do
    rel=${item#"$root"/}
    if [[ -L $item ]]; then fail "symlink in cache: $item"
    elif [[ -d $item ]]; then type=d
    elif [[ -f $item ]]; then type=f
    else fail "non-regular cache entry: $item"
    fi
    classify "$rel" "$type"
    [[ -n $kind ]] || fail "unknown cache path: $item"
    [[ $kind != repo || ${rel##*/} == file || -f ${item%/*}/file ]] || fail "repository marker without payload: $item"
  done <"$scratch/entries"
}

copy_file() {
  local src=$1 dst=$2
  mkdir -p -- "${dst%/*}"
  cp -p -- "$src" "$dst"
}

scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT

case ${1-} in
  export)
    (( $# == 5 )) || fail 'usage: export DISK_CACHE REPOSITORY_CACHE EPOCH DESTINATION'
    [[ $4 =~ ^[0-9]+$ ]] || fail 'epoch must be a nonnegative integer'
    epoch=$4
    disk=$(canonical "$2"); repo=$(canonical "$3"); dest=$(canonical "$5")
    separate "$dest" "$disk"; separate "$dest" "$repo"
    fresh_destination "$dest"
    if [[ -e $disk ]]; then
      [[ -d $disk ]] || fail "not a directory: $disk"
      find "$disk" -type f -printf '%T@ %p\0' >"$scratch/files"
      while IFS= read -r -d '' entry; do
        timestamp=${entry%% *}; file=${entry#* }; rel=${file#"$disk"/}
        [[ $timestamp != -* ]] || continue
        classify "disk/$rel" f
        [[ $kind == ac || $kind == cas ]] || continue
        (( 10#${timestamp%%.*} >= 10#$epoch )) || continue
        copy_file "$file" "$dest/disk/$rel"
      done <"$scratch/files"
    fi
    if [[ -e $repo ]]; then
      [[ -d $repo ]] || fail "not a directory: $repo"
      find "$repo" -type f -printf '%T@ %p\0' >"$scratch/files"
      while IFS= read -r -d '' entry; do
        timestamp=${entry%% *}; file=${entry#* }; rel=${file#"$repo"/}
        [[ $timestamp != -* ]] || continue
        [[ $rel =~ ^content_addressable/sha256/[0-9a-f]{64}/file$ ]] || continue
        (( 10#${timestamp%%.*} >= 10#$epoch )) || continue
        copy_file "$file" "$dest/repository/$rel"
        dir=${file%/*}
        find "$dir" -mindepth 1 -maxdepth 1 -type f -name 'id-?*' -print0 >"$scratch/markers"
        while IFS= read -r -d '' marker; do
          copy_file "$marker" "$dest/repository/${rel%/*}/${marker##*/}"
        done <"$scratch/markers"
      done <"$scratch/files"
    fi
    ;;
  merge)
    (( $# >= 3 )) || fail 'usage: merge DESTINATION SOURCE...'
    dest=$(canonical "$2"); shift 2
    sources=()
    for source in "$@"; do
      source=$(canonical "$source")
      separate "$dest" "$source"
      validate_tree "$source"
      sources+=("$source")
    done
    fresh_destination "$dest"
    # Lexical order makes equal-mtime action collisions independent of argument order.
    printf '%s\0' "${sources[@]}" | sort -z >"$scratch/sources"
    while IFS= read -r -d '' source; do
      find "$source" -type f -print0 >"$scratch/files"
      while IFS= read -r -d '' file; do
        rel=${file#"$source"/}; target=$dest/$rel
        classify "$rel" f
        if [[ -e $target ]]; then
          if [[ $kind == ac ]]; then
            [[ $file -nt $target ]] || continue
          else
            cmp -s -- "$file" "$target" || fail "conflicting cache entry: $rel"
            [[ $file -nt $target ]] || continue
          fi
        fi
        copy_file "$file" "$target"
      done <"$scratch/files"
    done <"$scratch/sources"
    ;;
  limit)
    (( $# == 3 )) || fail 'usage: limit DESTINATION MAX_COMPRESSED_BYTES'
    if [[ ! $3 =~ ^[0-9]+$ ]] || ! (( 10#$3 > 0 )); then
      fail 'limit must be a positive integer'
    fi
    max=$((10#$3)); dest=$(canonical "$2")
    validate_tree "$dest"
    measure() {
      tar -C "$dest" -cf - . | zstd -q -3 -T0 -o "$scratch/cache.zst" -f
      bytes=$(wc -c <"$scratch/cache.zst")
    }
    measure
    before=$bytes
    # Estimate the raw bytes to evict from the current compression ratio.
    # Recompress after each oldest-first batch to account for imperfect estimates.
    estimate() {
      raw=$(du -sb -- "$dest")
      raw=${raw%%$'\t'*}
      needed=$(awk -v raw="$raw" -v bytes="$bytes" -v max="$max" 'BEGIN {
        n = raw * (bytes - max) / bytes
        printf "%.0f", int(n) + (n > int(n))
      }')
      (( needed > 0 )) || needed=1
    }
    : >"$scratch/oldest"
    if [[ -d $dest/disk ]]; then
      find "$dest/disk" -type f -printf '%T@ %p\0' | sort -zn >"$scratch/oldest"
    fi
    exec 3<"$scratch/oldest"
    while (( bytes > max )); do
      estimate
      removed=0
      count=0
      while (( removed < needed )) && IFS= read -r -d '' entry <&3; do
        file=${entry#* }
        size=$(stat -c %s -- "$file")
        rm -- "$file"
        removed=$((removed + size))
        count=$((count + 1))
      done
      (( count > 0 )) || break
      find "$dest/disk" -depth -mindepth 1 -type d -empty -delete
      measure
    done
    exec 3<&-
    if (( bytes > max )) && [[ -d $dest/repository/content_addressable ]]; then
      find "$dest/repository/content_addressable" -type f -name file -printf '%T@ %h\0' | sort -zn >"$scratch/oldest"
      exec 3<"$scratch/oldest"
      while (( bytes > max )); do
        estimate
        removed=0
        count=0
        while (( removed < needed )) && IFS= read -r -d '' entry <&3; do
          dir=${entry#* }
          size=$(du -sb -- "$dir")
          size=${size%%$'\t'*}
          rm -r -- "$dir"
          removed=$((removed + size))
          count=$((count + 1))
        done
        (( count > 0 )) || break
        measure
      done
      exec 3<&-
    fi
    printf 'ci-cache: compressed bytes before=%s after=%s limit=%s\n' "$before" "$bytes" "$max" >&2
    (( bytes <= max )) || fail 'cache cannot fit limit'
    ;;
  *) fail 'usage: ci-cache.sh {export|merge|limit} ...' ;;
esac
