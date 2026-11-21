#!/usr/bin/env bash
# Check the injected remote action environment against the shipped rootfs.
set -euo pipefail

: "${GANG_BAZELRC:?GANG_BAZELRC must be set by the Nix devShell}"
: "${GANG_E2E_ACTION_ROOTFS:?GANG_E2E_ACTION_ROOTFS must be set by the Nix devShell}"
: "${GANG_E2E_BWRAP:?GANG_E2E_BWRAP must be set by the Nix devShell}"
BAZEL=${BAZEL:-bazel}

scratch=$(mktemp -d /tmp/gang-action-shell.XXXXXX)
startup=(--noworkspace_rc --nohome_rc --output_base="$scratch/bazel-output" --bazelrc="$GANG_BAZELRC")
cleanup() {
  local status=$?
  if ! "$BAZEL" "${startup[@]}" shutdown; then
    echo 'failed to shut down the action-shell Bazel server' >&2
    if (( status == 0 )); then status=1; fi
  fi
  if ! rm -rf "$scratch"; then
    echo 'failed to remove action-shell temporary directory' >&2
    if (( status == 0 )); then status=1; fi
  fi
  exit "$status"
}
trap cleanup EXIT

query_args=()
if [[ -n ${BAZEL_REPOSITORY_CACHE:-} ]]; then
  query_args+=(--repository_cache="$BAZEL_REPOSITORY_CACHE")
fi
"$BAZEL" "${startup[@]}" \
  aquery 'mnemonic("JsRunBinary|Genrule", deps(//crates/gang-ui:assets))' \
  --spawn_strategy=remote --incompatible_strict_action_env --output=jsonproto "${query_args[@]}" >"$scratch/actions.json"

if ! jq -e '
  [.actions[]? | select(.mnemonic == "JsRunBinary")] | length > 0
' "$scratch/actions.json" >/dev/null; then
  echo 'aquery returned no JsRunBinary actions' >&2
  exit 1
fi
if ! jq -e '
  [.actions[]? | select(.mnemonic == "Genrule")] | length > 0
' "$scratch/actions.json" >/dev/null; then
  echo 'aquery returned no Genrule actions' >&2
  exit 1
fi
if ! jq -e '
  [.actions[]? | select(.mnemonic == "JsRunBinary" or .mnemonic == "Genrule") |
    [.environmentVariables[]? | select(.key == "PATH") | .value] |
    length == 1 and (.[0] | type == "string" and (split(":") | all(. == "/bin" or . == "/usr/bin" or . == "/usr/local/bin")))] | all
' "$scratch/actions.json" >/dev/null; then
  echo 'every JsRunBinary and Genrule action must have exactly one PATH containing only FHS tool directories' >&2
  exit 1
fi
if ! jq -e '
  [.actions[]? | select(.mnemonic == "Genrule") | .arguments[0] == "/bin/bash"] | all
' "$scratch/actions.json" >/dev/null; then
  echo 'every Genrule action must start with /bin/bash' >&2
  exit 1
fi
js_path=$(jq -r '[.actions[] | select(.mnemonic == "JsRunBinary")][0].environmentVariables[] | select(.key == "PATH") | .value' "$scratch/actions.json")
genrule_shell=$(jq -r '[.actions[] | select(.mnemonic == "Genrule")][0].arguments[0]' "$scratch/actions.json")

root=$(realpath "$GANG_E2E_ACTION_ROOTFS/root")
find "$root" -mindepth 1 -maxdepth 1 -print0 >"$scratch/roots"
LC_ALL=C sort -z "$scratch/roots" >"$scratch/sorted-roots"
args=(--unshare-all --unshare-user --uid 1000 --gid 1000 --die-with-parent --new-session --clearenv)
while IFS= read -r -d '' source; do
  destination="/${source##*/}"
  case "$destination" in
    /proc|/dev|/tmp|/gang-reset)
      printf 'reserved rootfs entry: %s\n' "$source" >&2
      exit 1
      ;;
  esac
  if [[ -L $source ]]; then
    target=$(readlink "$source")
    case "/$target/" in
      //*|*/../*)
        printf 'invalid rootfs symlink: %s -> %s\n' "$source" "$target" >&2
        exit 1
        ;;
    esac
    args+=(--symlink "$target" "$destination")
  elif [[ -d $source ]]; then
    args+=(--ro-bind "$source" "$destination")
  else
    printf 'unsupported rootfs entry: %s\n' "$source" >&2
    exit 1
  fi
done <"$scratch/sorted-roots"

printf '#!/usr/bin/env bash\nprintf shebang-ok\n' >"$scratch/script"
chmod +x "$scratch/script"
args+=(--proc /proc --dev /dev --tmpfs /tmp --ro-bind "$scratch/script" /script
  --setenv PATH "$js_path" --remount-ro / --)

# An external bwrap is required to exercise the product's actual rootfs; Nix pins it.
check_output() {
  local expected=$1
  shift
  printf '%s' "$expected" >"$scratch/expected"
  if ! "$GANG_E2E_BWRAP" "${args[@]}" "$@" >"$scratch/output"; then
    printf 'rootfs command failed: %s\n' "$*" >&2
    return 1
  fi
  if ! cmp -s "$scratch/expected" "$scratch/output"; then
    printf 'unexpected output from rootfs command: %s (expected %s)\n' "$*" "$expected" >&2
    return 1
  fi
}
check_output shebang-ok /script
check_output env-bash-ok /usr/bin/env bash -c 'printf env-bash-ok'
check_output genrule-ok "$genrule_shell" -c 'printf genrule-ok'
printf 'Action shell regression passed\n'
