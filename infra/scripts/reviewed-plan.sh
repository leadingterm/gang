#!/usr/bin/env bash
set -euo pipefail

# Glue of the deploy workflow (.github/workflows/deploy.yml). It runs tofu, aws, gh, git and date,
# and pipes their JSON to the hidden `gang deploy` commands, which make every decision: the plan
# policy, the manifest, the plan key and age, and the checks of the plan run and the plan object
# (crates/gang-deploy).

ROOT="${ROOT:-.}"
# The gang executable, as the login action's `gang` input names it.
GANG="${GANG:-gang}"

die() {
  printf 'reviewed-plan: %s\n' "$*" >&2
  exit 1
}

log() {
  printf 'reviewed-plan: %s\n' "$*" >&2
}

require() {
  local var missing=0
  for var in "$@"; do
    if [[ -z "${!var:-}" ]]; then
      log "missing required environment variable: $var"
      missing=1
    fi
  done
  (( missing == 0 )) || exit 1
}

# Checked before any API call that puts the value in a URL or a request.
require_number() {
  [[ "${!1:-}" =~ ^[0-9]+$ ]] || die "$1 must be numeric"
}

# Prints the parents of HEAD as --parent=<sha> options, in order. `git rev-list --parents` reports
# zero parents in a shallow clone; the raw commit object does not.
parent_options() {
  local object
  object="$(git cat-file -p HEAD)" || die "cannot read the HEAD commit"
  sed -n '/^$/q; s/^parent /--parent=/p' <<<"$object"
}

cmd_validate() {
  tofu -chdir="$ROOT" fmt -check -recursive -no-color
  tofu -chdir="$ROOT" init -backend=false -input=false
  tofu -chdir="$ROOT" validate -no-color
}

cmd_init() {
  require INFRA_STATE_BUCKET INFRA_STATE_KEY INFRA_STATE_REGION INFRA_STATE_KMS_KEY_ARN
  tofu -chdir="$ROOT" init -input=false -reconfigure \
    -backend-config="bucket=$INFRA_STATE_BUCKET" \
    -backend-config="key=$INFRA_STATE_KEY" \
    -backend-config="region=$INFRA_STATE_REGION" \
    -backend-config="kms_key_id=$INFRA_STATE_KMS_KEY_ARN" \
    -backend-config=encrypt=true \
    -backend-config=use_lockfile=true
}

cmd_plan() {
  require RUNNER_TEMP GITHUB_OUTPUT
  local plan_file="$RUNNER_TEMP/infra.tfplan"
  local json_file="$RUNNER_TEMP/infra-plan.json"
  local rc=0 policy_rc=0

  tofu -chdir="$ROOT" plan -detailed-exitcode -input=false -lock=false -out="$plan_file" || rc=$?
  case "$rc" in
    0 | 2) ;;
    *) die "tofu plan failed with exit status $rc" ;;
  esac

  tofu -chdir="$ROOT" show -json "$plan_file" >"$json_file"
  "$GANG" deploy policy <"$json_file" >>"$GITHUB_OUTPUT" || policy_rc=$?
  case "$policy_rc" in
    0) ;;
    3)
      log "this revision must not merge"
      exit 3
      ;;
    *) die "policy check could not read $json_file" ;;
  esac
}

cmd_store() {
  require GITHUB_SHA GITHUB_REPOSITORY GITHUB_REPOSITORY_ID GITHUB_RUN_ID GITHUB_RUN_ATTEMPT PR_NUMBER PR_HEAD_SHA \
    RUNNER_TEMP INFRA_PLAN_BUCKET INFRA_PLAN_PREFIX INFRA_STATE_KMS_KEY_ARN INFRA_PLAN_MAX_AGE

  local plan_file="$RUNNER_TEMP/infra.tfplan"
  local json_file="$RUNNER_TEMP/infra-plan.json"
  local request="$RUNNER_TEMP/store-request.json"
  local manifest="$RUNNER_TEMP/manifest.json"
  local head tree now parent_text key metadata version
  local -a parents=()

  head="$(git rev-parse HEAD)"
  tree="$(git rev-parse 'HEAD^{tree}')"
  parent_text="$(parent_options)"
  [[ -z "$parent_text" ]] || mapfile -t parents <<<"$parent_text"
  now="$(date -u +%s)"

  "$GANG" deploy store-request \
    --repository "$GITHUB_REPOSITORY" --repository-id "$GITHUB_REPOSITORY_ID" \
    --pr-number "$PR_NUMBER" --pr-head-sha "$PR_HEAD_SHA" \
    --sha "$GITHUB_SHA" --head "$head" "${parents[@]}" --tree-sha "$tree" \
    --run-id "$GITHUB_RUN_ID" --run-attempt "$GITHUB_RUN_ATTEMPT" \
    --prefix "$INFRA_PLAN_PREFIX" --max-age "$INFRA_PLAN_MAX_AGE" \
    --now "$now" --plan-file "$plan_file" <"$json_file" >"$request"
  key="$(jq -r .plan_key "$request")"
  metadata="$(jq -r .metadata "$request")"

  version="$(aws s3api put-object --bucket "$INFRA_PLAN_BUCKET" --key "$key" --body "$plan_file" \
    --metadata "$metadata" --server-side-encryption aws:kms --ssekms-key-id "$INFRA_STATE_KMS_KEY_ARN" \
    --if-none-match '*' --query VersionId --output text)" || die "could not store plan at $key"
  "$GANG" deploy manifest --plan-version "$version" <"$request" >"$manifest.tmp"
  mv "$manifest.tmp" "$manifest"
}

cmd_publish() {
  require GITHUB_REPOSITORY PR_NUMBER GH_TOKEN RUNNER_TEMP
  require_number PR_NUMBER

  local payload="$RUNNER_TEMP/infra-comment.json"
  local comments="$RUNNER_TEMP/infra-comments.json"
  local plan_json="$RUNNER_TEMP/infra-plan.json"
  local manifest="$RUNNER_TEMP/manifest.json"
  local plan_input=/dev/null tree comment_id
  local -a tree_source

  # A plan that did not complete leaves no JSON; the comment then says so.
  [[ ! -s "$plan_json" ]] || plan_input="$plan_json"
  if [[ -f "$manifest" ]]; then
    tree_source=(--manifest "$manifest")
  else
    tree="$(git rev-parse 'HEAD^{tree}')"
    tree_source=(--tree "$tree")
  fi
  "$GANG" deploy comment "${tree_source[@]}" <"$plan_input" >"$payload"

  gh api --paginate "repos/$GITHUB_REPOSITORY/issues/$PR_NUMBER/comments" | jq -s 'add // []' >"$comments"
  comment_id="$("$GANG" deploy comment-id <"$comments")"

  if [[ -n "$comment_id" ]]; then
    gh api --method PATCH "repos/$GITHUB_REPOSITORY/issues/comments/$comment_id" --input "$payload" >/dev/null
  else
    gh api --method POST "repos/$GITHUB_REPOSITORY/issues/$PR_NUMBER/comments" --input "$payload" >/dev/null
  fi
}

cmd_resolve() {
  require GH_TOKEN GITHUB_REPOSITORY GITHUB_REPOSITORY_ID INFRA_PLAN_PREFIX GITHUB_SHA GITHUB_REF GITHUB_WORKFLOW_REF \
    RUNNER_TEMP

  local pulls="$RUNNER_TEMP/infra-commit-pulls.json"
  local comments="$RUNNER_TEMP/infra-comments.json"
  local candidate="$RUNNER_TEMP/infra-plan-comment-manifest.json"
  local run_file="$RUNNER_TEMP/infra-plan-run.json"
  local runs="$RUNNER_TEMP/infra-runs.json"
  local manifest="$RUNNER_TEMP/manifest.json"
  local head tree parent_text pr_line pr pr_head run_id workflow
  local -a parents=()

  head="$(git rev-parse HEAD)"
  tree="$(git rev-parse 'HEAD^{tree}')"
  parent_text="$(parent_options)"
  [[ -z "$parent_text" ]] || mapfile -t parents <<<"$parent_text"

  # Each fetch needs what the previous one named: the pull request, then the run of its plan.
  gh api "repos/$GITHUB_REPOSITORY/commits/$GITHUB_SHA/pulls" >"$pulls"
  pr_line="$("$GANG" deploy merged-pr --sha "$GITHUB_SHA" --repository "$GITHUB_REPOSITORY" <"$pulls")"
  read -r pr pr_head <<<"$pr_line"
  gh api --paginate "repos/$GITHUB_REPOSITORY/issues/$pr/comments" | jq -s 'add // []' >"$comments"
  "$GANG" deploy plan-comment <"$comments" >"$candidate"
  run_id="$(jq -r .workflow_run_id "$candidate")"
  gh api "repos/$GITHUB_REPOSITORY/actions/runs/$run_id" >"$run_file"
  workflow="$("$GANG" deploy workflow-file --ref "$GITHUB_WORKFLOW_REF")"
  gh api --paginate "repos/$GITHUB_REPOSITORY/actions/workflows/$workflow/runs?event=pull_request&head_sha=$pr_head&per_page=100" \
    | jq -s '{workflow_runs: (map(.workflow_runs) | add // [])}' >"$runs"

  # shellcheck disable=SC2016
  jq -n --slurpfile pulls "$pulls" --slurpfile comments "$comments" --slurpfile run "$run_file" \
    --slurpfile runs "$runs" '{pulls: $pulls[0], comments: $comments[0], run: $run[0], runs: $runs[0]}' \
    | "$GANG" deploy resolve --ref "$GITHUB_REF" --head "$head" --sha "$GITHUB_SHA" "${parents[@]}" \
      --tree-sha "$tree" --repository "$GITHUB_REPOSITORY" --repository-id "$GITHUB_REPOSITORY_ID" \
      --prefix "$INFRA_PLAN_PREFIX" --workflow-ref "$GITHUB_WORKFLOW_REF" >"$manifest.tmp"
  mv "$manifest.tmp" "$manifest"
}

cmd_fetch() {
  require INFRA_PLAN_BUCKET INFRA_PLAN_MAX_AGE RUNNER_TEMP

  local manifest="$RUNNER_TEMP/manifest.json"
  local dest="$RUNNER_TEMP/infra.tfplan"
  local key version versions object now

  key="$(jq -r .plan_key "$manifest")"
  version="$(jq -r .plan_version "$manifest")"
  versions="$(aws s3api list-object-versions --bucket "$INFRA_PLAN_BUCKET" --prefix "$key" --output json)" \
    || die "could not list versions of plan object $key"
  "$GANG" deploy check-versions --manifest "$manifest" <<<"$versions"

  object="$(aws s3api head-object --bucket "$INFRA_PLAN_BUCKET" --key "$key" --version-id "$version" --output json)" \
    || die "could not read plan object $key"
  now="$(date -u +%s)"
  "$GANG" deploy check-object --manifest "$manifest" --now "$now" \
    --max-age "$INFRA_PLAN_MAX_AGE" <<<"$object"

  aws s3api get-object --bucket "$INFRA_PLAN_BUCKET" --key "$key" --version-id "$version" "$dest" >/dev/null \
    || { rm -f "$dest"; die "could not fetch plan object $key"; }
  "$GANG" deploy check-plan --manifest "$manifest" --plan-file "$dest" || { rm -f "$dest"; exit 1; }
}

cmd_verify() {
  require RUNNER_TEMP GITHUB_OUTPUT
  local json_file="$RUNNER_TEMP/infra-plan.json"
  local outputs

  tofu -chdir="$ROOT" show -json "$RUNNER_TEMP/infra.tfplan" >"$json_file"
  outputs="$("$GANG" deploy policy <"$json_file")" || die "reviewed plan is blocked by policy"
  printf '%s\n' "$outputs" >>"$GITHUB_OUTPUT"
}

cmd_apply() {
  require RUNNER_TEMP HAS_CHANGES INFRA_LOCK_TIMEOUT
  if [[ "$HAS_CHANGES" != true ]]; then
    log "reviewed plan has no changes; nothing to apply"
    return 0
  fi
  tofu -chdir="$ROOT" apply -input=false -lock-timeout="$INFRA_LOCK_TIMEOUT" "$RUNNER_TEMP/infra.tfplan"
}

cmd_converge() {
  require INFRA_LOCK_TIMEOUT
  local rc=0
  tofu -chdir="$ROOT" plan -detailed-exitcode -input=false -lock-timeout="$INFRA_LOCK_TIMEOUT" || rc=$?
  case "$rc" in
    0) log "root converged" ;;
    2) die "root is not converged after apply: a follow-up plan has changes" ;;
    *) die "convergence plan failed with exit status $rc" ;;
  esac
}

cmd_restart() {
  require RUNNER_TEMP
  local plan="$RUNNER_TEMP/infra-plan.json"
  local needed gang region controller command_id status wait_rc=0

  needed="$("$GANG" deploy restart-needed <"$plan")" || die "cannot parse $plan"
  if [[ "$needed" != true ]]; then
    log "no configuration change on a running controller; restart not needed"
    return 0
  fi

  gang="$(tofu -chdir="$ROOT" output -json gang)"
  region="$(jq -er '.region' <<<"$gang")" || die "output gang has no region"
  controller="$(jq -er '.controller_instance_id' <<<"$gang")" || die "output gang has no controller_instance_id"

  command_id="$(aws ssm send-command --region "$region" --document-name AWS-RunShellScript \
    --instance-ids "$controller" \
    --parameters 'commands=["systemctl restart gang-config.service"]' \
    --query Command.CommandId --output text)"
  aws ssm wait command-executed --region "$region" --command-id "$command_id" --instance-id "$controller" || wait_rc=$?
  status="$(aws ssm get-command-invocation --region "$region" --command-id "$command_id" --instance-id "$controller" \
    --query Status --output text)"
  if [[ "$status" != Success ]]; then
    die "restart on $controller ended $status (waiter exit $wait_rc): $(aws ssm get-command-invocation \
      --region "$region" --command-id "$command_id" --instance-id "$controller" \
      --query StandardErrorContent --output text)"
  fi
  log "gang-config restarted on $controller"
}

usage() {
  printf 'usage: %s {validate|init|plan|store|publish|resolve|fetch|verify|apply|converge|restart}\n' "$0" >&2
  exit 2
}

main() {
  (( $# == 1 )) || usage
  case "$1" in
    validate) cmd_validate ;;
    init) cmd_init ;;
    plan) cmd_plan ;;
    store) cmd_store ;;
    publish) cmd_publish ;;
    resolve) cmd_resolve ;;
    fetch) cmd_fetch ;;
    verify) cmd_verify ;;
    apply) cmd_apply ;;
    converge) cmd_converge ;;
    restart) cmd_restart ;;
    *) usage ;;
  esac
}

main "$@"
