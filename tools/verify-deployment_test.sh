#!/usr/bin/env bash
# Exercise the deployed workflow step with controlled AWS, OpenTofu and HTTP boundaries.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
ln -s "$root" "$scratch/.gang"
workflow="$root/.github/workflows/deploy.yml"
yq -o=json 'explode(.) | .jobs' "$workflow" | jq -e '
  [.apply.steps[], .apply_without_environment.steps[] | select(.name == "Verify server version and controller targets")]
  | length == 2 and all(.[]; .run == ".gang/infra/scripts/verify-deployment.sh" and .["timeout-minutes"] == 45 and .env.GANG_WORKFLOW_REF == "${{ job.workflow_ref }}")
' >/dev/null
yq -er 'explode(.) | .jobs.apply.steps[] | select(.name == "Verify server version and controller targets") | .run' "$workflow" >"$scratch/step"
export GANG_WORKFLOW_REF=example-org/gang/.github/workflows/deploy.yml@refs/tags/v1.2.3 ROOT=infra
export CASE CALLS="$scratch/calls"
tofu() {
  [[ $* == '-chdir=infra output -json gang' ]] || return 1
  if [[ $CASE == missing ]]; then echo '{"endpoint":"https://gang.example.com","region":"region-1","controller_autoscaling_group_name":null}'; return; fi
  local asg='"controller"' targets='["arn:one","arn:two"]' workers='["worker-one","worker-two"]'
  if [[ $CASE == sqlite ]]; then asg=null; targets='[]'; workers='[]'; fi
  printf '{"endpoint":"https://gang.example.com","region":"region-1","controller_autoscaling_group_name":%s,"controller_target_group_arns":%s,"asg_names":%s}\n' "$asg" "$targets" "$workers"
}
aws() {
  printf '%s\n' "$*" >>"$CALLS"
  case "$*" in
    '--region region-1 autoscaling describe-instance-refreshes --auto-scaling-group-name controller --max-records 1 --output json')
      if [[ $CASE == aws_failure ]]; then return 1; fi
      if [[ $CASE == refresh_failed ]]; then echo '{"InstanceRefreshes":[{"Status":"Failed","StatusReason":"fixture failure"}]}'; else echo '{"InstanceRefreshes":[{"Status":"Successful"}]}'; fi ;;
    '--region region-1 elbv2 wait target-in-service --target-group-arn arn:one'|'--region region-1 elbv2 wait target-in-service --target-group-arn arn:two')
      [[ $CASE != unhealthy ]] ;;
    '--region region-1 autoscaling describe-auto-scaling-groups --auto-scaling-group-names worker-one --output json'|'--region region-1 autoscaling describe-auto-scaling-groups --auto-scaling-group-names worker-two --output json')
      if [[ $CASE == worker_missing ]]; then echo '{"AutoScalingGroups":[]}'; return; fi
      local name=${6} state=Healthy desired=1
      if [[ $CASE == worker_unhealthy ]]; then state=Unhealthy; fi
      if [[ $CASE == worker_short ]]; then desired=2; fi
      printf '{"AutoScalingGroups":[{"AutoScalingGroupName":"%s","DesiredCapacity":%s,"Instances":[{"HealthStatus":"%s","LifecycleState":"InService"}]}]}\n' "$name" "$desired" "$state" ;;
    '--region region-1 elbv2 describe-target-health --target-group-arn arn:one --output json')
      echo '{"TargetHealthDescriptions":[{"Target":{"Id":"fixture"},"TargetHealth":{"State":"unhealthy","Reason":"fixture"}}]}' ;;
    *) return 1 ;;
  esac
}
curl() {
  [[ $* == '--fail --silent --show-error --connect-timeout 10 --max-time 20 https://gang.example.com/v1/server' ]] || return 1
  case $CASE in wrong) echo '{"version":"0.0.0"}' ;; no_version) echo '{}' ;; http_failure) return 1 ;; *) echo '{"version":"1.2.3"}' ;; esac
}
sleep() { :; }
export -f tofu aws curl sleep
for CASE in healthy sqlite wrong no_version http_failure unhealthy missing aws_failure refresh_failed worker_missing worker_unhealthy worker_short; do
  : >"$CALLS"
  status=0
  (cd "$scratch" && bash --noprofile --norc -euo pipefail "$scratch/step") >"$scratch/out" 2>&1 || status=$?
  case $CASE in healthy|sqlite) [[ $status == 0 ]] ;; *) [[ $status != 0 ]] ;; esac
  case $CASE in
    healthy) [[ $(wc -l <"$CALLS") -eq 5 ]] && grep -Fx -- '--region region-1 elbv2 wait target-in-service --target-group-arn arn:one' "$CALLS" >/dev/null && grep -Fx -- '--region region-1 elbv2 wait target-in-service --target-group-arn arn:two' "$CALLS" >/dev/null && grep -Fx -- '--region region-1 autoscaling describe-auto-scaling-groups --auto-scaling-group-names worker-two --output json' "$CALLS" >/dev/null ;;
    sqlite) [[ ! -s $CALLS ]] ;;
    unhealthy) grep -F '::error::controller targets did not become healthy in arn:one' "$scratch/out" >/dev/null && grep -F '"State":"unhealthy"' "$scratch/out" >/dev/null ;;
    aws_failure) grep -F '::error::failed to describe controller instance refreshes' "$scratch/out" >/dev/null ;;
    refresh_failed) grep -F 'controller instance refresh Failed: fixture failure' "$scratch/out" >/dev/null ;;
    wrong) grep -F 'server version mismatch: expected 1.2.3, observed 0.0.0' "$scratch/out" >/dev/null ;;
    no_version) grep -F 'server response has no version' "$scratch/out" >/dev/null ;;
    http_failure) grep -F 'failed to query server version' "$scratch/out" >/dev/null ;;
    worker_missing|worker_unhealthy|worker_short) grep -F 'worker Auto Scaling group worker-one is missing or has unhealthy or insufficient instances' "$scratch/out" >/dev/null ;;
  esac
done
