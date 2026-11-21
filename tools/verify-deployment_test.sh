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
export CASE CALLS="$scratch/calls" CURL_CALLS="$scratch/curl-calls"
tofu() {
  [[ $* == '-chdir=infra output -json gang' ]] || return 1
  if [[ $CASE == missing ]]; then echo '{"endpoint":"https://gang.example.com","region":"region-1","controller_autoscaling_group_name":null}'; return; fi
  local asg='"controller"' targets='["arn:one","arn:two"]' workers='["worker-one","worker-two"]'
  if [[ $CASE == sqlite || $CASE == sqlite_lb || $CASE == wrong_pin ]]; then asg=null; targets='[]'; workers='[]'; fi
  if [[ $CASE == sqlite_lb ]]; then targets='["arn:one"]'; fi
  printf '{"endpoint":"https://gang.example.com","region":"region-1","gang_toml":"fixture config","controller_instance_id":"i-one","controller_autoscaling_group_name":%s,"controller_target_group_arns":%s,"asg_names":%s}\n' "$asg" "$targets" "$workers"
}
aws() {
  printf '%s\n' "$*" >>"$CALLS"
  case "$*" in
    '--region region-1 autoscaling describe-instance-refreshes --auto-scaling-group-name controller --max-records 1 --no-paginate --output json')
      if [[ $CASE == aws_failure ]]; then return 1; fi
      if [[ $CASE == refresh_failed ]]; then echo '{"InstanceRefreshes":[{"Status":"Failed","StatusReason":"fixture failure"}]}'; else echo '{"InstanceRefreshes":[{"Status":"Successful"}]}'; fi ;;
    '--region region-1 autoscaling describe-auto-scaling-groups --auto-scaling-group-names controller --output json')
      local version=7
      if [[ $CASE == old_template ]]; then version=6; fi
      printf '{"AutoScalingGroups":[{"AutoScalingGroupName":"controller","DesiredCapacity":2,"LaunchTemplate":{"LaunchTemplateId":"lt-one","Version":"7"},"Instances":[{"InstanceId":"i-one","HealthStatus":"Healthy","LifecycleState":"InService","LaunchTemplate":{"LaunchTemplateId":"lt-one","Version":"7"}},{"InstanceId":"i-two","HealthStatus":"Healthy","LifecycleState":"InService","LaunchTemplate":{"LaunchTemplateId":"lt-one","Version":"%s"}}]}]}\n' "$version" ;;
    '--region region-1 autoscaling describe-auto-scaling-groups --auto-scaling-group-names worker-one --output json'|'--region region-1 autoscaling describe-auto-scaling-groups --auto-scaling-group-names worker-two --output json')
      if [[ $CASE == worker_missing ]]; then echo '{"AutoScalingGroups":[]}'; return; fi
      local name=${6} state=Healthy desired=1
      if [[ $CASE == worker_unhealthy ]]; then state=Unhealthy; fi
      if [[ $CASE == worker_short ]]; then desired=2; fi
      printf '{"AutoScalingGroups":[{"AutoScalingGroupName":"%s","DesiredCapacity":%s,"Instances":[{"HealthStatus":"%s","LifecycleState":"InService"}]}]}\n' "$name" "$desired" "$state" ;;
    '--region region-1 elbv2 describe-target-health --target-group-arn arn:one --output json'|'--region region-1 elbv2 describe-target-health --target-group-arn arn:two --output json')
      local leader=healthy
      if [[ $CASE == unhealthy ]]; then leader=unhealthy; fi
      if [[ $CASE == sqlite_lb ]]; then echo '{"TargetHealthDescriptions":[{"Target":{"Id":"i-one"},"TargetHealth":{"State":"healthy"}}]}'; return; fi
      printf '{"TargetHealthDescriptions":[{"Target":{"Id":"i-one"},"TargetHealth":{"State":"%s"}},{"Target":{"Id":"i-two"},"TargetHealth":{"State":"unhealthy","Reason":"Target.ResponseCodeMismatch","Description":"Health checks failed with these codes: [503]"}}]}\n' "$leader" ;;
    *) return 1 ;;
  esac
}
gang() {
  [[ $1 == ca-pin && $2 == --config && $(<"$3") == 'fixture config' ]] || return 1
  if [[ $CASE == wrong_pin ]]; then echo sha256:wrong; else echo sha256:fixture; fi
}
openssl() {
  case "$*" in
    x509\ -in\ */ca.pem\ -pubkey\ -noout) echo public-key ;;
    'pkey -pubin -outform DER') cat >/dev/null; echo der ;;
    'dgst -sha256 -r') cat >/dev/null; echo 'fixture *stdin' ;;
    *) return 1 ;;
  esac
}
curl() {
  [[ ${1-} == --fail && ${2-} == --silent && ${3-} == --show-error && ${4-} == --connect-timeout && ${5-} == 10 && ${6-} == --max-time && ${7-} == 20 ]] || return 1
  local trusted=0
  shift 7
  printf '%s\n' "$*" >>"$CURL_CALLS"
  if [[ ${1-} == --insecure ]]; then
    [[ $# == 2 && $2 == https://gang.example.com/v1/server ]] || return 1
    echo '{"caCertificate":"fixture certificate","version":"ignored"}'; return
  fi
  if [[ ${1-} == --cacert ]]; then
    trusted=1
    [[ $(<"$2") == 'fixture certificate' ]] || return 1
    shift 2
  fi
  if [[ $CASE == sqlite && $trusted != 1 ]]; then return 1; fi
  [[ $# == 1 && $1 == https://gang.example.com/v1/server ]] || return 1
  case $CASE in wrong) echo '{"version":"0.0.0"}' ;; no_version) echo '{}' ;; http_failure) return 1 ;; *) echo '{"version":"1.2.3"}' ;; esac
}
sleep() { SECONDS=$((SECONDS + 601)); }
export -f tofu aws gang openssl curl sleep
for CASE in healthy sqlite sqlite_lb wrong_pin wrong no_version http_failure unhealthy old_template missing aws_failure refresh_failed worker_missing worker_unhealthy worker_short; do
  : >"$CALLS"
  : >"$CURL_CALLS"
  status=0
  (cd "$scratch" && bash --noprofile --norc -euo pipefail "$scratch/step") >"$scratch/out" 2>&1 || status=$?
  case $CASE in healthy|sqlite|sqlite_lb) [[ $status == 0 ]] ;; *) [[ $status != 0 ]] ;; esac
  case $CASE in
    healthy) [[ $(wc -l <"$CALLS") -eq 6 ]] && grep -Fx -- '--region region-1 elbv2 describe-target-health --target-group-arn arn:one --output json' "$CALLS" >/dev/null && grep -Fx -- '--region region-1 elbv2 describe-target-health --target-group-arn arn:two --output json' "$CALLS" >/dev/null && grep -Fx -- '--region region-1 autoscaling describe-auto-scaling-groups --auto-scaling-group-names worker-two --output json' "$CALLS" >/dev/null ;;
    sqlite) [[ ! -s $CALLS ]] ;;
    unhealthy) grep -F '::error::controller targets did not reach leader/passive readiness in arn:one' "$scratch/out" >/dev/null && grep -F '"State":"unhealthy"' "$scratch/out" >/dev/null ;;
    old_template) grep -F 'controller Auto Scaling group controller is not healthy on its current launch template' "$scratch/out" >/dev/null ;;
    wrong_pin) grep -F 'controller CA pin mismatch' "$scratch/out" >/dev/null && [[ $(wc -l <"$CURL_CALLS") -eq 1 ]] ;;
    aws_failure) grep -F '::error::failed to describe controller instance refreshes' "$scratch/out" >/dev/null ;;
    refresh_failed) grep -F 'controller instance refresh Failed: fixture failure' "$scratch/out" >/dev/null ;;
    wrong) grep -F 'server version mismatch: expected 1.2.3, observed 0.0.0' "$scratch/out" >/dev/null ;;
    no_version) grep -F 'server response has no version' "$scratch/out" >/dev/null ;;
    http_failure) grep -F 'failed to query server version' "$scratch/out" >/dev/null ;;
    worker_missing|worker_unhealthy|worker_short) grep -F 'worker Auto Scaling group worker-one is missing or has unhealthy or insufficient instances' "$scratch/out" >/dev/null ;;
  esac
done
