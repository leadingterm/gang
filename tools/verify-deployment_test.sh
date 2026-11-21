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
export CASE CALLS="$scratch/calls" CURL_CALLS="$scratch/curl-calls" PENDING_MARKER="$scratch/pending"
export CLOCK="$scratch/clock" SLEEPS="$scratch/sleeps" ATTEMPTS="$scratch/attempts" TIMEOUTS="$scratch/timeouts"
tofu() {
  [[ $* == '-chdir=infra output -json gang' ]] || return 1
  if [[ $CASE == missing ]]; then echo '{"endpoint":"https://gang.example.com","region":"region-1","controller_autoscaling_group_name":null}'; return; fi
  local asg='"controller"' targets='["arn:one","arn:two"]' workers='["worker-one","worker-two"]'
  if [[ $CASE == sqlite || $CASE == sqlite_lb || $CASE == wrong_pin ]]; then asg=null; targets='[]'; workers='[]'; fi
  if [[ $CASE == sqlite_lb ]]; then targets='["arn:one"]'; fi
  printf '{"endpoint":"https://gang.example.com","region":"region-1","gang_toml":"fixture config","controller_instance_id":"i-one","controller_autoscaling_group_name":%s,"controller_target_group_arns":%s,"asg_names":%s}\n' "$asg" "$targets" "$workers"
}
timeout() {
  [[ $1 == --signal=KILL && $2 -gt 0 && $3 == aws && $4 == --cli-connect-timeout && $5 == "$2" && $6 == --cli-read-timeout && $7 == "$2" ]] || return 1
  if [[ $CASE == cli_timeout ]]; then
    echo 'fixture CLI timed out' >&2
    return 124
  fi
  shift 2
  "$@"
}
aws() {
  [[ $AWS_RETRY_MODE == standard && $AWS_MAX_ATTEMPTS == 3 ]] || return 1
  [[ $1 == --cli-connect-timeout && $3 == --cli-read-timeout && $2 == "$4" && $2 -gt 0 ]] || return 1
  printf '%s\n' "$2" >>"$TIMEOUTS"
  shift 4
  local site=
  case "$*" in
    *describe-instance-refreshes*) site=refresh ;;
    *describe-auto-scaling-groups*controller*) site=controller ;;
    *describe-target-health*arn:one*) site=target ;;
    *describe-auto-scaling-groups*worker-one*) site=worker ;;
  esac
  if [[ $CASE == global_worker && $* == *describe-target-health*arn:two* ]]; then echo 2397 >"$CLOCK"; fi
  if [[ $CASE == "throttle_$site" || ( $CASE == global_exhaust && $site == refresh ) || ( $CASE == worker_exhaust && $site == worker ) || ( $CASE == global_worker && $site == worker ) || ( $CASE == expired && $site == refresh ) || ( $CASE == worker_expired && $site == worker ) || ( $CASE == permanent && $site == refresh ) || ( $CASE == late && $site == refresh ) ]]; then
    local count
    count=$(<"$ATTEMPTS")
    count=$((count + 1))
    echo "$count" >"$ATTEMPTS"
    if [[ $CASE == late ]]; then echo 2400 >"$CLOCK"
    elif [[ $CASE == expired || $CASE == worker_expired ]]; then
      if [[ $CASE == expired ]]; then echo 2400 >"$CLOCK"; else echo 180 >"$CLOCK"; fi
      echo 'An error occurred (Throttling) when calling the operation: expired throttle' >&2
      return 254
    elif [[ $CASE == permanent ]]; then
      echo 'An error occurred (AccessDenied) when calling the operation: fixture denied' >&2
      return 253
    elif [[ $CASE != throttle_* || $count -le 7 ]]; then
      local codes=(Throttling ThrottlingException RequestLimitExceeded TooManyRequestsException RequestThrottled)
      printf 'partial output from failed AWS request'
      printf 'An error occurred (%s) when calling the operation: fixture throttle\n' "${codes[$(((count - 1) % 5))]}" >&2
      return 254
    fi
  fi
  printf '%s\n' "$*" >>"$CALLS"
  case "$*" in
    '--region region-1 autoscaling describe-instance-refreshes --auto-scaling-group-name controller --max-records 1 --no-paginate --output json')
      if [[ $CASE == aws_failure ]]; then return 1; fi
      if [[ $CASE == refresh_failed ]]; then echo '{"InstanceRefreshes":[{"Status":"Failed","StatusReason":"fixture failure"}]}'; else echo '{"InstanceRefreshes":[{"Status":"Successful"}]}'; fi ;;
    '--region region-1 autoscaling describe-auto-scaling-groups --auto-scaling-group-names controller --output json')
      local version=7 lifecycle=InService
      if [[ $CASE == old_template ]]; then version=6; fi
      if [[ $CASE == controller_terminating && ! -e $PENDING_MARKER-controller ]]; then lifecycle=Terminating; : >"$PENDING_MARKER-controller"; fi
      printf '{"AutoScalingGroups":[{"AutoScalingGroupName":"controller","DesiredCapacity":2,"LaunchTemplate":{"LaunchTemplateId":"lt-one","Version":"7"},"Instances":[{"InstanceId":"i-one","HealthStatus":"Healthy","LifecycleState":"InService","LaunchTemplate":{"LaunchTemplateId":"lt-one","Version":"7"}},{"InstanceId":"i-two","HealthStatus":"Healthy","LifecycleState":"%s","LaunchTemplate":{"LaunchTemplateId":"lt-one","Version":"%s"}}]}]}\n' "$lifecycle" "$version" ;;
    '--region region-1 autoscaling describe-auto-scaling-groups --auto-scaling-group-names worker-one --output json'|'--region region-1 autoscaling describe-auto-scaling-groups --auto-scaling-group-names worker-two --output json')
      if [[ $CASE == worker_missing ]]; then echo '{"AutoScalingGroups":[]}'; return; fi
      local name=${6} state=Healthy desired=1 lifecycle=InService
      if [[ $CASE == worker_unhealthy ]]; then state=Unhealthy; fi
      if [[ $CASE == worker_short ]]; then desired=2; fi
      if [[ $CASE == worker_pending && ! -e $PENDING_MARKER-$name ]]; then lifecycle=Pending; : >"$PENDING_MARKER-$name"; fi
      if [[ $CASE == worker_terminating ]]; then desired=0; lifecycle=Terminating:Wait; state=Unhealthy; fi
      printf '{"AutoScalingGroups":[{"AutoScalingGroupName":"%s","DesiredCapacity":%s,"Instances":[{"HealthStatus":"%s","LifecycleState":"%s"}]}]}\n' "$name" "$desired" "$state" "$lifecycle" ;;
    '--region region-1 elbv2 describe-target-health --target-group-arn arn:one --output json'|'--region region-1 elbv2 describe-target-health --target-group-arn arn:two --output json')
      local leader=healthy
      if [[ $CASE == unhealthy ]]; then leader=unhealthy; fi
      if [[ $CASE == sqlite_lb ]]; then echo '{"TargetHealthDescriptions":[{"Target":{"Id":"i-one"},"TargetHealth":{"State":"healthy"}}]}'; return; fi
      printf '{"TargetHealthDescriptions":[{"Target":{"Id":"i-one"},"TargetHealth":{"State":"%s"}},{"Target":{"Id":"i-two"},"TargetHealth":{"State":"unhealthy","Reason":"Target.FailedHealthChecks","Description":"Health checks failed"}}]}\n' "$leader" ;;
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
  [[ ${1-} == --fail && ${2-} == --silent && ${3-} == --show-error && ${4-} == --connect-timeout && ${5-} == 10 && ${6-} == --max-time && ${7-} == 20 && ${8-} == --retry && ${9-} == 12 && ${10-} == --retry-delay && ${11-} == 5 && ${12-} == --retry-all-errors && ${13-} == --retry-max-time && ${14-} == 120 ]] || return 1
  local trusted=0
  shift 14
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
date() {
  [[ $* == +%s ]] || return 1
  cat "$CLOCK"
}
sleep() {
  printf '%s\n' "$1" >>"$SLEEPS"
  local now
  now=$(<"$CLOCK")
  case $CASE in unhealthy|old_template) now=$((now + 2401)) ;; worker_short) now=$((now + 181)) ;; *) now=$((now + $1)) ;; esac
  echo "$now" >"$CLOCK"
}
export -f tofu aws gang openssl curl sleep date timeout
for CASE in healthy sqlite sqlite_lb wrong_pin wrong no_version http_failure unhealthy old_template controller_terminating missing aws_failure refresh_failed worker_missing worker_unhealthy worker_short worker_pending worker_terminating throttle_refresh throttle_controller throttle_target throttle_worker global_exhaust worker_exhaust global_worker expired worker_expired permanent late cli_timeout; do
  : >"$CALLS"
  : >"$CURL_CALLS"
  echo 0 >"$CLOCK"
  echo 0 >"$ATTEMPTS"
  : >"$SLEEPS"
  : >"$TIMEOUTS"
  rm -f "$PENDING_MARKER"-*
  status=0
  (cd "$scratch" && bash --noprofile --norc -euo pipefail "$scratch/step") >"$scratch/out" 2>&1 || status=$?
  case $CASE in healthy|sqlite|sqlite_lb|controller_terminating|worker_pending|worker_terminating|throttle_refresh|throttle_controller|throttle_target|throttle_worker) [[ $status == 0 ]] ;; *) [[ $status != 0 ]] ;; esac
  case $CASE in
    healthy) [[ $(wc -l <"$CALLS") -eq 6 ]] && grep -Fx -- '--region region-1 elbv2 describe-target-health --target-group-arn arn:one --output json' "$CALLS" >/dev/null && grep -Fx -- '--region region-1 elbv2 describe-target-health --target-group-arn arn:two --output json' "$CALLS" >/dev/null && grep -Fx -- '--region region-1 autoscaling describe-auto-scaling-groups --auto-scaling-group-names worker-two --output json' "$CALLS" >/dev/null ;;
    sqlite) [[ ! -s $CALLS ]] ;;
    unhealthy) grep -F '::error::failed to describe target health in arn:one' "$scratch/out" >/dev/null ;;
    old_template) grep -F '::error::failed to describe controller Auto Scaling group controller' "$scratch/out" >/dev/null ;;
    wrong_pin) grep -F 'controller CA pin mismatch' "$scratch/out" >/dev/null && [[ $(wc -l <"$CURL_CALLS") -eq 1 ]] ;;
    aws_failure) grep -F '::error::failed to describe controller instance refreshes' "$scratch/out" >/dev/null ;;
    refresh_failed) grep -F 'controller instance refresh Failed: fixture failure' "$scratch/out" >/dev/null ;;
    wrong) grep -F 'server version mismatch: expected 1.2.3, observed 0.0.0' "$scratch/out" >/dev/null ;;
    no_version) grep -F 'server response has no version' "$scratch/out" >/dev/null ;;
    http_failure) grep -F 'failed to query server version' "$scratch/out" >/dev/null ;;
    worker_missing) grep -F 'worker Auto Scaling group worker-one is missing' "$scratch/out" >/dev/null ;;
    worker_unhealthy) grep -F 'worker Auto Scaling group worker-one is unhealthy' "$scratch/out" >/dev/null ;;
    controller_terminating) [[ -e $PENDING_MARKER-controller ]] ;;
    worker_short) grep -F '::error::failed to describe worker Auto Scaling group worker-one' "$scratch/out" >/dev/null ;;
    worker_pending) [[ -e $PENDING_MARKER-worker-one && -e $PENDING_MARKER-worker-two ]] ;;
    throttle_*)
      [[ $(<"$ATTEMPTS") == 8 ]]
      if [[ $CASE == throttle_worker ]]; then
        [[ $(<"$TIMEOUTS") == *$'\n180\n179\n177\n173\n165\n149\n119\n89'* ]]
      else
        [[ $(<"$TIMEOUTS") == *$'2400\n2399\n2397\n2393\n2385\n2369\n2339\n2309'* ]]
      fi
      [[ $(<"$SLEEPS") == $'1\n2\n4\n8\n16\n30\n30' ]]
      grep -F 'fixture throttle' "$scratch/out" >/dev/null ;;
    global_exhaust|worker_exhaust)
      [[ $(<"$ATTEMPTS") -gt 7 ]]
      [[ $(<"$SLEEPS") == $'1\n2\n4\n8\n16\n30\n30'* ]]
      grep -F 'fixture throttle' "$scratch/out" >/dev/null
      if [[ $CASE == global_exhaust ]]; then [[ $(<"$CLOCK") -lt 2400 ]]; else [[ $(<"$CLOCK") -lt 180 ]]; fi ;;
    global_worker)
      [[ $(<"$ATTEMPTS") == 2 && $(<"$SLEEPS") == 1 && $(<"$CLOCK") == 2398 ]]
      [[ $(<"$TIMEOUTS") == *$'\n3\n2' ]] ;;
    expired|worker_expired)
      [[ $(<"$ATTEMPTS") == 1 && ! -s $SLEEPS ]]
      grep -F 'An error occurred (Throttling) when calling the operation: expired throttle' "$scratch/out" >/dev/null ;;
    permanent)
      [[ $(<"$ATTEMPTS") == 1 && ! -s $SLEEPS ]]
      grep -F 'An error occurred (AccessDenied) when calling the operation: fixture denied' "$scratch/out" >/dev/null ;;
    cli_timeout)
      [[ ! -s $SLEEPS ]]
      grep -F 'fixture CLI timed out' "$scratch/out" >/dev/null
      grep -F 'AWS read deadline expired' "$scratch/out" >/dev/null ;;
    late)
      [[ $(<"$ATTEMPTS") == 1 && ! -s $SLEEPS ]]
      grep -F 'AWS read deadline expired' "$scratch/out" >/dev/null ;;
  esac
done
