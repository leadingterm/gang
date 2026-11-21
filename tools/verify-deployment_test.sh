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
  if [[ $CASE == cli_timeout || ( $CASE == timeout_ssm_send && $* == *'ssm send-command'* ) || ( $CASE == timeout_ssm_invocation && $* == *'ssm get-command-invocation'* ) ]]; then
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
    *'ssm send-command --instance-ids i-worker-one '*) site=ssm_send ;;
    *'ssm get-command-invocation '*' --instance-id i-worker-one '*) site=ssm_invocation ;;
  esac
  if [[ ( $CASE == global_worker || $CASE == global_ssm_send || $CASE == global_ssm_invocation ) && $* == *describe-target-health*arn:two* ]]; then echo 2397 >"$CLOCK"; fi
  if [[ $CASE == boundary_refresh && $site == refresh || $CASE == boundary_controller && $site == controller || $CASE == boundary_target && $site == target ]]; then
    echo 2397 >"$CLOCK"
  elif [[ $CASE == boundary_worker && $site == worker ]]; then
    echo 2397 >"$CLOCK"
  fi
  if [[ $CASE == "throttle_$site" || ( $CASE == global_exhaust && $site == refresh ) || ( $CASE == worker_exhaust && $site == worker ) || ( $CASE == "global_$site" && -n $site ) || ( $CASE == expired && $site == refresh ) || ( $CASE == worker_expired && $site == worker ) || ( $CASE == permanent && $site == refresh ) || ( $CASE == "permanent_$site" && -n $site ) || ( $CASE == late && $site == refresh ) || ( $CASE == "late_$site" && -n $site ) ]]; then
    local count
    count=$(<"$ATTEMPTS")
    count=$((count + 1))
    echo "$count" >"$ATTEMPTS"
    if [[ $CASE == late || $CASE == late_* ]]; then echo 2400 >"$CLOCK"
    elif [[ $CASE == expired || $CASE == worker_expired ]]; then
      echo 2400 >"$CLOCK"
      echo 'An error occurred (Throttling) when calling the operation: expired throttle' >&2
      return 254
    elif [[ $CASE == permanent || $CASE == permanent_* ]]; then
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
      if [[ $CASE == refresh_failed ]]; then echo '{"InstanceRefreshes":[{"Status":"Failed","StatusReason":"fixture failure"}]}'; elif [[ $CASE == boundary_refresh ]]; then echo '{"InstanceRefreshes":[{"Status":"InProgress"}]}'; else echo '{"InstanceRefreshes":[{"Status":"Successful"}]}'; fi ;;
    '--region region-1 autoscaling describe-auto-scaling-groups --auto-scaling-group-names controller --output json')
      local version=7 lifecycle=InService
      if [[ $CASE == old_template || $CASE == boundary_controller ]]; then version=6; fi
      if [[ $CASE == controller_terminating && ! -e $PENDING_MARKER-controller ]]; then lifecycle=Terminating; : >"$PENDING_MARKER-controller"; fi
      printf '{"AutoScalingGroups":[{"AutoScalingGroupName":"controller","DesiredCapacity":2,"LaunchTemplate":{"LaunchTemplateId":"lt-one","Version":"7"},"Instances":[{"InstanceId":"i-one","HealthStatus":"Healthy","LifecycleState":"InService","LaunchTemplate":{"LaunchTemplateId":"lt-one","Version":"7"}},{"InstanceId":"i-two","HealthStatus":"Healthy","LifecycleState":"%s","LaunchTemplate":{"LaunchTemplateId":"lt-one","Version":"%s"}}]}]}\n' "$lifecycle" "$version" ;;
    '--region region-1 autoscaling describe-auto-scaling-groups --auto-scaling-group-names worker-one --output json'|'--region region-1 autoscaling describe-auto-scaling-groups --auto-scaling-group-names worker-two --output json')
      if [[ $CASE == worker_missing ]]; then echo '{"AutoScalingGroups":[]}'; return; fi
      local name=${6} state=Healthy desired=1 lifecycle=InService instance="i-${6}"
      if [[ $CASE == worker_unhealthy ]]; then state=Unhealthy; fi
      if [[ $CASE == worker_short || $CASE == boundary_worker ]]; then desired=2; fi
      if [[ $CASE == worker_pending && ! -e $PENDING_MARKER-$name ]]; then lifecycle=Pending; : >"$PENDING_MARKER-$name"; fi
      if [[ $CASE == worker_pending_long && $name == worker-one && $(<"$CLOCK") -lt 181 ]]; then lifecycle=Pending; fi
      if [[ $CASE == worker_terminating ]]; then desired=0; lifecycle=Terminating:Wait; state=Unhealthy; fi
      if [[ $CASE == worker_zero ]]; then echo "{\"AutoScalingGroups\":[{\"AutoScalingGroupName\":\"$name\",\"DesiredCapacity\":0,\"Instances\":[]}]}"; return; fi
      if [[ $CASE == worker_replacement && $name == worker-one && -e $PENDING_MARKER-replace ]]; then instance=i-replacement; fi
      if [[ ( $CASE == worker_delayed_rollover || $CASE == worker_pending_replacement ) && $name == worker-one && -e $PENDING_MARKER-rollover ]]; then
        instance=i-replacement
        if [[ ! -e $PENDING_MARKER-rollover-pending ]]; then
          lifecycle=Pending
          : >"$PENDING_MARKER-rollover-pending"
        fi
      fi
      if [[ $CASE == worker_appearing && $name == worker-one && -e $PENDING_MARKER-appeared ]]; then
        echo '{"AutoScalingGroups":[{"AutoScalingGroupName":"worker-one","DesiredCapacity":2,"Instances":[{"InstanceId":"i-worker-one","HealthStatus":"Healthy","LifecycleState":"InService"},{"InstanceId":"i-new","HealthStatus":"Healthy","LifecycleState":"InService"}]}]}'; return
      fi
      if [[ $CASE == worker_mixed && $name == worker-one ]]; then
        echo '{"AutoScalingGroups":[{"AutoScalingGroupName":"worker-one","DesiredCapacity":2,"Instances":[{"InstanceId":"i-current","HealthStatus":"Healthy","LifecycleState":"InService"},{"InstanceId":"i-old","HealthStatus":"Healthy","LifecycleState":"InService"}]}]}'; return
      fi
      if [[ $CASE == worker_terminating_mixed && $name == worker-one ]]; then
        echo '{"AutoScalingGroups":[{"AutoScalingGroupName":"worker-one","DesiredCapacity":1,"Instances":[{"InstanceId":"i-current","HealthStatus":"Healthy","LifecycleState":"InService"},{"InstanceId":"i-old","HealthStatus":"Unhealthy","LifecycleState":"Terminating:Wait"}]}]}'; return
      fi
      printf '{"AutoScalingGroups":[{"AutoScalingGroupName":"%s","DesiredCapacity":%s,"Instances":[{"InstanceId":"%s","HealthStatus":"%s","LifecycleState":"%s"}]}]}\n' "$name" "$desired" "$instance" "$state" "$lifecycle" ;;
    '--region region-1 ssm send-command '*)
      [[ $5 == --instance-ids && $7 == --document-name && $8 == AWS-RunShellScript && $9 == --parameters && ${11} == --output && ${12} == json ]] || return 1
      jq -e '.commands | length == 1 and (.[0] | contains("systemctl show gang-worker.service -p MainPID --value") and contains("/proc/$pid/exe") and contains("-V") and contains("kill -0"))' <<<"${10}" >/dev/null || return 1
      if [[ $CASE == worker_registration_pending && $6 == i-worker-one && ! -e $PENDING_MARKER-registration ]]; then
        : >"$PENDING_MARKER-registration"
        echo 2380 >"$CLOCK"
        echo 'An error occurred (InvalidInstanceId) when calling the SendCommand operation: fixture SSM Agent not registered yet' >&2
        return 254
      fi
      if [[ $CASE == worker_send_failure ]]; then return 1; fi
      echo '{"Command":{"CommandId":"12345678-1234-1234-1234-123456789abc"}}' ;;
    '--region region-1 ssm get-command-invocation '*)
      [[ $5 == --command-id && $6 == 12345678-1234-1234-1234-123456789abc && $7 == --instance-id && $9 == --output && ${10} == json ]] || return 1
      if [[ $CASE == worker_ssm_pending && ! -e $PENDING_MARKER-ssm-$8 ]]; then
        : >"$PENDING_MARKER-ssm-$8"; echo '{"Status":"Pending"}'; return
      fi
      if [[ $CASE == worker_invocation_failure ]]; then echo 'An error occurred (AccessDenied) when calling the operation: fixture invocation denied' >&2; return 253; fi
      if [[ $CASE == worker_pending_replacement && $8 == i-worker-one ]]; then
        : >"$PENDING_MARKER-rollover"; echo '{"Status":"Pending"}'; return
      fi
      if [[ $CASE == worker_invocation_missing && ! -e $PENDING_MARKER-invocation-$8 ]]; then
        : >"$PENDING_MARKER-invocation-$8"; echo 'An error occurred (InvocationDoesNotExist) when calling the GetCommandInvocation operation: fixture invocation not visible yet' >&2; return 254
      fi
      if [[ $CASE == worker_ssm_failure || $CASE == worker_missing_pid ]]; then echo '{"Status":"Failed","ResponseCode":1,"StandardOutputContent":""}'; return; fi
      if [[ $CASE == worker_missing_output ]]; then echo '{"Status":"Success","ResponseCode":0,"StandardOutputContent":""}'; return; fi
      if [[ $CASE == worker_mixed && $8 == i-old ]]; then : >"$PENDING_MARKER-old"; printf '%s\n' '{"Status":"Success","ResponseCode":0,"StandardOutputContent":"gang 0.9.0\n"}'; return; fi
      if [[ $CASE == worker_replacement && $8 == i-worker-one ]]; then
        : >"$PENDING_MARKER-replace"; printf '%s\n' '{"Status":"Success","ResponseCode":0,"StandardOutputContent":"gang 0.9.0\n"}'; return
      fi
      if [[ $CASE == worker_delayed_rollover && $8 == i-worker-one ]]; then
        : >"$PENDING_MARKER-rollover"; printf '%s\n' '{"Status":"Success","ResponseCode":0,"StandardOutputContent":"gang 0.9.0\n"}'; return
      fi
      if [[ $CASE == worker_appearing && $8 == i-worker-one ]]; then : >"$PENDING_MARKER-appeared"; fi
      printf '%s\n' '{"Status":"Success","ResponseCode":0,"StandardOutputContent":"gang 1.2.3\n"}' ;;
    '--region region-1 elbv2 describe-target-health --target-group-arn arn:one --output json'|'--region region-1 elbv2 describe-target-health --target-group-arn arn:two --output json')
      local leader=healthy
      if [[ $CASE == unhealthy || $CASE == boundary_target ]]; then leader=unhealthy; fi
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
  case $CASE in
    unhealthy|old_template|worker_short|worker_missing_pid|worker_missing_output|worker_ssm_failure) now=$((now + 2401)) ;;
    worker_mixed) if [[ -e $PENDING_MARKER-old ]]; then now=$((now + 2401)); else now=$((now + $1)); fi ;;
    worker_delayed_rollover)
      if [[ -e $PENDING_MARKER-rollover && ! -e $PENDING_MARKER-elapsed ]]; then
        : >"$PENDING_MARKER-elapsed"; now=$((now + 181))
      else
        now=$((now + $1))
      fi ;;
    *) now=$((now + $1)) ;;
  esac
  echo "$now" >"$CLOCK"
}
export -f tofu aws gang openssl curl sleep date timeout
for CASE in healthy sqlite sqlite_lb wrong_pin wrong no_version http_failure unhealthy old_template controller_terminating missing aws_failure refresh_failed worker_missing worker_unhealthy worker_short worker_pending worker_pending_long worker_terminating worker_zero worker_mixed worker_missing_pid worker_missing_output worker_replacement worker_delayed_rollover worker_pending_replacement worker_appearing worker_terminating_mixed worker_ssm_pending worker_invocation_missing worker_registration_pending worker_ssm_failure worker_send_failure worker_invocation_failure boundary_refresh boundary_controller boundary_target boundary_worker throttle_refresh throttle_controller throttle_target throttle_worker throttle_ssm_send throttle_ssm_invocation global_exhaust worker_exhaust global_worker global_ssm_send global_ssm_invocation expired worker_expired permanent permanent_ssm_send permanent_ssm_invocation late late_ssm_send late_ssm_invocation cli_timeout timeout_ssm_send timeout_ssm_invocation; do
  : >"$CALLS"
  : >"$CURL_CALLS"
  echo 0 >"$CLOCK"
  echo 0 >"$ATTEMPTS"
  : >"$SLEEPS"
  : >"$TIMEOUTS"
  rm -f "$PENDING_MARKER"-*
  status=0
  (cd "$scratch" && bash --noprofile --norc -euo pipefail "$scratch/step") >"$scratch/out" 2>&1 || status=$?
  case $CASE in healthy|sqlite|sqlite_lb|controller_terminating|worker_pending|worker_pending_long|worker_terminating|worker_zero|worker_replacement|worker_delayed_rollover|worker_pending_replacement|worker_appearing|worker_terminating_mixed|worker_ssm_pending|worker_invocation_missing|worker_registration_pending|throttle_*) [[ $status == 0 ]] ;; *) [[ $status != 0 ]] ;; esac
  case $CASE in
    healthy) [[ $(grep -c -- '--region region-1 ssm send-command' "$CALLS") -eq 2 ]] && grep -Fx -- '--region region-1 elbv2 describe-target-health --target-group-arn arn:one --output json' "$CALLS" >/dev/null && grep -Fx -- '--region region-1 elbv2 describe-target-health --target-group-arn arn:two --output json' "$CALLS" >/dev/null && grep -Fx -- '--region region-1 autoscaling describe-auto-scaling-groups --auto-scaling-group-names worker-two --output json' "$CALLS" >/dev/null ;;
    sqlite) [[ ! -s $CALLS ]] ;;
    unhealthy|boundary_target)
      grep -F '::error::controller targets did not reach leader/passive readiness in arn:one' "$scratch/out" >/dev/null
      grep -F '"State":"unhealthy"' "$scratch/out" >/dev/null ;;
    old_template|boundary_controller)
      grep -F '::error::controller Auto Scaling group controller is not healthy on its current launch template' "$scratch/out" >/dev/null
      grep -F '"Version":"6"' "$scratch/out" >/dev/null ;;
    wrong_pin) grep -F 'controller CA pin mismatch' "$scratch/out" >/dev/null && [[ $(wc -l <"$CURL_CALLS") -eq 1 ]] ;;
    aws_failure) grep -F '::error::failed to describe controller instance refreshes' "$scratch/out" >/dev/null ;;
    refresh_failed) grep -F 'controller instance refresh Failed: fixture failure' "$scratch/out" >/dev/null ;;
    boundary_refresh) grep -F '::error::controller instance refresh timed out: InProgress' "$scratch/out" >/dev/null ;;
    wrong) grep -F 'server version mismatch: expected 1.2.3, observed 0.0.0' "$scratch/out" >/dev/null ;;
    no_version) grep -F 'server response has no version' "$scratch/out" >/dev/null ;;
    http_failure) grep -F 'failed to query server version' "$scratch/out" >/dev/null ;;
    worker_missing) grep -F 'worker Auto Scaling group worker-one is missing' "$scratch/out" >/dev/null ;;
    worker_unhealthy) grep -F 'worker Auto Scaling group worker-one is unhealthy' "$scratch/out" >/dev/null ;;
    controller_terminating) [[ -e $PENDING_MARKER-controller ]] ;;
    worker_short|boundary_worker)
      grep -F '::error::worker Auto Scaling group worker-one did not reach desired healthy capacity' "$scratch/out" >/dev/null
      grep -F '"DesiredCapacity":2' "$scratch/out" >/dev/null ;;
    worker_pending) [[ -e $PENDING_MARKER-worker-one && -e $PENDING_MARKER-worker-two ]] ;;
    throttle_*)
      if [[ $CASE == throttle_worker ]]; then
        [[ $(<"$ATTEMPTS") == 9 ]]
      else
        [[ $(<"$ATTEMPTS") == 8 ]]
      fi
      [[ $(<"$TIMEOUTS") == *$'2400\n2399\n2397\n2393\n2385\n2369\n2339\n2309'* ]]
      [[ $(<"$SLEEPS") == $'1\n2\n4\n8\n16\n30\n30\n5\n5' ]]
      grep -F 'fixture throttle' "$scratch/out" >/dev/null
      ! grep -F 'partial output from failed AWS request' "$scratch/out" >/dev/null ;;
    global_exhaust|worker_exhaust)
      [[ $(<"$ATTEMPTS") -gt 7 ]]
      [[ $(<"$SLEEPS") == $'1\n2\n4\n8\n16\n30\n30'* ]]
      grep -F 'fixture throttle' "$scratch/out" >/dev/null
      [[ $(<"$CLOCK") -lt 2400 ]]
      ! grep -F 'partial output from failed AWS request' "$scratch/out" >/dev/null ;;
    global_worker|global_ssm_send|global_ssm_invocation)
      [[ $(<"$ATTEMPTS") == 2 && $(<"$SLEEPS") == 1 && $(<"$CLOCK") == 2398 ]]
      [[ $(<"$TIMEOUTS") == *$'\n3\n2' ]]
      ! grep -F 'partial output from failed AWS request' "$scratch/out" >/dev/null ;;
    expired|worker_expired)
      [[ $(<"$ATTEMPTS") == 1 && ! -s $SLEEPS ]]
      grep -F 'An error occurred (Throttling) when calling the operation: expired throttle' "$scratch/out" >/dev/null ;;
    permanent|permanent_ssm_send|permanent_ssm_invocation)
      [[ $(<"$ATTEMPTS") == 1 && ! -s $SLEEPS ]]
      grep -F 'An error occurred (AccessDenied) when calling the operation: fixture denied' "$scratch/out" >/dev/null ;;
    cli_timeout|timeout_ssm_send|timeout_ssm_invocation)
      [[ ! -s $SLEEPS ]]
      grep -F 'fixture CLI timed out' "$scratch/out" >/dev/null
      grep -F 'AWS read deadline expired' "$scratch/out" >/dev/null ;;
    late|late_ssm_send|late_ssm_invocation)
      [[ $(<"$ATTEMPTS") == 1 && ! -s $SLEEPS ]]
      grep -F 'AWS read deadline expired' "$scratch/out" >/dev/null ;;
    worker_zero|worker_terminating) ! grep -F 'ssm send-command' "$CALLS" >/dev/null ;;
    worker_mixed) [[ -e $PENDING_MARKER-old ]] && grep -F 'did not attest the expected running version' "$scratch/out" >/dev/null ;;
    worker_missing_pid|worker_missing_output|worker_ssm_failure|worker_send_failure|worker_invocation_failure) grep -F 'did not attest the expected running version' "$scratch/out" >/dev/null ;;
    worker_replacement) [[ -e $PENDING_MARKER-replace ]] && grep -F -- '--instance-ids i-replacement' "$CALLS" >/dev/null ;;
    worker_delayed_rollover) [[ -e $PENDING_MARKER-elapsed && -e $PENDING_MARKER-rollover-pending && $(<"$CLOCK") -gt 180 ]] && grep -F -- '--instance-ids i-replacement' "$CALLS" >/dev/null ;;
    worker_pending_long) [[ $(<"$CLOCK") -gt 180 && $(<"$CLOCK") -lt 2400 ]] && grep -F -- '--instance-ids i-worker-one' "$CALLS" >/dev/null ;;
    worker_pending_replacement)
      [[ -e $PENDING_MARKER-rollover-pending && $(grep -c -- 'ssm get-command-invocation .* --instance-id i-worker-one ' "$CALLS") -eq 1 ]]
      grep -F -- '--instance-ids i-replacement' "$CALLS" >/dev/null ;;
    worker_appearing)
      [[ -e $PENDING_MARKER-appeared && $(grep -c -- 'ssm send-command --instance-ids i-worker-one ' "$CALLS") -eq 2 ]]
      grep -F -- '--instance-ids i-new' "$CALLS" >/dev/null ;;
    worker_terminating_mixed) ! grep -F -- '--instance-ids i-old' "$CALLS" >/dev/null && grep -F -- '--instance-ids i-current' "$CALLS" >/dev/null ;;
    worker_ssm_pending) [[ -e $PENDING_MARKER-ssm-i-worker-one && $(grep -c -- 'ssm send-command' "$CALLS") -eq 2 ]] ;;
    worker_invocation_missing) [[ -e $PENDING_MARKER-invocation-i-worker-one && $(grep -c -- 'ssm send-command' "$CALLS") -eq 2 ]] ;;
    worker_registration_pending)
      [[ -e $PENDING_MARKER-registration && $(<"$CLOCK") == 2395 && $(<"$SLEEPS") == $'5\n5\n5' ]]
      [[ $(grep -c -- 'ssm send-command --instance-ids i-worker-one ' "$CALLS") -eq 2 && $(grep -c -- 'ssm get-command-invocation .* --instance-id i-worker-one ' "$CALLS") -eq 1 ]]
      [[ $(<"$TIMEOUTS") == $'2400\n2400\n2400\n2400\n2400\n2400\n15\n15\n15\n10\n10\n10\n10\n5' ]]
      grep -F 'An error occurred (InvalidInstanceId) when calling the SendCommand operation' "$scratch/out" >/dev/null ;;
  esac
  case $CASE in
    boundary_refresh) expected_calls=1 ;;
    boundary_controller) expected_calls=2 ;;
    boundary_target) expected_calls=3 ;;
    boundary_worker) expected_calls=5 ;;
    *) continue ;;
  esac
  [[ $(<"$SLEEPS") == 3 && $(wc -l <"$TIMEOUTS") -eq $expected_calls ]]
  [[ $(<"$CLOCK") == 2400 ]]
done
