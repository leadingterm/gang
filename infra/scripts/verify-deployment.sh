#!/usr/bin/env bash
set -euo pipefail

ref=${GANG_WORKFLOW_REF:-}
if [[ $ref != *@refs/tags/v* || ${ref##*@refs/tags/v} == '' ]]; then
  echo '::error::deploy workflow must be pinned to a version tag' >&2
  exit 1
fi
expected=${ref##*@refs/tags/v}
gang=$(tofu -chdir="${ROOT:-.}" output -json gang)
endpoint=$(jq -er '.endpoint | select(type == "string" and length > 0)' <<<"$gang")
region=$(jq -er '.region | select(type == "string" and length > 0)' <<<"$gang")
asg=$(jq -er 'if has("controller_autoscaling_group_name") then .controller_autoscaling_group_name // "" else error("missing controller ASG") end' <<<"$gang")
targets=$(jq -er '.controller_target_group_arns | select(type == "array" and all(.[]; type == "string" and length > 0)) | join("\n")' <<<"$gang")
workers=$(jq -er '.asg_names | select(type == "array" and all(.[]; type == "string" and length > 0)) | join("\n")' <<<"$gang")
deadline=$(($(date +%s) + 2400))
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT

# Retry only AWS throttling errors, without extending the caller's deadline.
aws_read() {
  local end=$1 remaining delay=1 status now
  shift
  while :; do
    now=$(date +%s)
    remaining=$((end - now))
    if (( remaining <= 0 )); then
      echo '::error::AWS read deadline expired' >&2
      return 1
    fi
    if AWS_RETRY_MODE=standard AWS_MAX_ATTEMPTS=3 timeout --signal=KILL "$remaining" aws --cli-connect-timeout "$remaining" --cli-read-timeout "$remaining" "$@" >"$dir/aws-output" 2>"$dir/aws-error"; then
      cat "$dir/aws-error" >&2
      now=$(date +%s)
      if (( now >= end )); then
        echo '::error::AWS read deadline expired' >&2
        return 1
      fi
      cat "$dir/aws-output"
      return 0
    else
      status=$?
    fi
    cat "$dir/aws-error" >&2
    if (( status == 124 || status == 137 )); then
      echo '::error::AWS read deadline expired' >&2
      return "$status"
    fi
    if ! grep -Eq 'An error occurred \((Throttling|ThrottlingException|RequestLimitExceeded|TooManyRequestsException|RequestThrottled)\) when calling' "$dir/aws-error"; then
      return "$status"
    fi
    now=$(date +%s)
    remaining=$((end - now))
    if (( remaining <= delay )); then
      return "$status"
    fi
    sleep "$delay"
    if (( delay < 30 )); then
      delay=$((delay * 2))
      if (( delay > 30 )); then delay=30; fi
    fi
  done
}

# Sleep only while there is time for another readiness read.
readiness_sleep() {
  local end=$1 remaining
  remaining=$((end - $(date +%s)))
  if (( remaining > 0 )); then sleep "$((remaining < 5 ? remaining : 5))"; fi
  (( $(date +%s) < end ))
}

cancel_unready_refresh() {
  local failure_reason remaining
  failure_reason="controller instance refresh${refresh_id:+ $refresh_id} has no serving incoming controller after 600 seconds"
  if [[ $refresh_state == inactive ]]; then
    echo "::error::$failure_reason; no active instance refresh to cancel" >&2; exit 1
  fi
  if [[ $refresh_state == cancelling ]]; then
    echo "::error::$failure_reason; instance refresh is already cancelling" >&2; exit 1
  fi
  echo "::error::$failure_reason; requesting cancellation" >&2
  remaining=$((deadline - $(date +%s)))
  if (( remaining <= 0 )); then
    echo '::error::controller instance refresh timed out before cancellation could be requested' >&2; exit 1
  fi
  if ! AWS_RETRY_MODE=standard AWS_MAX_ATTEMPTS=1 timeout --signal=KILL "$remaining" aws --cli-connect-timeout "$remaining" --cli-read-timeout "$remaining" --region "$region" autoscaling cancel-instance-refresh --auto-scaling-group-name "$asg" --output json; then
    echo "::error::$failure_reason; failed to request instance refresh cancellation" >&2; exit 1
  fi
  echo "::error::$failure_reason; instance refresh cancellation requested (not rolled back)" >&2
  exit 1
}

controller_read() {
  local end=$deadline
  if [[ -z $serving ]] && (( startup_deadline < end )); then end=$startup_deadline; fi
  if ! aws_read "$end" "$@"; then
    if [[ -z $serving ]] && (( $(date +%s) >= startup_deadline )); then cancel_unready_refresh; fi
    return 1
  fi
}

controller_sleep() {
  local end=$deadline
  if [[ -z $serving ]] && (( startup_deadline < end )); then end=$startup_deadline; fi
  if ! readiness_sleep "$end"; then
    if [[ -z $serving ]] && (( $(date +%s) >= startup_deadline )); then cancel_unready_refresh; fi
    return 1
  fi
}

if [[ -n $asg ]]; then
  refresh_id='' rollback_observed='' serving='' refresh_state=unknown
  startup_deadline=$(($(date +%s) + 600))
  while :; do
    if ! refresh=$(controller_read --region "$region" autoscaling describe-instance-refreshes --auto-scaling-group-name "$asg" --max-records 1 --no-paginate --output json); then
      echo '::error::failed to describe controller instance refreshes' >&2; exit 1
    fi
    status=$(jq -er 'if (.InstanceRefreshes | type) != "array" then error("missing refreshes") elif .InstanceRefreshes | length == 0 then "None" else .InstanceRefreshes[0].Status | select(type == "string" and length > 0) end' <<<"$refresh")
    case $status in
      None|Successful) refresh_state=inactive ;;
      Cancelling|RollbackInProgress) refresh_state=cancelling ;;
      Pending|InProgress|Baking) refresh_state=active ;;
    esac
    case $status in
      None|Successful) break ;;
      InProgress)
        current_id=$(jq -er '.InstanceRefreshes[0].InstanceRefreshId | select(type == "string" and length > 0)' <<<"$refresh")
        if [[ $current_id != "$refresh_id" ]]; then
          refresh_id=$current_id
        fi
        if [[ -z $serving ]]; then
          if ! groups=$(controller_read --region "$region" autoscaling describe-auto-scaling-groups --auto-scaling-group-names "$asg" --output json); then
            echo "::error::failed to describe controller Auto Scaling group $asg" >&2; exit 1
          fi
          if ! candidates=$(jq -ec --arg name "$asg" '
            .AutoScalingGroups | select(type == "array" and length == 1 and .[0].AutoScalingGroupName == $name) | .[0]
            | .LaunchTemplate as $template
            | select(($template.LaunchTemplateId | type == "string" and length > 0) and ($template.Version | type == "string" and test("^[0-9]+$")) and (.Instances | type == "array"))
            | [.Instances[] | select(.LaunchTemplate.LaunchTemplateId == $template.LaunchTemplateId and (.LaunchTemplate.Version | tostring) == $template.Version and (.LifecycleState == "Pending:Wait" or .LifecycleState == "Pending:Proceed" or .LifecycleState == "InService") and (.InstanceId | type == "string" and length > 0)) | .InstanceId]
          ' <<<"$groups"); then
            echo '::error::controller Auto Scaling group is missing its numeric launch template or instances' >&2; exit 1
          fi
          if [[ -z $targets ]]; then
            echo '::error::controller target groups are missing during instance refresh' >&2; exit 1
          fi
          while IFS= read -r arn; do
            if ! health=$(controller_read --region "$region" elbv2 describe-target-health --target-group-arn "$arn" --output json); then
              echo "::error::failed to describe target health in $arn" >&2; exit 1
            fi
            if ! candidates=$(jq -ec --argjson ids "$candidates" '
              .TargetHealthDescriptions | select(type == "array")
              | [.[] | select(.TargetHealth.State == "healthy" and (.Target.Id as $id | $ids | index($id))) | .Target.Id]
            ' <<<"$health"); then
              echo "::error::invalid target health in $arn" >&2; exit 1
            fi
          done <<<"$targets"
          if [[ $candidates != '[]' ]]; then serving=1; fi
        fi
        if ! controller_sleep; then echo "::error::controller instance refresh timed out: $status" >&2; exit 1; fi ;;
      RollbackInProgress)
        serving=1
        if [[ -z $rollback_observed ]]; then
          echo 'controller instance refresh RollbackInProgress' >&2
          rollback_observed=1
        fi
        if ! readiness_sleep "$deadline"; then echo "::error::controller instance refresh timed out: $status" >&2; exit 1; fi ;;
      Pending|Baking|Cancelling)
        if ! controller_sleep; then echo "::error::controller instance refresh timed out: $status" >&2; exit 1; fi ;;
      RollbackSuccessful)
        reason=$(jq -r '.InstanceRefreshes[0].StatusReason // ""' <<<"$refresh")
        echo "::error::controller instance refresh RollbackSuccessful (deployment rolled back): $reason" >&2; exit 1 ;;
      RollbackFailed)
        reason=$(jq -r '.InstanceRefreshes[0].StatusReason // ""' <<<"$refresh")
        echo "::error::controller instance refresh RollbackFailed (rollback failure): $reason" >&2; exit 1 ;;
      *) reason=$(jq -r '.InstanceRefreshes[0].StatusReason // ""' <<<"$refresh")
        echo "::error::controller instance refresh $status: $reason" >&2; exit 1 ;;
    esac
  done
  while :; do
    if ! groups=$(controller_read --region "$region" autoscaling describe-auto-scaling-groups --auto-scaling-group-names "$asg" --output json); then
      echo "::error::failed to describe controller Auto Scaling group $asg" >&2; exit 1
    fi
    if controller_ids=$(jq -ec --arg name "$asg" '
    .AutoScalingGroups | select(type == "array" and length == 1 and .[0].AutoScalingGroupName == $name) | .[0]
    | select((.DesiredCapacity | type == "number" and . > 0) and (.Instances | type == "array") and .DesiredCapacity == (.Instances | length))
    | .LaunchTemplate as $template
    | select(($template.LaunchTemplateId | type == "string" and length > 0) and ($template.Version | type == "string" and test("^[0-9]+$")))
    | select(all(.Instances[]; .HealthStatus == "Healthy" and .LifecycleState == "InService" and .LaunchTemplate.LaunchTemplateId == $template.LaunchTemplateId and (.LaunchTemplate.Version | tostring) == $template.Version and (.InstanceId | type == "string" and length > 0)))
    | [.Instances[].InstanceId] | sort | select(length == (unique | length))
    ' <<<"$groups"); then break; fi
    if ! controller_sleep; then
      jq -c '[.AutoScalingGroups[]? | {AutoScalingGroupName, DesiredCapacity, LaunchTemplate, Instances: [.Instances[]? | {InstanceId, HealthStatus, LifecycleState, LaunchTemplate}]}]' <<<"$groups" >&2
      echo "::error::controller Auto Scaling group $asg is not healthy on its current launch template" >&2; exit 1
    fi
  done
elif [[ -n $targets ]]; then
  controller_ids=$(jq -ec '[.controller_instance_id | select(type == "string" and length > 0)] | select(length == 1)' <<<"$gang")
fi

if [[ -n $targets ]]; then
  while IFS= read -r arn; do
    while :; do
      if [[ -n $asg ]]; then
        health=$(controller_read --region "$region" elbv2 describe-target-health --target-group-arn "$arn" --output json) || { echo "::error::failed to describe target health in $arn" >&2; exit 1; }
      elif ! health=$(aws_read "$deadline" --region "$region" elbv2 describe-target-health --target-group-arn "$arn" --output json); then
        echo "::error::failed to describe target health in $arn" >&2; exit 1
      fi
      if jq -e --argjson ids "$controller_ids" '
        .TargetHealthDescriptions | select(type == "array")
        | ([.[].Target.Id] | sort) == $ids
          and ([.[] | select(.TargetHealth.State == "healthy")] | length == 1)
          and all(.[]; .TargetHealth.State == "healthy" or (.TargetHealth.State == "unhealthy" and .TargetHealth.Reason == "Target.FailedHealthChecks"))
      ' <<<"$health" >/dev/null; then
        break
      fi
      if [[ -n $asg ]]; then
        controller_sleep && continue
      elif readiness_sleep "$deadline"; then
        continue
      fi
      printf '%s\n' "$health" >&2
      echo "::error::controller targets did not reach leader/passive readiness in $arn" >&2; exit 1
    done
  done <<<"$targets"
  if [[ -n $asg ]]; then serving=1; fi
fi

if [[ -n $workers ]]; then
  while IFS= read -r worker; do
    attested='[]'
    membership=''
    command_id=''
    command_instance=''
    commands=$(jq -nc --arg command 'pid=$(systemctl show gang-worker.service -p MainPID --value)
case "$pid" in ""|*[!0-9]*) exit 1 ;; esac
case "$pid" in *[1-9]*) ;; *) exit 1 ;; esac
kill -0 "$pid" || exit 1
exec "/proc/$pid/exe" -V' '{commands: [$command]}')
    while :; do
      if ! groups=$(aws_read "$deadline" --region "$region" autoscaling describe-auto-scaling-groups --auto-scaling-group-names "$worker" --output json); then
        echo "::error::failed to describe worker Auto Scaling group $worker" >&2; exit 1
      fi
      state=$(jq -r --arg name "$worker" '
        if (.AutoScalingGroups | type) != "array" or (.AutoScalingGroups | length) != 1 or .AutoScalingGroups[0].AutoScalingGroupName != $name then "missing"
        else .AutoScalingGroups[0] |
          if (.Instances | type) != "array" or (.DesiredCapacity | type) != "number" then "missing"
          else [.Instances[] | select((.LifecycleState // "") | startswith("Terminating") | not)] as $active
            | if any($active[]; .HealthStatus == "Unhealthy") then "unhealthy"
              elif ($active | length) == .DesiredCapacity and all($active[]; .HealthStatus == "Healthy" and .LifecycleState == "InService" and (.InstanceId | type == "string" and length > 0)) and ([$active[].InstanceId] | length == (unique | length)) then "ready"
              else "pending" end
          end
        end' <<<"$groups")
      ids=$(jq -c '[.AutoScalingGroups[0].Instances[]? | select((.LifecycleState // "") | startswith("Terminating") | not) | .InstanceId] | sort' <<<"$groups")
      if [[ $ids != "$membership" ]]; then
        membership=$ids
        attested='[]'
        command_id=''
        command_instance=''
      fi
      case $state in
        ready)
          if (( $(date +%s) >= deadline )); then
            echo "::error::worker Auto Scaling group $worker did not attest the expected running version $expected" >&2; exit 1
          fi
          instance=$(jq -r --argjson 'done' "$attested" '[.[] | select(. as $id | $done | index($id) | not)] | .[0] // ""' <<<"$ids")
          if [[ -z $instance ]]; then break; fi
          if [[ -z $command_id ]]; then
            command_instance=$instance
            if result=$(aws_read "$deadline" --region "$region" ssm send-command --instance-ids "$instance" --document-name AWS-RunShellScript --parameters "$commands" --output json) &&
              command_id=$(jq -er '.Command.CommandId | select(type == "string" and test("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"))' <<<"$result"); then
              :
            elif grep -Fq 'An error occurred (InvalidInstanceId) when calling' "$dir/aws-error"; then
              command_id=''
            else
              echo "::error::worker Auto Scaling group $worker did not attest the expected running version $expected: failed to send SSM command" >&2; exit 1
            fi
          fi
          if [[ -n $command_id ]]; then
            if invocation=$(aws_read "$deadline" --region "$region" ssm get-command-invocation --command-id "$command_id" --instance-id "$command_instance" --output json); then
              status=$(jq -r '.Status // ""' <<<"$invocation" 2>/dev/null) || status=''
              case $status in
                Pending|InProgress|Delayed) ;;
                Success)
                  if jq -e --arg expected "gang $expected" '.ResponseCode == 0 and (.StandardOutputContent | type == "string" and rtrimstr("\n") == $expected)' <<<"$invocation" >/dev/null; then
                    attested=$(jq -c --arg id "$command_instance" '. + [$id]' <<<"$attested")
                  fi
                  command_id='' ;;
                *) command_id='' ;;
              esac
            elif ! grep -Fq 'An error occurred (InvocationDoesNotExist) when calling' "$dir/aws-error"; then
              echo "::error::worker Auto Scaling group $worker did not attest the expected running version $expected: failed to read SSM invocation" >&2; exit 1
            fi
          fi
          if ! readiness_sleep "$deadline"; then
            echo "::error::worker Auto Scaling group $worker did not attest the expected running version $expected" >&2; exit 1
          fi
          ;;
        missing|unhealthy) echo "::error::worker Auto Scaling group $worker is $state" >&2; exit 1 ;;
        *)
          if ! readiness_sleep "$deadline"; then
            jq -c '.AutoScalingGroups[] | {AutoScalingGroupName, DesiredCapacity, Instances: [.Instances[] | {InstanceId, HealthStatus, LifecycleState}]}' <<<"$groups" >&2
            echo "::error::worker Auto Scaling group $worker did not reach desired healthy capacity" >&2; exit 1
          fi ;;
      esac
    done
  done <<<"$workers"
fi

curl_trust=()
if [[ -z $targets ]]; then
  jq -er '.gang_toml | select(type == "string" and length > 0)' <<<"$gang" >"$dir/gang.toml"
  if ! pin=$("${GANG:-gang}" ca-pin --config "$dir/gang.toml"); then
    echo '::error::failed to derive controller CA pin' >&2; exit 1
  fi
  if ! bootstrap=$(curl --fail --silent --show-error --connect-timeout 10 --max-time 20 --retry 12 --retry-delay 5 --retry-all-errors --retry-max-time 120 --insecure "${endpoint%/}/v1/server"); then
    echo '::error::failed to obtain controller CA certificate' >&2; exit 1
  fi
  if ! jq -er '.caCertificate | select(type == "string" and length > 0)' <<<"$bootstrap" >"$dir/ca.pem"; then
    echo '::error::server response has no CA certificate' >&2; exit 1
  fi
  if ! digest=$(openssl x509 -in "$dir/ca.pem" -pubkey -noout | openssl pkey -pubin -outform DER | openssl dgst -sha256 -r); then
    echo '::error::failed to calculate controller CA pin' >&2; exit 1
  fi
  if [[ "sha256:${digest%% *}" != "$pin" ]]; then
    echo '::error::controller CA pin mismatch' >&2; exit 1
  fi
  curl_trust=(--cacert "$dir/ca.pem")
fi

if ! response=$(curl --fail --silent --show-error --connect-timeout 10 --max-time 20 --retry 12 --retry-delay 5 --retry-all-errors --retry-max-time 120 "${curl_trust[@]}" "${endpoint%/}/v1/server"); then
  echo "::error::failed to query server version at $endpoint" >&2; exit 1
fi
if ! version=$(jq -er '.version | select(type == "string" and length > 0)' <<<"$response"); then
  echo '::error::server response has no version' >&2; exit 1
fi
if [[ $version != "$expected" ]]; then
  echo "::error::server version mismatch: expected $expected, observed $version" >&2; exit 1
fi
