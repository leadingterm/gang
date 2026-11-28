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
service=$(jq -er 'if has("controller_service_name") then .controller_service_name // "" else error("missing controller service") end' <<<"$gang")
cluster=$(jq -er 'if has("controller_cluster_name") then .controller_cluster_name // "" else error("missing controller cluster") end' <<<"$gang")
task_definition=$(jq -er 'if has("controller_task_definition_arn") then .controller_task_definition_arn // "" else error("missing controller task definition") end' <<<"$gang")
capacity_asg=$(jq -er 'if has("controller_capacity_asg_name") then .controller_capacity_asg_name // "" else error("missing controller capacity ASG") end' <<<"$gang")
targets=$(jq -er '.controller_target_group_arns | select(type == "array" and all(.[]; type == "string" and length > 0)) | join("\n")' <<<"$gang")
workers=$(jq -er '.asg_names | select(type == "array" and all(.[]; type == "string" and length > 0)) | join("\n")' <<<"$gang")
wait_seconds=2400
if [[ -n $service ]]; then
  stop_timeout=$(jq -er '.controller_stop_timeout_seconds | select(type == "number" and . > 0 and floor == .)' <<<"$gang") || { echo '::error::invalid controller stop timeout' >&2; exit 1; }
  if (( stop_timeout + 600 > wait_seconds )); then wait_seconds=$((stop_timeout + 600)); fi
fi
deadline=$(($(date +%s) + wait_seconds))
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

if [[ -n $service ]]; then
  [[ -n $cluster && -n $task_definition && -n $capacity_asg ]] || { echo '::error::incomplete controller ECS outputs' >&2; exit 1; }
  controller_count=$(jq -er '.controller_count | select(type == "number" and . > 0 and floor == .)' <<<"$gang")
  while :; do
    if ! service_description=$(aws_read "$deadline" --region "$region" ecs describe-services --cluster "$cluster" --services "$service" --output json); then
      echo '::error::failed to describe controller ECS service' >&2; exit 1
    fi
    if jq -e --arg definition "$task_definition" --argjson count "$controller_count" '
      (.failures | type == "array" and length == 0) and
      (.services | type == "array" and length == 1 and
        (.[0] | .status == "ACTIVE" and .taskDefinition == $definition and
          .desiredCount == $count and .runningCount == $count and .pendingCount == 0 and
          ([.deployments[]? | select(.status == "PRIMARY")] | length == 1 and
            .[0].rolloutState == "COMPLETED" and .[0].taskDefinition == $definition)))
    ' <<<"$service_description" >/dev/null; then break; fi
    if jq -e 'any(.services[]?.deployments[]?; .status == "PRIMARY" and .rolloutState == "FAILED")' <<<"$service_description" >/dev/null; then
      printf '%s\n' "$service_description" >&2
      echo '::error::controller ECS service deployment failed' >&2; exit 1
    fi
    if ! readiness_sleep "$deadline"; then
      printf '%s\n' "$service_description" >&2
      echo '::error::controller ECS service deployment did not become ready' >&2; exit 1
    fi
  done
  while :; do
    if ! task_list=$(aws_read "$deadline" --region "$region" ecs list-tasks --cluster "$cluster" --service-name "$service" --desired-status RUNNING --output json); then
      echo '::error::failed to list controller ECS tasks' >&2; exit 1
    fi
    if task_arns=$(jq -er --argjson count "$controller_count" '
      .taskArns | select(type == "array" and length == $count and all(.[]; type == "string" and length > 0) and length == (unique | length)) | .[]
    ' <<<"$task_list"); then
      task_array=()
      while IFS= read -r task_arn; do task_array+=("$task_arn"); done <<<"$task_arns"
      : >"$dir/controller-tasks"
      for ((offset = 0; offset < ${#task_array[@]}; offset += 100)); do
        if ! task_description=$(aws_read "$deadline" --region "$region" ecs describe-tasks --cluster "$cluster" --tasks "${task_array[@]:offset:100}" --output json); then
          echo '::error::failed to describe controller ECS tasks' >&2; exit 1
        fi
        printf '%s\n' "$task_description" >>"$dir/controller-tasks"
      done
      if container_instances=$(jq -ser --arg definition "$task_definition" --argjson count "$controller_count" --argjson arns "$(jq -Rsc 'split("\n") | map(select(length > 0)) | sort' <<<"$task_arns")" '
        select(all(.[]; (.failures | type == "array" and length == 0) and (.tasks | type == "array")))
        | [.[].tasks[]] | select(length == $count and ([.[].taskArn] | sort) == $arns)
        | select(all(.[]; .lastStatus == "RUNNING" and .taskDefinitionArn == $definition and
            (.healthStatus == null or .healthStatus == "UNKNOWN" or .healthStatus == "HEALTHY")))
        | [.[].containerInstanceArn]
        | select(length == $count and all(.[]; type == "string" and length > 0) and length == (unique | length)) | sort | .[]
      ' "$dir/controller-tasks"); then
        instance_array=()
        while IFS= read -r instance_arn; do instance_array+=("$instance_arn"); done <<<"$container_instances"
        : >"$dir/controller-instances"
        for ((offset = 0; offset < ${#instance_array[@]}; offset += 100)); do
          if ! instance_description=$(aws_read "$deadline" --region "$region" ecs describe-container-instances --cluster "$cluster" --container-instances "${instance_array[@]:offset:100}" --output json); then
            echo '::error::failed to describe controller ECS container instances' >&2; exit 1
          fi
          printf '%s\n' "$instance_description" >>"$dir/controller-instances"
        done
        if controller_ids=$(jq -sec --argjson count "$controller_count" --argjson arns "$(jq -Rsc 'split("\n") | map(select(length > 0)) | sort' <<<"$container_instances")" '
          select(all(.[]; (.failures | type == "array" and length == 0) and (.containerInstances | type == "array")))
          | [.[].containerInstances[]]
          | select(length == $count and ([.[].containerInstanceArn] | sort) == $arns)
          | [.[].ec2InstanceId]
          | select(length == $count and all(.[]; type == "string" and test("^i-[A-Za-z0-9]+$")) and length == (unique | length)) | sort
        ' "$dir/controller-instances"); then break; fi
      fi
    fi
    if ! readiness_sleep "$deadline"; then
      echo '::error::controller ECS tasks did not reach current-revision readiness' >&2; exit 1
    fi
  done
  while :; do
    if ! capacity=$(aws_read "$deadline" --region "$region" autoscaling describe-auto-scaling-groups --auto-scaling-group-names "$capacity_asg" --output json); then
      echo '::error::failed to describe controller capacity Auto Scaling group' >&2; exit 1
    fi
    if jq -e --arg name "$capacity_asg" --argjson ids "$controller_ids" '
      .AutoScalingGroups | select(type == "array" and length == 1)
      | .[0] | select(.AutoScalingGroupName == $name)
      | .LaunchTemplate as $template
      | select(($template.LaunchTemplateId | type == "string" and length > 0) and
          ($template.Version | type == "string" and test("^[1-9][0-9]*$")))
      | .Instances | select(type == "array")
      | select(all(.[]; (.InstanceId | type == "string" and test("^i-[A-Za-z0-9]+$")) and
          (.LifecycleState | type == "string" and length > 0)))
      | select(([.[].InstanceId] | length) == ([.[].InstanceId] | unique | length))
      | . as $members
      | select(all(.[]; if (.LifecycleState | startswith("Terminating")) then true else
          .LaunchTemplate.LaunchTemplateId == $template.LaunchTemplateId and
          .LaunchTemplate.Version == $template.Version end))
      | all($ids[]; . as $id | any($members[]; .InstanceId == $id and
          .LifecycleState == "InService" and .HealthStatus == "Healthy"))
    ' <<<"$capacity" >/dev/null; then break; fi
    if ! readiness_sleep "$deadline"; then
      printf '%s\n' "$capacity" >&2
      echo '::error::controller capacity did not reach current launch template readiness' >&2; exit 1
    fi
  done
elif [[ -n $cluster || -n $task_definition || -n $capacity_asg ]]; then
  echo '::error::incomplete controller ECS outputs' >&2; exit 1
elif [[ -n $targets ]]; then
  controller_ids=$(jq -ec '[.controller_instance_id | select(type == "string" and length > 0)] | select(length == 1)' <<<"$gang")
fi

if [[ -n $service && -n $targets ]]; then
  jq -e 'length == 3 and length == (unique | length)' <<<"$(jq -Rsc 'split("\n") | map(select(length > 0))' <<<"$targets")" >/dev/null || { echo '::error::expected three distinct controller target groups' >&2; exit 1; }
fi

if [[ -n $targets ]]; then
  target_index=0
  while IFS= read -r arn; do
    port=443
    if (( target_index == 2 )); then port=9090; fi
    target_index=$((target_index + 1))
    if ! group=$(aws_read "$deadline" --region "$region" elbv2 describe-target-groups --target-group-arns "$arn" --output json); then
      echo "::error::failed to describe target group $arn" >&2; exit 1
    fi
    if ! jq -e --arg arn "$arn" --argjson port "$port" '
      .TargetGroups | type == "array" and length == 1 and
        (.[0] | .TargetGroupArn == $arn and .TargetType == "instance" and .Port == $port)
    ' <<<"$group" >/dev/null; then
      echo "::error::unexpected controller target group type or port in $arn" >&2; exit 1
    fi
    while :; do
      if ! health=$(aws_read "$deadline" --region "$region" elbv2 describe-target-health --target-group-arn "$arn" --output json); then
        echo "::error::failed to describe target health in $arn" >&2; exit 1
      fi
      if jq -e --argjson ids "$controller_ids" --argjson port "$port" '
        .TargetHealthDescriptions | select(type == "array")
        | ([.[].Target.Id] | sort) == $ids
          and all(.[]; .Target.Port == $port and .TargetHealth.State == "healthy")
      ' <<<"$health" >/dev/null; then break; fi
      if ! readiness_sleep "$deadline"; then
        printf '%s\n' "$health" >&2
        echo "::error::controller targets did not reach healthy readiness in $arn" >&2; exit 1
      fi
    done
  done <<<"$targets"
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
