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

if [[ -n $asg ]]; then
  deadline=$((SECONDS + 600))
  while :; do
    if ! refresh=$(aws --region "$region" autoscaling describe-instance-refreshes --auto-scaling-group-name "$asg" --max-records 1 --no-paginate --output json); then
      echo '::error::failed to describe controller instance refreshes' >&2; exit 1
    fi
    status=$(jq -er 'if (.InstanceRefreshes | type) != "array" then error("missing refreshes") elif .InstanceRefreshes | length == 0 then "None" else .InstanceRefreshes[0].Status | select(type == "string" and length > 0) end' <<<"$refresh")
    case $status in
      None|Successful) break ;;
      Pending|InProgress|Baking|Cancelling|RollbackInProgress)
        if (( SECONDS >= deadline )); then echo "::error::controller instance refresh timed out: $status" >&2; exit 1; fi
        sleep 5 ;;
      *) reason=$(jq -r '.InstanceRefreshes[0].StatusReason // ""' <<<"$refresh")
        echo "::error::controller instance refresh $status: $reason" >&2; exit 1 ;;
    esac
  done
  if ! groups=$(aws --region "$region" autoscaling describe-auto-scaling-groups --auto-scaling-group-names "$asg" --output json); then
    echo "::error::failed to describe controller Auto Scaling group $asg" >&2; exit 1
  fi
  if ! controller_ids=$(jq -ec --arg name "$asg" '
    .AutoScalingGroups | select(type == "array" and length == 1 and .[0].AutoScalingGroupName == $name) | .[0]
    | select((.DesiredCapacity | type == "number" and . > 0) and (.Instances | type == "array") and .DesiredCapacity == (.Instances | length))
    | .LaunchTemplate as $template
    | select(($template.LaunchTemplateId | type == "string" and length > 0) and ($template.Version | type == "string" and test("^[0-9]+$")))
    | select(all(.Instances[]; .HealthStatus == "Healthy" and .LifecycleState == "InService" and .LaunchTemplate.LaunchTemplateId == $template.LaunchTemplateId and (.LaunchTemplate.Version | tostring) == $template.Version and (.InstanceId | type == "string" and length > 0)))
    | [.Instances[].InstanceId] | sort | select(length == (unique | length))
  ' <<<"$groups"); then
    echo "::error::controller Auto Scaling group $asg is not healthy on its current launch template" >&2; exit 1
  fi
elif [[ -n $targets ]]; then
  controller_ids=$(jq -ec '[.controller_instance_id | select(type == "string" and length > 0)] | select(length == 1)' <<<"$gang")
fi

if [[ -n $targets ]]; then
  while IFS= read -r arn; do
    deadline=$((SECONDS + 600))
    while :; do
      if ! health=$(aws --region "$region" elbv2 describe-target-health --target-group-arn "$arn" --output json); then
        echo "::error::failed to describe target health in $arn" >&2; exit 1
      fi
      if jq -e --argjson ids "$controller_ids" '
        .TargetHealthDescriptions | select(type == "array")
        | ([.[].Target.Id] | sort) == $ids
          and ([.[] | select(.TargetHealth.State == "healthy")] | length == 1)
          and all(.[]; .TargetHealth.State == "healthy" or (.TargetHealth.State == "unhealthy" and .TargetHealth.Reason == "Target.ResponseCodeMismatch" and (.TargetHealth.Description | type == "string" and contains("[503]"))))
      ' <<<"$health" >/dev/null; then break; fi
      if (( SECONDS >= deadline )); then
        printf '%s\n' "$health" >&2
        echo "::error::controller targets did not reach leader/passive readiness in $arn" >&2; exit 1
      fi
      sleep 5
    done
  done <<<"$targets"
fi

if [[ -n $workers ]]; then
  while IFS= read -r worker; do
    if ! groups=$(aws --region "$region" autoscaling describe-auto-scaling-groups --auto-scaling-group-names "$worker" --output json); then
      echo "::error::failed to describe worker Auto Scaling group $worker" >&2; exit 1
    fi
    if ! jq -e --arg name "$worker" '.AutoScalingGroups | type == "array" and length == 1 and .[0].AutoScalingGroupName == $name and (.[0] | (.DesiredCapacity | type == "number") and (.Instances | type == "array") and (.DesiredCapacity == (.Instances | length)) and all(.Instances[]; .HealthStatus == "Healthy" and .LifecycleState == "InService"))' <<<"$groups" >/dev/null; then
      echo "::error::worker Auto Scaling group $worker is missing or has unhealthy or insufficient instances" >&2; exit 1
    fi
  done <<<"$workers"
fi

curl_trust=()
if [[ -z $targets ]]; then
  dir=$(mktemp -d)
  trap 'rm -rf "$dir"' EXIT
  jq -er '.gang_toml | select(type == "string" and length > 0)' <<<"$gang" >"$dir/gang.toml"
  if ! pin=$("${GANG:-gang}" ca-pin --config "$dir/gang.toml"); then
    echo '::error::failed to derive controller CA pin' >&2; exit 1
  fi
  if ! bootstrap=$(curl --fail --silent --show-error --connect-timeout 10 --max-time 20 --insecure "${endpoint%/}/v1/server"); then
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

if ! response=$(curl --fail --silent --show-error --connect-timeout 10 --max-time 20 "${curl_trust[@]}" "${endpoint%/}/v1/server"); then
  echo "::error::failed to query server version at $endpoint" >&2; exit 1
fi
if ! version=$(jq -er '.version | select(type == "string" and length > 0)' <<<"$response"); then
  echo '::error::server response has no version' >&2; exit 1
fi
if [[ $version != "$expected" ]]; then
  echo "::error::server version mismatch: expected $expected, observed $version" >&2; exit 1
fi
