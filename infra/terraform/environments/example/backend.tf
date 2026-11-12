terraform {
  # Partial configuration: bucket, key, region, kms_key_id and use_lockfile arrive
  # through -backend-config at `tofu init` time.
  backend "s3" {}
}
