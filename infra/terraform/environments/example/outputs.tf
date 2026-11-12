output "gang" {
  description = "Every output of the deployment module: region, controller_instance_id, endpoint, issuer_url, gang_toml, ... The deploy workflow reads region and controller_instance_id from it."
  value       = module.gang
}
