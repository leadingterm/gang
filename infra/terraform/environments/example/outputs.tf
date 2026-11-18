output "gang" {
  description = "Every deployment output, including region, controller_shape, controller_instance_id, endpoint and gang_toml. The deploy workflow uses the shape to select configuration rollout behavior."
  value       = module.gang
}
