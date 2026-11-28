locals {
  # Ownership: The module's version is its repository's [workspace.package] version; git sources fetch the whole repository.
  version               = regex("(?ms)^\\[workspace\\.package\\]$.*?^version = \"([^\"]+)\"$", file("${path.module}/../../../../Cargo.toml"))[0]
  effective_release_tag = var.release_tag != null ? var.release_tag : "v${local.version}"
  # Correctness: Map each fleet ISA to Gang's image architecture.
  isa_architectures       = { "x86-64" = "x86_64", aarch64 = "aarch64" }
  controller_architecture = contains(data.aws_ec2_instance_type.controller.supported_architectures, "arm64") ? "aarch64" : "x86_64"
  # Ownership: Only architectures without an override come from this module's release.
  release_architectures = setsubtract(toset(concat(local.group ? [] : [local.controller_architecture], [for fleet in var.fleets : local.isa_architectures[fleet.isa] if fleet.image == null])), keys(var.images))
  release_images = { for architecture in local.release_architectures : architecture => {
    owner  = var.image_publisher
    sha256 = regex("(?m)^([0-9a-f]{64})  gang-image-${architecture}\\.raw\\.zst$", data.http.sha256sums[0].response_body)[0]
  } }
  architecture_images = merge(local.release_images, var.images)
  # Correctness: A fleet override does not require an architecture-level image.
  node_images = merge(
    local.group ? {} : { controller = merge(local.architecture_images[local.controller_architecture], { architecture = local.controller_architecture }) },
    { for fleet in var.fleets : fleet.name => merge(fleet.image == null ? local.architecture_images[local.isa_architectures[fleet.isa]] : fleet.image, { architecture = local.isa_architectures[fleet.isa] }) },
  )
  # Correctness: Group nodes sharing an image into one lookup.
  grouped_images           = { for image in values(local.node_images) : "${image.owner}/${image.architecture}/${image.sha256}" => image... }
  image_lookups            = { for key, images in local.grouped_images : key => images[0] }
  node_ami_ids             = { for node, image in local.node_images : node => data.aws_ami.image["${image.owner}/${image.architecture}/${image.sha256}"].id }
  node_action_environments = { for node, image in local.node_images : node => lookup(data.aws_ami.image["${image.owner}/${image.architecture}/${image.sha256}"].tags, "ec2:SharedTag/ActionEnvironment", "") }
  # Ownership: A superseded release is deprecated at least 184 days after its successor and deregistered at least 92 days later.
  deprecation_warning_window = "4416h"
  deregistration_delay       = "2208h"
}

data "aws_ec2_instance_type" "controller" {
  instance_type = var.controller_instance_type
}

# Correctness: The SHA-256 of each image comes from the release of the selected release.
data "http" "sha256sums" {
  count = length(local.release_architectures) > 0 ? 1 : 0
  url   = "https://github.com/${var.release_repository}/releases/download/${local.effective_release_tag}/SHA256SUMS"

  lifecycle {
    postcondition {
      condition     = self.status_code == 200 && alltrue([for architecture in local.release_architectures : length(regexall("(?m)^[0-9a-f]{64}  gang-image-${architecture}\\.raw\\.zst$", self.response_body)) == 1])
      error_message = "${self.url} must list gang-image-<architecture>.raw.zst once for ${join(", ", sort(local.release_architectures))} (status ${self.status_code}): the module is used outside a release of ${var.release_repository}; set images for those architectures."
    }
  }
}

# Security: An image is the owner's AMI named after the SHA-256, and its shared tag must carry that SHA-256.
data "aws_ami" "image" {
  for_each = local.image_lookups

  owners             = [each.value.owner]
  include_deprecated = true

  filter {
    name   = "name"
    values = ["gang-${each.value.architecture}-${substr(each.value.sha256, 0, 16)}"]
  }

  filter {
    name   = "architecture"
    values = [each.value.architecture == "aarch64" ? "arm64" : "x86_64"]
  }

  lifecycle {
    postcondition {
      condition     = lookup(self.tags, "ec2:SharedTag/ImageSha256", "") == each.value.sha256 && self.boot_mode == "uefi"
      error_message = "${self.name} (${self.id}) of ${each.value.owner} must carry ec2:SharedTag/ImageSha256 = ${each.value.sha256} and boot UEFI."
    }
    postcondition {
      condition     = can(regex("^[0-9a-f]{64}$", lookup(self.tags, "ec2:SharedTag/ActionEnvironment", "")))
      error_message = "${self.name} (${self.id}) must carry ec2:SharedTag/ActionEnvironment as 64 lowercase hexadecimal digits."
    }
  }
}

check "images_current" {
  assert {
    condition     = alltrue([for image in data.aws_ami.image : image.deprecation_time == "" ? true : timecmp(image.deprecation_time, timeadd(plantimestamp(), local.deprecation_warning_window)) > 0])
    error_message = "Deprecated within 6 months: ${join("; ", [for image in data.aws_ami.image : "${image.name} on ${image.deprecation_time}, deregistered from ${timeadd(image.deprecation_time, local.deregistration_delay)}" if image.deprecation_time == "" ? false : timecmp(image.deprecation_time, timeadd(plantimestamp(), local.deprecation_warning_window)) <= 0])}. Move to a newer Gang release."
  }
}
