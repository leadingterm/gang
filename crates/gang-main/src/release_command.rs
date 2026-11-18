//! The hidden `gang release` commands: public AMI publication decisions.

use std::collections::BTreeMap;
use std::io::Read;
use std::process::ExitCode;

use gang_deploy::{Architecture, ImageSha256, Publication, Regions, Release, ReleaseTag};

use crate::cli::{AmiPublicationArgs, ReleaseCommand};
use crate::errors::{MainError, Result};
use crate::output::Output;

/// Runs one `gang release` command.
pub(crate) fn run(command: ReleaseCommand, mut input: impl Read, out: &Output) -> Result<ExitCode> {
    let (args, release, manifest) = match command {
        ReleaseCommand::AmiSteps(args) => (
            args.publication,
            args.release
                .map(|tag| release(tag, args.image))
                .transpose()?,
            false,
        ),
        ReleaseCommand::AmiManifest(args) => (
            args.publication,
            Some(release(args.release, args.image)?),
            true,
        ),
    };
    let mut text = String::new();
    input.read_to_string(&mut text).map_err(MainError::Stdin)?;
    let regions: Regions = serde_json::from_str(&text).map_err(MainError::ReleaseJson)?;
    let AmiPublicationArgs { home_region, now } = args;
    let publication = Publication::new(home_region, regions, release, now)?;
    if manifest {
        let manifest = publication.manifest()?;
        out.data(serde_json::to_string_pretty(&manifest).map_err(MainError::ReleaseJson)?)?;
    } else {
        for step in publication.steps()? {
            out.data(step.to_string())?;
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// Builds a release only when each architecture has exactly one digest.
fn release(tag: ReleaseTag, images: Vec<(Architecture, ImageSha256)>) -> Result<Release> {
    let mut by_arch = BTreeMap::new();
    for (arch, sha) in images {
        if by_arch.insert(arch, sha).is_some() {
            return Err(MainError::ReleaseImages);
        }
    }
    if by_arch.len() != 2
        || !by_arch.contains_key(&Architecture::X86_64)
        || !by_arch.contains_key(&Architecture::Aarch64)
    {
        return Err(MainError::ReleaseImages);
    }
    Ok(Release {
        tag,
        images: by_arch,
    })
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::*;
    use crate::cli::Command;

    fn gang_release(args: &[&str], stdin: &str) -> (ExitCode, String, String) {
        let command_line: Vec<OsString> = ["gang", "release"]
            .into_iter()
            .chain(args.iter().copied())
            .map(OsString::from)
            .collect();
        let parsed = crate::dispatch::parse(&command_line).expect("valid release arguments");
        let Command::Release(command) = parsed.command else {
            panic!("expected a release command");
        };
        let subject = command.subject();
        let (out, stdout, stderr) = Output::captured();
        let code = run(command, stdin.as_bytes(), &out)
            .unwrap_or_else(|error| out.failure(subject, &error));
        (code, stdout.text(), stderr.text())
    }

    #[test]
    fn backfill_of_empty_regions_has_no_steps() {
        let (code, stdout, _) = gang_release(
            &["ami-steps", "--home-region", "us-east-1", "--now", "0"],
            r#"{"us-east-1":{"quota":10,"images":{"Images":[]},"snapshots":{"Snapshots":[]}}}"#,
        );
        assert_eq!(code, ExitCode::SUCCESS);
        assert!(stdout.is_empty());
    }

    #[test]
    fn release_steps_and_manifest_use_the_region_document() {
        let x86 = "x86_64=0000000000000000000000000000000000000000000000000000000000000000";
        let arm = "aarch64=1111111111111111111111111111111111111111111111111111111111111111";
        let args = [
            "--home-region",
            "us-east-1",
            "--now",
            "1767312000",
            "--release",
            "v1",
            "--image",
            x86,
            "--image",
            arm,
        ];
        let mut steps_args = vec!["ami-steps"];
        steps_args.extend(args);
        let (code, stdout, _) = gang_release(
            &steps_args,
            r#"{"us-east-1":{"quota":10,"images":{"Images":[]},"snapshots":{"Snapshots":[]}}}"#,
        );
        assert_eq!(code, ExitCode::SUCCESS);
        assert!(stdout.contains("upload\tx86_64\tgang-x86_64-0000000000000000\t"));
        assert!(stdout.contains("upload\taarch64\tgang-aarch64-1111111111111111\t"));

        let image = |id: &str, arch: &str, sha: &str| {
            serde_json::json!({
                "ImageId": id,
                "Name": format!("gang-{arch}-{}", &sha[..16]),
                "State": "available",
                "Public": true,
                "CreationDate": "2026-01-01T00:00:00Z",
                "DeregistrationProtection": "enabled-with-cooldown",
                "Tags": [
                    {"Key": "ec2:SharedTag/GangRelease", "Value": "v1"},
                    {"Key": "ec2:SharedTag/ImageSha256", "Value": sha}
                ]
            })
        };
        let regions = serde_json::json!({
            "us-east-1": {"quota": 10, "images": {"Images": [
                image("ami-01", "x86_64", &x86[7..]),
                image("ami-02", "aarch64", &arm[8..])
            ]}, "snapshots": {"Snapshots": []}}
        });
        let mut manifest_args = vec!["ami-manifest"];
        manifest_args.extend(args);
        let (code, stdout, _) = gang_release(&manifest_args, &regions.to_string());
        assert_eq!(code, ExitCode::SUCCESS);
        let manifest: serde_json::Value = serde_json::from_str(&stdout).expect("manifest json");
        assert_eq!(manifest["x86_64"]["sha256"], &x86[7..]);
        assert_eq!(manifest["x86_64"]["regions"]["us-east-1"], "ami-01");
        assert_eq!(manifest["aarch64"]["sha256"], &arm[8..]);
        assert_eq!(manifest["aarch64"]["regions"]["us-east-1"], "ami-02");
    }

    #[test]
    fn release_requires_both_distinct_images() {
        let sha = "x86_64=0000000000000000000000000000000000000000000000000000000000000000";
        for images in [vec!["--image", sha], vec!["--image", sha, "--image", sha]] {
            let mut args = vec![
                "ami-steps",
                "--home-region",
                "us-east-1",
                "--now",
                "0",
                "--release",
                "v1",
            ];
            args.extend(images);
            let (code, stdout, stderr) = gang_release(&args, "{}");
            assert_ne!(code, ExitCode::SUCCESS);
            assert!(stdout.is_empty());
            assert!(stderr.contains("exactly one image for each architecture"));
        }
    }
}
