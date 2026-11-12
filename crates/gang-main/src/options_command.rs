//! `gang options`: lists the options each provisioner accepts in `[fleets.options]`.

use std::process::ExitCode;

use gang_spi::OptionSpec;

use crate::errors::{MainError, Result};
use crate::output::{Line, Output};
use crate::plugins::Plugins;
use crate::record::COLUMN;

/// One provisioner plugin's key and the options it accepts.
type Listing = [(&'static str, &'static [OptionSpec])];

/// Prints the options of every provisioner, or only of the one named by `provisioner`.
pub(crate) fn run(provisioner: Option<&str>, out: &Output) -> Result<ExitCode> {
    present(out, Plugins::provisioner_options(), provisioner)?;
    Ok(ExitCode::SUCCESS)
}

/// Prints one man-page block per selected provisioner, blocks separated by a blank line.
///
/// # Errors
///
/// [`MainError::UnknownProvisioner`] when `provisioner` names no key of `listing`; nothing is
/// printed then.
fn present(out: &Output, listing: &Listing, provisioner: Option<&str>) -> Result<()> {
    for (index, (key, specs)) in select(listing, provisioner)?.into_iter().enumerate() {
        if index > 0 {
            out.data("")?;
        }
        present_block(out, key, specs)?;
    }
    Ok(())
}

/// The provisioners `provisioner` selects from `listing`: all of them when it is `None`.
fn select<'a>(
    listing: &'a Listing,
    provisioner: Option<&str>,
) -> Result<Vec<&'a (&'static str, &'static [OptionSpec])>> {
    let Some(name) = provisioner else {
        return Ok(listing.iter().collect());
    };
    let entry = listing
        .iter()
        .find(|(key, _)| *key == name)
        .ok_or_else(|| MainError::UnknownProvisioner {
            name: name.to_owned(),
            accepted: listing
                .iter()
                .map(|(key, _)| *key)
                .collect::<Vec<_>>()
                .join(", "),
        })?;
    Ok(vec![entry])
}

/// The block of one provisioner: its heading, then each option's name and tags with its
/// description below.
fn present_block(out: &Output, key: &str, specs: &[OptionSpec]) -> Result<()> {
    out.data(Line::new().strong(format!("provisioner.{key}")))?;
    if specs.is_empty() {
        out.data("")?;
        return out.data(row().muted("no options"));
    }
    for spec in specs {
        let requirement = if spec.required {
            "required"
        } else {
            "optional"
        };
        let launch = if spec.launch { ", launch-only" } else { "" };
        out.data("")?;
        out.data(
            row()
                .accent(spec.key)
                .plain("  ")
                .muted(format!("{requirement}{launch}")),
        )?;
        out.block(COLUMN + 2, spec.doc)?;
    }
    Ok(())
}

/// An empty row of the option column: the line a name row or a note starts from.
fn row() -> Line {
    Line::new().pad(COLUMN)
}

#[cfg(test)]
mod tests {
    use std::process::ExitCode;

    use gang_spi::OptionSpec;

    use super::{Listing, present, run};
    use crate::errors::MainError;
    use crate::output::Output;

    const LISTING: &Listing = &[
        (
            "aws",
            &[
                OptionSpec {
                    key: "capacity",
                    doc: "`spot` or `on-demand`. Actions request it with the `capacity` property.",
                    required: true,
                    launch: false,
                },
                OptionSpec {
                    key: "machine_class",
                    doc: "Optional qualitative class, for example `gpu`. Actions request it with the \
                          `machine_class` property.",
                    required: false,
                    launch: false,
                },
                OptionSpec {
                    key: "fallback",
                    doc: "Presence opts a spot fleet into on-demand fallback: when Spot launches \
                          fail, new capacity is bought on-demand for `retry_spot_after`. Requires \
                          `capacity = \"spot\"`. Launch-only: it is not a worker property.",
                    required: false,
                    launch: true,
                },
            ],
        ),
        ("local", &[]),
    ];

    /// The plain output of `present`, with what reached stderr asserted empty.
    fn plain(listing: &Listing, provisioner: Option<&str>) -> String {
        let (out, stdout, stderr) = Output::captured();
        present(&out, listing, provisioner).expect("the listing renders");
        assert_eq!(stderr.text(), "");
        stdout.text()
    }

    #[test]
    fn lists_every_provisioner_as_man_page_blocks() {
        assert_eq!(
            plain(LISTING, None),
            "provisioner.aws\n\
             \n\
             \x20       capacity  required\n\
             \x20         `spot` or `on-demand`. Actions request it with the `capacity` property.\n\
             \n\
             \x20       machine_class  optional\n\
             \x20         Optional qualitative class, for example `gpu`. Actions request it with the \
             `machine_class` property.\n\
             \n\
             \x20       fallback  optional, launch-only\n\
             \x20         Presence opts a spot fleet into on-demand fallback: when Spot launches fail, \
             new capacity is bought on-demand for `retry_spot_after`. Requires \
             `capacity = \"spot\"`. Launch-only: it is not a worker property.\n\
             \n\
             provisioner.local\n\
             \n\
             \x20       no options\n"
        );
    }

    #[test]
    fn named_provisioner_lists_only_its_block() {
        assert_eq!(
            plain(LISTING, Some("local")),
            "provisioner.local\n\n        no options\n"
        );
    }

    #[test]
    fn unknown_provisioner_is_an_error_listing_the_accepted_keys_and_prints_nothing() {
        let (out, stdout, stderr) = Output::captured();
        let error = present(&out, LISTING, Some("gcp")).expect_err("gcp is not a provisioner");
        match error {
            MainError::UnknownProvisioner { name, accepted } => {
                assert_eq!(name, "gcp");
                assert_eq!(accepted, "aws, local");
            }
            other => panic!("expected an unknown provisioner error, got {other:?}"),
        }
        assert_eq!(stdout.text(), "");
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn only_launch_options_carry_the_launch_only_tag() {
        let text = plain(LISTING, Some("aws"));
        let tagged: Vec<_> = text
            .lines()
            .filter(|line| line.ends_with(", launch-only"))
            .collect();
        assert_eq!(tagged, ["        fallback  optional, launch-only"]);
        assert!(text.contains("\n        capacity  required\n"));
    }

    #[test]
    fn an_unknown_provisioner_lists_the_accepted_ones() {
        let (out, _stdout, _stderr) = Output::captured();
        let error = run(Some("nope"), &out).expect_err("nope is not a provisioner");
        match error {
            MainError::UnknownProvisioner { name, accepted } => {
                assert_eq!(name, "nope");
                assert!(accepted.contains("aws"), "accepted: {accepted}");
                assert!(accepted.contains("local"), "accepted: {accepted}");
            }
            other => panic!("expected an unknown provisioner error, got {other:?}"),
        }
    }

    #[test]
    fn a_named_provisioner_prints_only_its_options() {
        let (out, stdout, stderr) = Output::captured();
        let code = run(Some("local"), &out).expect("local is a provisioner");
        assert_eq!(code, ExitCode::SUCCESS);
        assert_eq!(stderr.text(), "");
        let text = stdout.text();
        assert!(text.contains("local"), "stdout: {text}");
        assert!(!text.contains("capacity"), "stdout: {text}");
        assert!(!text.contains("machine_class"), "stdout: {text}");
        assert!(!text.contains("fallback"), "stdout: {text}");
    }

    #[test]
    fn no_name_prints_every_provisioner() {
        let (out, stdout, stderr) = Output::captured();
        let code = run(None, &out).expect("the listing renders");
        assert_eq!(code, ExitCode::SUCCESS);
        assert_eq!(stderr.text(), "");
        let text = stdout.text();
        assert!(text.contains("provisioner.aws"), "stdout: {text}");
        assert!(text.contains("provisioner.local"), "stdout: {text}");
    }
}
