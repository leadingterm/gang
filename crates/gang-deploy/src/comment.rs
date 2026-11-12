//! The plan comment a pull request shows: the verdict, the action table, the changed resources,
//! and the hidden manifest line the merge reads back.

use std::fmt;

use crate::github::PLAN_COMMENT_MARKER;
use crate::manifest::Manifest;
use crate::plan::Plan;
use crate::sha::Sha;

/// The most changed resources the comment lists.
const LISTED_CHANGES: usize = 200;

/// Where the comment takes the planned tree from.
#[derive(Debug, Clone, Copy)]
pub enum CommentSource<'a> {
    /// No plan was stored: the tree of the checked-out commit.
    Tree(&'a Sha),
    /// A plan was stored: its manifest, whose line the comment carries when the plan completed.
    Manifest(&'a Manifest),
}

/// The body of the plan comment; it prints as the Markdown the pull request shows.
#[derive(Debug, Clone, Copy)]
pub struct PlanComment<'a> {
    /// Where the planned tree comes from.
    source: CommentSource<'a>,
    /// The plan; `None` when the plan did not complete.
    plan: Option<&'a Plan>,
}

impl<'a> PlanComment<'a> {
    /// The comment for `plan` of the tree `source` names; `plan` is `None` when planning did not
    /// complete.
    #[must_use]
    pub const fn new(source: CommentSource<'a>, plan: Option<&'a Plan>) -> Self {
        Self { source, plan }
    }
}

impl fmt::Display for PlanComment<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tree = match self.source {
            CommentSource::Tree(tree) => tree,
            CommentSource::Manifest(manifest) => &manifest.facts.tree_sha,
        };
        writeln!(f, "{PLAN_COMMENT_MARKER}")?;
        writeln!(f, "## Infrastructure plan")?;
        writeln!(f)?;
        writeln!(f, "Tree: `{tree}`")?;
        writeln!(f)?;
        let Some(plan) = self.plan else {
            return writeln!(f, "**Plan did not complete.** See the workflow run logs.");
        };

        let protected = plan.protected_changes();
        if !protected.is_empty() {
            writeln!(
                f,
                "**Blocked: this revision must not merge.** The plan deletes or replaces \
                 protected resources:"
            )?;
            writeln!(f)?;
            for change in &protected {
                writeln!(
                    f,
                    "- `{}` ({}: {})",
                    change.address,
                    change.resource_type,
                    change.actions.join(",")
                )?;
            }
            writeln!(f)?;
        } else if plan.has_changes() {
            writeln!(
                f,
                "Plan has changes. Merging this pull request applies this exact saved plan."
            )?;
            writeln!(f)?;
        } else {
            writeln!(f, "No changes.")?;
            writeln!(f)?;
        }

        writeln!(f, "| Action | Count |")?;
        writeln!(f, "| --- | ---: |")?;
        for (kind, count) in plan.action_counts() {
            writeln!(f, "| {kind} | {count} |")?;
        }
        writeln!(f)?;

        writeln!(f, "Changed resources:")?;
        writeln!(f)?;
        let mut changed = plan.changed_resources();
        for resource in changed.by_ref().take(LISTED_CHANGES) {
            writeln!(
                f,
                "- `{}` ({})",
                resource.address,
                resource.change.actions.join(", ")
            )?;
        }
        let remaining = changed.count();
        if remaining > 0 {
            writeln!(f, "- ... and {remaining} more")?;
        }
        writeln!(f)?;

        if let CommentSource::Manifest(manifest) = self.source {
            writeln!(f, "{}", manifest.comment_line())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{CommentSource, PlanComment};
    use crate::manifest::MANIFEST_LINE_PREFIX;
    use crate::manifest::tests::{TREE, manifest};
    use crate::plan::Plan;
    use crate::plan::tests::{fixture, plan, resource};
    use crate::sha::Sha;

    /// The tree of the checked-out commit when no manifest exists.
    const HEAD_TREE: &str = "8888888888888888888888888888888888888888";

    fn tree() -> Sha {
        HEAD_TREE.parse().unwrap()
    }

    fn render(plan: &Plan) -> String {
        PlanComment::new(CommentSource::Tree(&tree()), Some(plan)).to_string()
    }

    fn eip_delete() -> Value {
        resource("aws_eip", "aws_eip.gateway", &["delete"])
    }

    fn kms_create() -> Value {
        resource("aws_kms_key", "aws_kms_key.state", &["create"])
    }

    #[test]
    fn render_comment_writes_the_marker_heading_and_tree() {
        let body = render(&plan(&[]));
        assert!(body.starts_with(&format!(
            "<!-- gang-deploy-plan -->\n## Infrastructure plan\n\nTree: `{HEAD_TREE}`\n\n"
        )));
    }

    #[test]
    fn render_comment_reports_a_blocked_plan_when_a_protected_resource_is_deleted() {
        let body = render(&plan(&[eip_delete(), kms_create()]));
        assert!(body.contains(
            "**Blocked: this revision must not merge.** The plan deletes or replaces protected \
             resources:\n\n- `aws_eip.gateway` (aws_eip: delete)\n\n| Action"
        ));
    }

    #[test]
    fn render_comment_reports_that_merging_applies_the_saved_plan_when_there_are_changes() {
        let body = render(&fixture("parameter-value"));
        assert!(body.contains(
            "\n\nPlan has changes. Merging this pull request applies this exact saved plan.\n\n"
        ));
        assert!(!body.contains(MANIFEST_LINE_PREFIX));
    }

    #[test]
    fn render_comment_reports_no_changes_for_a_no_op_plan() {
        let body = render(&fixture("no-op"));
        assert!(body.contains("\n\nNo changes.\n\n| Action | Count |\n| --- | ---: |\n"));
        assert!(body.ends_with("Changed resources:\n\n\n"));
    }

    #[test]
    fn render_comment_reports_an_incomplete_plan_without_a_manifest_line() {
        let manifest = manifest();
        let body = PlanComment::new(CommentSource::Manifest(&manifest), None).to_string();
        assert!(body.ends_with("\n\n**Plan did not complete.** See the workflow run logs.\n"));
        assert!(!body.contains(MANIFEST_LINE_PREFIX));
    }

    #[test]
    fn render_comment_writes_the_exact_template() {
        let body = render(&plan(&[
            kms_create(),
            kms_create(),
            resource(
                "aws_ssm_parameter",
                "module.gang.aws_ssm_parameter.config",
                &["update"],
            ),
            resource("aws_instance", "aws_instance.worker", &["delete"]),
            resource("aws_kms_key", "aws_kms_key.state", &["delete", "create"]),
            resource("aws_iam_role", "aws_iam_role.reader", &["read"]),
        ]));
        assert_eq!(
            body,
            format!(
                "<!-- gang-deploy-plan -->\n## Infrastructure plan\n\nTree: `{HEAD_TREE}`\n\n\
                 **Blocked: this revision must not merge.** The plan deletes or replaces \
                 protected resources:\n\n\
                 - `aws_kms_key.state` (aws_kms_key: delete,create)\n\n\
                 | Action | Count |\n| --- | ---: |\n\
                 | create | 2 |\n| update | 1 |\n| delete | 1 |\n| replace | 1 |\n\n\
                 Changed resources:\n\n\
                 - `aws_kms_key.state` (create)\n\
                 - `aws_kms_key.state` (create)\n\
                 - `module.gang.aws_ssm_parameter.config` (update)\n\
                 - `aws_instance.worker` (delete)\n\
                 - `aws_kms_key.state` (delete, create)\n\n"
            )
        );
    }

    #[test]
    fn render_comment_lists_at_most_200_changed_resources() {
        let many: Vec<Value> = (1..=205)
            .map(|index| {
                resource(
                    "aws_ssm_parameter",
                    &format!("module.gang.aws_ssm_parameter.config[{index}]"),
                    &["update"],
                )
            })
            .collect();
        let body = render(&plan(&many));
        let listed: Vec<&str> = body
            .lines()
            .filter(|line| line.starts_with("- `"))
            .collect();
        assert_eq!(listed.len(), 200);
        assert_eq!(
            listed.last().copied(),
            Some("- `module.gang.aws_ssm_parameter.config[200]` (update)")
        );
        assert!(body.contains("config[200]` (update)\n- ... and 5 more\n\n"));
        assert!(!body.contains("config[201]"));
        assert!(body.contains("| update | 205 |"));
    }

    #[test]
    fn render_comment_takes_the_tree_from_the_manifest_when_present() {
        let manifest = manifest();
        let body =
            PlanComment::new(CommentSource::Manifest(&manifest), Some(&plan(&[]))).to_string();
        assert!(body.contains(&format!("Tree: `{TREE}`")));
        assert!(!body.contains(HEAD_TREE));
    }

    #[test]
    fn render_comment_ends_the_body_with_the_manifest_line() {
        let manifest = manifest();
        let body =
            PlanComment::new(CommentSource::Manifest(&manifest), Some(&plan(&[]))).to_string();
        assert!(body.ends_with(&format!("\n\n{}\n", manifest.comment_line())));
        assert_eq!(
            crate::manifest::Manifest::from_comment(&body).unwrap(),
            manifest
        );
    }

    /// The comments the shell `render_comment` printed for real plans of the tree `HEAD_TREE`.
    #[test]
    fn render_comment_matches_the_shell_rendering_of_real_plans() {
        for (name, expected) in [
            (
                "blocked",
                include_str!("../tests/fixtures/blocked-comment.md"),
            ),
            (
                "parameter-unknown",
                include_str!("../tests/fixtures/parameter-unknown-comment.md"),
            ),
            ("no-op", include_str!("../tests/fixtures/no-op-comment.md")),
        ] {
            assert_eq!(render(&fixture(name)), expected, "{name}");
        }
    }
}
