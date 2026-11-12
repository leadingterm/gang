//! Checks the actual workspace against its architecture plan.

use std::{error::Error, path::Path};
use workspace_architecture::{Plan, Workspace, check};

#[test]
fn workspace_follows_architecture_plan() -> Result<(), Box<dyn Error>> {
    let manifest = std::env::var("GANG_WORKSPACE_MANIFEST")?;
    let plan = Plan::parse(&std::fs::read_to_string(std::env::var("GANG_PLAN")?)?)?;
    let root = Path::new(&manifest)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let workspace = Workspace::load(root)?;
    let violations = check(&plan, &workspace)?;
    assert!(
        violations.is_empty(),
        "{}",
        violations
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    );
    Ok(())
}
