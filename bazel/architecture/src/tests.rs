use std::{error::Error, fs, path::Path};

use super::architecture::{Plan, Violation, Workspace, check};

fn plan(rows: &str) -> Result<Plan, Box<dyn Error>> {
    Ok(Plan::parse(&format!(
        "| Crate Name | Ring | Tier | Role |\n|---|---|---|---|\n{rows}"
    ))?)
}

fn package(root: &Path, name: &str, extra: &str, lib: &str) -> Result<(), Box<dyn Error>> {
    let dir = root.join(name);
    fs::create_dir_all(dir.join("src"))?;
    fs::write(
        dir.join("Cargo.toml"),
        format!("[package]\nname = \"{name}\"\nversion = \"0.1.0\"\n{extra}"),
    )?;
    fs::write(dir.join("src/lib.rs"), lib)?;
    Ok(())
}

fn setup(root: &Path) -> Result<(), Box<dyn Error>> {
    fs::write(
        root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"a\", \"b\"]\n",
    )?;
    package(root, "a", "", "mod child;\npub use child::Thing;\n")?;
    package(root, "b", "", "")?;
    Ok(())
}

const CLEAN: &str = "| `a` | `ring` | 2 | a |\n| `b` | `ring` | 1 | b |\n";

#[test]
fn membership_reports_missing_and_extra() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    setup(temp.path())?;
    let workspace = Workspace::load(temp.path())?;
    assert!(check(&plan(CLEAN)?, &workspace)?.is_empty());
    let violations = check(
        &plan("| `a` | `ring` | 2 | a |\n| `c` | `ring` | 1 | c |\n")?,
        &workspace,
    )?;
    assert!(violations.contains(&Violation::MissingMember { name: "b".into() }));
    assert!(violations.contains(&Violation::ExtraPlanCrate { name: "c".into() }));
    Ok(())
}

#[test]
fn tier_edges_are_strict_and_dev_dependencies_are_exempt() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    setup(temp.path())?;
    package(
        temp.path(),
        "a",
        "[dependencies]\nb = { path = \"../b\" }\n",
        "",
    )?;
    let workspace = Workspace::load(temp.path())?;
    assert!(check(&plan(CLEAN)?, &workspace)?.is_empty());
    for tier in ["2", "3", "-"] {
        let rows = format!("| `a` | `ring` | 2 | a |\n| `b` | `ring` | {tier} | b |\n");
        assert!(check(&plan(&rows)?, &workspace)?.iter().any(
            |v| matches!(v, Violation::TierEdge { from, to, .. } if from == "a" && to == "b")
        ));
    }
    package(
        temp.path(),
        "a",
        "[dev-dependencies]\nb = { path = \"../b\" }\n",
        "",
    )?;
    let workspace = Workspace::load(temp.path())?;
    assert!(
        check(
            &plan("| `a` | `ring` | 2 | a |\n| `b` | `ring` | - | b |\n")?,
            &workspace
        )?
        .is_empty()
    );
    package(
        temp.path(),
        "a",
        "[build-dependencies]\nb = { path = \"../b\" }\n",
        "",
    )?;
    assert!(
        check(
            &plan("| `a` | `ring` | 2 | a |\n| `b` | `ring` | 2 | b |\n")?,
            &Workspace::load(temp.path())?
        )?
        .iter()
        .any(|v| matches!(v, Violation::TierEdge { .. }))
    );
    Ok(())
}

#[test]
fn nested_rust_source_is_rejected() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    setup(temp.path())?;
    assert!(check(&plan(CLEAN)?, &Workspace::load(temp.path())?)?.is_empty());
    fs::create_dir_all(temp.path().join("a/src/nested"))?;
    fs::write(temp.path().join("a/src/nested/item.rs"), "")?;
    assert!(check(&plan(CLEAN)?, &Workspace::load(temp.path())?)?.iter().any(|v| matches!(v, Violation::NestedSource { file, .. } if file == Path::new("a/src/nested/item.rs"))));
    Ok(())
}

#[test]
fn nested_module_path_attributes_are_rejected_in_tests() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    setup(temp.path())?;
    fs::create_dir_all(temp.path().join("a/tests"))?;
    fs::write(
        temp.path().join("a/tests/check.rs"),
        "mod outer { mod inner; }",
    )?;
    assert!(check(&plan(CLEAN)?, &Workspace::load(temp.path())?)?.is_empty());
    fs::write(
        temp.path().join("a/tests/check.rs"),
        "mod outer { #[path = \"elsewhere.rs\"] mod inner; }",
    )?;
    assert!(
        check(&plan(CLEAN)?, &Workspace::load(temp.path())?)?
            .iter()
            .any(|v| matches!(v, Violation::PathAttribute { module, .. } if module == "inner"))
    );
    Ok(())
}

#[test]
fn missing_member_and_missing_src_fail_to_load() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    setup(temp.path())?;
    fs::remove_dir_all(temp.path().join("a/src"))?;
    assert!(matches!(
        Workspace::load(temp.path()),
        Err(super::architecture::Error::Io { .. })
    ));
    fs::remove_dir_all(temp.path().join("a"))?;
    assert!(matches!(
        Workspace::load(temp.path()),
        Err(super::architecture::Error::Io { .. })
    ));
    Ok(())
}

#[test]
fn invalid_rust_source_is_reported_as_an_error() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    setup(temp.path())?;
    fs::write(temp.path().join("a/src/child.rs"), "mod broken {")?;
    assert!(matches!(
        check(&plan(CLEAN)?, &Workspace::load(temp.path())?),
        Err(super::architecture::Error::Rust { .. })
    ));
    Ok(())
}

#[cfg(unix)]
#[test]
fn symlinked_test_sources_are_checked() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    setup(temp.path())?;
    fs::create_dir_all(temp.path().join("a/tests"))?;
    let target = temp.path().join("scenario.rs");
    fs::write(&target, "#[path = \"scenarios/support.rs\"] mod support;")?;
    std::os::unix::fs::symlink(&target, temp.path().join("a/tests/scenarios.rs"))?;
    assert!(
        check(&plan(CLEAN)?, &Workspace::load(temp.path())?)?
            .iter()
            .any(|v| matches!(
                v,
                Violation::PathAttribute { module, file, .. }
                    if module == "support" && file == Path::new("a/tests/scenarios.rs")
            ))
    );
    Ok(())
}

#[test]
fn facade_allows_restricted_reexports_but_not_private_uses() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    setup(temp.path())?;
    let file = temp.path().join("a/src/lib.rs");
    fs::write(&file, "mod child;\npub(crate) use child::Thing;\n")?;
    assert!(check(&plan(CLEAN)?, &Workspace::load(temp.path())?)?.is_empty());
    fs::write(&file, "mod child;\nuse child::Thing;\n")?;
    assert!(
        check(&plan(CLEAN)?, &Workspace::load(temp.path())?)?
            .iter()
            .any(|v| matches!(
                v,
                Violation::FacadeItem { item, .. } if item == "non-public use"
            ))
    );
    Ok(())
}

#[test]
fn facade_rejects_inline_modules_and_exported_macros() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    setup(temp.path())?;
    assert!(check(&plan(CLEAN)?, &Workspace::load(temp.path())?)?.is_empty());
    fs::write(
        temp.path().join("a/src/lib.rs"),
        "pub mod inline {}\n#[macro_export] macro_rules! example { () => {} }",
    )?;
    let violations = check(&plan(CLEAN)?, &Workspace::load(temp.path())?)?;
    assert_eq!(
        violations
            .iter()
            .filter(|v| matches!(v, Violation::FacadeItem { .. }))
            .count(),
        2
    );
    Ok(())
}

#[test]
fn procedural_macro_functions_require_a_proc_macro_crate() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    setup(temp.path())?;
    let lib = "#[proc_macro] pub fn derive(input: TokenStream) -> TokenStream { input }";
    package(temp.path(), "a", "", lib)?;
    assert!(
        check(&plan(CLEAN)?, &Workspace::load(temp.path())?)?
            .iter()
            .any(|v| matches!(v, Violation::FacadeItem { .. }))
    );
    package(temp.path(), "a", "[lib]\nproc-macro = true\n", lib)?;
    assert!(check(&plan(CLEAN)?, &Workspace::load(temp.path())?)?.is_empty());
    Ok(())
}

#[test]
fn unsafe_lint_overrides_require_designated_file_and_reason() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let root = temp.path();
    let package_dir = root.join("crates/gang-os");
    fs::create_dir_all(package_dir.join("src"))?;
    fs::write(
        root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"crates/gang-os\"]\n",
    )?;
    fs::write(
        package_dir.join("Cargo.toml"),
        "[package]\nname = \"gang-os\"\nversion = \"0.1.0\"\n",
    )?;
    fs::write(package_dir.join("src/lib.rs"), "mod sys;\n")?;
    let plan = plan("| `gang-os` | `0_kernel` | 2 | os |\n")?;
    let file = package_dir.join("src/sys.rs");
    let expected = "#![expect(unsafe_code, reason = \"required by the os\")]\n";
    fs::write(&file, expected)?;
    assert!(check(&plan, &Workspace::load(root)?)?.is_empty());
    fs::write(package_dir.join("src/other.rs"), expected)?;
    let violations = check(&plan, &Workspace::load(root)?)?;
    assert!(violations.iter().any(|v| matches!(
        v,
        Violation::UnsafeConfinement {
            reason: "expect(unsafe_code) is outside the designated file",
            ..
        }
    )));
    fs::write(package_dir.join("src/other.rs"), "")?;
    fs::write(&file, "#[allow(unsafe_code)] mod confined {}\n")?;
    let violations = check(&plan, &Workspace::load(root)?)?;
    assert!(violations.iter().any(|v| matches!(
        v,
        Violation::UnsafeConfinement {
            reason: "allow(unsafe_code) is forbidden",
            ..
        }
    )));
    fs::write(&file, "#[expect(unsafe_code)] mod confined {}\n")?;
    let violations = check(&plan, &Workspace::load(root)?)?;
    assert!(violations.iter().any(|v| matches!(
        v,
        Violation::UnsafeConfinement {
            reason: "expect(unsafe_code) requires a reason",
            ..
        }
    )));
    fs::create_dir_all(package_dir.join("tests"))?;
    fs::write(&file, "")?;
    fs::write(
        package_dir.join("tests/unsafe.rs"),
        "#![allow(unsafe_code)]\n",
    )?;
    assert!(
        check(&plan, &Workspace::load(root)?)?
            .iter()
            .any(|v| matches!(
                v,
                Violation::UnsafeConfinement {
                    reason: "allow(unsafe_code) is forbidden",
                    ..
                }
            ))
    );
    Ok(())
}
