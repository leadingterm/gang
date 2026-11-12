use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    path::{Path, PathBuf},
};

use syn::visit::{self, Visit};

/// An error reading or parsing architecture inputs.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A workspace file could not be read.
    #[error("cannot read {path}: {source}")]
    Io {
        /// Path being read.
        path: PathBuf,
        /// Underlying I/O error.
        source: io::Error,
    },
    /// A manifest is not valid TOML.
    #[error("invalid manifest {path}: {source}")]
    Manifest {
        /// Manifest path.
        path: PathBuf,
        /// Underlying parsing error.
        source: toml::de::Error,
    },
    /// A manifest or plan is missing required information.
    #[error("invalid architecture input: {0}")]
    Input(String),
    /// A Rust file could not be parsed.
    #[error("invalid rust file {path}: {source}")]
    Rust {
        /// Source path.
        path: PathBuf,
        /// Underlying syntax error.
        source: syn::Error,
    },
}

fn read(path: &Path) -> Result<String, Error> {
    fs::read_to_string(path).map_err(|source| Error::Io {
        path: path.to_owned(),
        source,
    })
}

fn manifest(path: &Path) -> Result<toml::Table, Error> {
    toml::from_str(&read(path)?).map_err(|source| Error::Manifest {
        path: path.to_owned(),
        source,
    })
}

/// Crate tiers extracted from the architecture plan.
#[derive(Debug)]
pub struct Plan {
    tiers: BTreeMap<String, Option<u32>>,
}

impl Plan {
    /// Parses the crate table identified by its named header columns.
    ///
    /// # Errors
    /// Returns an error if the table is missing or contains malformed crate rows.
    pub fn parse(text: &str) -> Result<Self, Error> {
        let mut lines = text.lines();
        let header = lines
            .by_ref()
            .find(|line| {
                line.trim_start()
                    .starts_with("| Crate Name | Ring | Tier | Role |")
            })
            .ok_or_else(|| Error::Input("crate table header not found".into()))?;
        let columns: Vec<_> = header
            .trim()
            .trim_matches('|')
            .split('|')
            .map(str::trim)
            .collect();
        let column = |name: &str| {
            columns
                .iter()
                .position(|value| *value == name)
                .ok_or_else(|| Error::Input(format!("missing {name} column")))
        };
        let name_index = column("Crate Name")?;
        let ring_index = column("Ring")?;
        let tier_index = column("Tier")?;
        let separator = lines
            .next()
            .ok_or_else(|| Error::Input("missing table separator".into()))?;
        if !separator.trim_start().starts_with('|') {
            return Err(Error::Input("missing table separator".into()));
        }
        let mut tiers = BTreeMap::new();
        for line in lines.take_while(|line| line.trim_start().starts_with('|')) {
            let cells: Vec<_> = line
                .trim()
                .trim_matches('|')
                .split('|')
                .map(str::trim)
                .collect();
            let cell = |index: usize| {
                cells
                    .get(index)
                    .copied()
                    .ok_or_else(|| Error::Input(format!("malformed table row: {line}")))
            };
            let quoted = |value: &str| {
                value
                    .strip_prefix('`')
                    .and_then(|s| s.strip_suffix('`'))
                    .map(str::to_owned)
                    .ok_or_else(|| Error::Input(format!("expected backticks in table row: {line}")))
            };
            let name = quoted(cell(name_index)?)?;
            let _ring = quoted(cell(ring_index)?)?;
            let tier = cell(tier_index)?.trim_matches('`');
            let tier = if tier == "-" {
                None
            } else {
                Some(
                    tier.parse()
                        .map_err(|_| Error::Input(format!("invalid tier in table row: {line}")))?,
                )
            };
            if tiers.insert(name.clone(), tier).is_some() {
                return Err(Error::Input(format!("duplicate crate in plan: {name}")));
            }
        }
        Ok(Self { tiers })
    }
}

#[derive(Debug)]
struct Crate {
    path: PathBuf,
    proc_macro: bool,
    dependencies: BTreeSet<String>,
    files: BTreeMap<PathBuf, String>,
}

/// Workspace manifests and Rust sources loaded from disk.
#[derive(Debug)]
pub struct Workspace {
    crates: BTreeMap<String, Crate>,
}

fn collect_sources(
    root: &Path,
    dir: &Path,
    files: &mut BTreeMap<PathBuf, String>,
) -> Result<(), Error> {
    let entries = fs::read_dir(dir).map_err(|source| Error::Io {
        path: dir.to_owned(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| Error::Io {
            path: dir.to_owned(),
            source,
        })?;
        let path = entry.path();
        let kind = fs::metadata(&path).map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
        if kind.is_dir() {
            collect_sources(root, &path, files)?;
        } else if kind.is_file() && path.extension().is_some_and(|ext| ext == "rs") {
            let relative = path.strip_prefix(root).map_err(|_| {
                Error::Input(format!("source outside workspace: {}", path.display()))
            })?;
            files.insert(relative.to_owned(), read(&path)?);
        }
    }
    Ok(())
}

impl Workspace {
    /// Loads all workspace member manifests and their Rust source files.
    ///
    /// # Errors
    /// Returns an error on invalid manifests or inaccessible files.
    pub fn load(root: &Path) -> Result<Self, Error> {
        let root_manifest = manifest(&root.join("Cargo.toml"))?;
        let workspace = root_manifest
            .get("workspace")
            .ok_or_else(|| Error::Input("missing workspace".into()))?;
        let members = workspace
            .get("members")
            .and_then(toml::Value::as_array)
            .ok_or_else(|| Error::Input("missing workspace members".into()))?;
        let workspace_deps = workspace
            .get("dependencies")
            .and_then(toml::Value::as_table);
        let mut manifests = Vec::new();
        for member in members {
            let path = member
                .as_str()
                .ok_or_else(|| Error::Input("invalid workspace member".into()))?;
            let parsed = manifest(&root.join(path).join("Cargo.toml"))?;
            let name = parsed
                .get("package")
                .and_then(|p| p.get("name"))
                .and_then(toml::Value::as_str)
                .ok_or_else(|| Error::Input(format!("missing package name in {path}")))?
                .to_owned();
            manifests.push((PathBuf::from(path), name, parsed));
        }
        let names: BTreeSet<_> = manifests.iter().map(|(_, name, _)| name.clone()).collect();
        let mut crates = BTreeMap::new();
        for (path, name, parsed) in manifests {
            let mut dependencies = BTreeSet::new();
            for section in ["dependencies", "build-dependencies"] {
                if let Some(table) = parsed.get(section).and_then(toml::Value::as_table) {
                    for (key, value) in table {
                        let inherited =
                            value.get("workspace").and_then(toml::Value::as_bool) == Some(true);
                        let package = value
                            .get("package")
                            .and_then(toml::Value::as_str)
                            .or_else(|| {
                                if inherited {
                                    workspace_deps
                                        .and_then(|deps| deps.get(key))
                                        .and_then(|dep| dep.get("package"))
                                        .and_then(toml::Value::as_str)
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(key);
                        if names.contains(package) {
                            dependencies.insert(package.to_owned());
                        }
                    }
                }
            }
            let proc_macro = parsed
                .get("lib")
                .and_then(|lib| lib.get("proc-macro"))
                .and_then(toml::Value::as_bool)
                .unwrap_or(false);
            let mut files = BTreeMap::new();
            collect_sources(root, &root.join(&path).join("src"), &mut files)?;
            let tests = root.join(&path).join("tests");
            match fs::metadata(&tests) {
                Ok(_) => collect_sources(root, &tests, &mut files)?,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(Error::Io {
                        path: tests,
                        source,
                    });
                }
            }
            if crates
                .insert(
                    name.clone(),
                    Crate {
                        path,
                        proc_macro,
                        dependencies,
                        files,
                    },
                )
                .is_some()
            {
                return Err(Error::Input(format!("duplicate workspace package: {name}")));
            }
        }
        Ok(Self { crates })
    }
}

/// A violation of the workspace architecture rules.
#[derive(Debug, PartialEq, Eq)]
pub enum Violation {
    /// A workspace package is absent from the plan.
    MissingMember {
        /// Package name.
        name: String,
    },
    /// A plan entry has no corresponding workspace package.
    ExtraPlanCrate {
        /// Package name.
        name: String,
    },
    /// A normal or build dependency does not point strictly downward.
    TierEdge {
        /// Dependent package.
        from: String,
        /// Dependency package.
        to: String,
        /// Dependent tier.
        from_tier: u32,
        /// Dependency tier, or none for untiered.
        to_tier: Option<u32>,
    },
    /// A Rust source is nested under the src directory.
    NestedSource {
        /// Package name.
        name: String,
        /// Workspace-relative file path.
        file: PathBuf,
    },
    /// A module uses a path override.
    PathAttribute {
        /// Package name.
        name: String,
        /// Workspace-relative file path.
        file: PathBuf,
        /// Module name.
        module: String,
    },
    /// An unsafe-code lint override violates confinement rules.
    UnsafeConfinement {
        /// Package name.
        name: String,
        /// Workspace-relative file path.
        file: PathBuf,
        /// Explanation of the invalid override.
        reason: &'static str,
    },
    /// A library root contains an item outside the facade vocabulary.
    FacadeItem {
        /// Package name.
        name: String,
        /// Workspace-relative file path.
        file: PathBuf,
        /// Description of the disallowed item.
        item: String,
    },
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingMember { name } => {
                write!(f, "workspace package {name} is missing from plan")
            }
            Self::ExtraPlanCrate { name } => {
                write!(f, "plan crate {name} is not a workspace package")
            }
            Self::TierEdge {
                from,
                to,
                from_tier,
                to_tier,
            } => write!(
                f,
                "invalid dependency {from} (tier {from_tier}) -> {to} (tier {})",
                to_tier.map_or_else(|| "untiered".to_owned(), |tier| tier.to_string())
            ),
            Self::NestedSource { name, file } => {
                write!(f, "{name}: nested source {}", file.display())
            }
            Self::PathAttribute { name, file, module } => write!(
                f,
                "{name}: {}: module {module} has a path attribute",
                file.display()
            ),
            Self::UnsafeConfinement { name, file, reason } => {
                write!(f, "{name}: {}: {reason}", file.display())
            }
            Self::FacadeItem { name, file, item } => write!(
                f,
                "{name}: {}: disallowed facade item {item}",
                file.display()
            ),
        }
    }
}

// Adding another unavoidable unsafe site requires an explicit reviewed change.
const UNSAFE_CONFINEMENT_FILE: &str = "crates/gang-os/src/sys.rs";

struct PathVisitor<'a> {
    name: &'a str,
    file: &'a Path,
    violations: &'a mut Vec<Violation>,
}

impl<'ast> Visit<'ast> for PathVisitor<'_> {
    fn visit_attribute(&mut self, node: &'ast syn::Attribute) {
        let is_allow = node.path().is_ident("allow");
        let is_expect = node.path().is_ident("expect");
        if (is_allow || is_expect)
            && let Ok(arguments) = node.parse_args_with(
                syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
            )
            && arguments
                .iter()
                .any(|meta| meta.path().is_ident("unsafe_code"))
        {
            let has_reason = arguments.iter().any(|meta| {
                matches!(
                    meta,
                    syn::Meta::NameValue(value)
                        if value.path.is_ident("reason")
                            && matches!(&value.value, syn::Expr::Lit(lit) if matches!(&lit.lit, syn::Lit::Str(_)))
                )
            });
            let confined = self.file == Path::new(UNSAFE_CONFINEMENT_FILE);
            let mut report = |reason| {
                self.violations.push(Violation::UnsafeConfinement {
                    name: self.name.to_owned(),
                    file: self.file.to_owned(),
                    reason,
                });
            };
            if is_expect && !confined {
                report("expect(unsafe_code) is outside the designated file");
            }
            if is_allow {
                report("allow(unsafe_code) is forbidden");
            }
            if is_expect && !has_reason {
                report("expect(unsafe_code) requires a reason");
            }
        }
        visit::visit_attribute(self, node);
    }

    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        for attr in &node.attrs {
            if attr.path().is_ident("path") {
                self.violations.push(Violation::PathAttribute {
                    name: self.name.to_owned(),
                    file: self.file.to_owned(),
                    module: node.ident.to_string(),
                });
            }
        }
        visit::visit_item_mod(self, node);
    }
}

fn facade_item(item: &syn::Item, proc_macro: bool) -> Option<String> {
    match item {
        syn::Item::Mod(module) if module.content.is_none() => None,
        syn::Item::Use(item) if !matches!(item.vis, syn::Visibility::Inherited) => None,
        syn::Item::ExternCrate(_) => None,
        syn::Item::Fn(item)
            if proc_macro
                && item.attrs.iter().any(|attr| {
                    ["proc_macro", "proc_macro_derive", "proc_macro_attribute"]
                        .iter()
                        .any(|name| attr.path().is_ident(name))
                }) =>
        {
            None
        }
        syn::Item::Mod(item) => Some(format!("inline module {}", item.ident)),
        syn::Item::Fn(item) => Some(format!("function {}", item.sig.ident)),
        syn::Item::Macro(item) => Some(format!(
            "macro {}",
            item.ident
                .as_ref()
                .map_or("<anonymous>".to_owned(), ToString::to_string)
        )),
        syn::Item::Struct(item) => Some(format!("struct {}", item.ident)),
        syn::Item::Enum(item) => Some(format!("enum {}", item.ident)),
        syn::Item::Union(item) => Some(format!("union {}", item.ident)),
        syn::Item::Trait(item) => Some(format!("trait {}", item.ident)),
        syn::Item::TraitAlias(item) => Some(format!("trait alias {}", item.ident)),
        syn::Item::Type(item) => Some(format!("type {}", item.ident)),
        syn::Item::Const(item) => Some(format!("const {}", item.ident)),
        syn::Item::Static(item) => Some(format!("static {}", item.ident)),
        syn::Item::Use(_) => Some("non-public use".to_owned()),
        syn::Item::Impl(_) => Some("impl".to_owned()),
        syn::Item::ForeignMod(_) => Some("extern block".to_owned()),
        _ => Some("non-facade item".to_owned()),
    }
}

/// Checks membership, tiers, source layout, facades and unsafe-code confinement.
///
/// # Errors
/// Returns an error when any Rust source cannot be parsed.
pub fn check(plan: &Plan, workspace: &Workspace) -> Result<Vec<Violation>, Error> {
    let mut violations = Vec::new();
    for name in workspace.crates.keys() {
        if !plan.tiers.contains_key(name) {
            violations.push(Violation::MissingMember { name: name.clone() });
        }
    }
    for name in plan.tiers.keys() {
        if !workspace.crates.contains_key(name) {
            violations.push(Violation::ExtraPlanCrate { name: name.clone() });
        }
    }
    for (name, krate) in &workspace.crates {
        if let Some(Some(from_tier)) = plan.tiers.get(name) {
            for dependency in &krate.dependencies {
                if let Some(to_tier) = plan.tiers.get(dependency)
                    && to_tier.is_none_or(|tier| tier >= *from_tier)
                {
                    violations.push(Violation::TierEdge {
                        from: name.clone(),
                        to: dependency.clone(),
                        from_tier: *from_tier,
                        to_tier: *to_tier,
                    });
                }
            }
        }
        for (file, text) in &krate.files {
            let src = krate.path.join("src");
            if let Ok(relative) = file.strip_prefix(&src)
                && relative.components().count() > 1
            {
                violations.push(Violation::NestedSource {
                    name: name.clone(),
                    file: file.clone(),
                });
            }
            let syntax = syn::parse_file(text).map_err(|source| Error::Rust {
                path: file.clone(),
                source,
            })?;
            PathVisitor {
                name,
                file,
                violations: &mut violations,
            }
            .visit_file(&syntax);
            if file == &src.join("lib.rs") {
                for item in &syntax.items {
                    if let Some(item) = facade_item(item, krate.proc_macro) {
                        violations.push(Violation::FacadeItem {
                            name: name.clone(),
                            file: file.clone(),
                            item,
                        });
                    }
                }
            }
        }
    }
    Ok(violations)
}
