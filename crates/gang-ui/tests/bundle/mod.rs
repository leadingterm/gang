//! Checks that the embedded asset table matches the `//ui:dist` tree.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::support::*;
use gang_ui::lookup;

/// Environment variable naming the dist directory, set by the Bazel test rule.
const UI_DIST_ENV: &str = "UI_DIST";
/// Suffix of the Brotli variant beside an identity file.
const BR_SUFFIX: &str = ".br";
/// Suffix of the gzip variant beside an identity file.
const GZ_SUFFIX: &str = ".gz";
/// Length of a SHA-256 digest rendered as lowercase hex.
const SHA256_HEX_LEN: usize = 64;
/// Length of a strong entity tag: the hex digest plus two quotes.
const QUOTED_ETAG_LEN: usize = SHA256_HEX_LEN + 2;
/// Prefix shared by every hashed, immutable asset path.
const ASSETS_PREFIX: &str = "assets/";

#[test]
fn every_asset_the_index_references_is_embedded() {
    let references = referenced();
    for reference in &references {
        assert!(
            lookup(reference).is_some(),
            "index.html references {reference:?} but the table has no such asset"
        );
    }
    assert!(
        references.iter().any(|r| has_extension(r, "js")),
        "index.html must reference a script: {references:?}"
    );
    assert!(
        references.iter().any(|r| has_extension(r, "css")),
        "index.html must reference a stylesheet: {references:?}"
    );
}

fn has_extension(path: &str, extension: &str) -> bool {
    Path::new(path)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case(extension))
}

#[test]
fn lookup_finds_every_referenced_asset_and_misses_unknown() {
    for reference in referenced() {
        let asset =
            lookup(reference).unwrap_or_else(|| panic!("referenced asset {reference:?} not found"));
        assert_eq!(asset.path, reference, "lookup must return the exact path");
    }
    assert!(lookup("nope.js").is_none(), "unknown name must miss");
    assert!(lookup("").is_none(), "empty path must miss");
    assert!(lookup("/index.html").is_none(), "leading slash must miss");
}

#[test]
fn assets_are_sorted_hashed_and_immutable() {
    let script = script();
    let index = index();
    let font = font();

    assert!(script.immutable, "script must be immutable");
    assert!(
        script.path.starts_with(ASSETS_PREFIX),
        "script path {:?} must be under assets/",
        script.path
    );
    assert!(font.immutable, "font must be immutable");
    assert!(
        font.path.starts_with(ASSETS_PREFIX),
        "font path {:?} must be under assets/",
        font.path
    );
    assert!(!index.immutable, "index.html must not be immutable");

    for asset in [index, script, font] {
        assert_strong_etag(asset.path, asset.etag);
    }
}

#[test]
fn embedded_bundle_equals_the_ui_dist_tree() {
    let root = PathBuf::from(std::env::var(UI_DIST_ENV).expect(UI_DIST_ENV));
    let mut files = BTreeSet::new();
    walk(&root, &root, &mut files);

    let identities: Vec<&str> = files
        .iter()
        .filter(|p| strip_variant(p.as_str()).is_none())
        .map(String::as_str)
        .collect();
    assert!(
        identities.contains(&"index.html"),
        "dist must contain index.html: {identities:?}"
    );

    let mut looked_up: BTreeSet<&str> = BTreeSet::new();
    for path in &identities {
        let asset = lookup(path).unwrap_or_else(|| panic!("dist file {path:?} is not embedded"));
        let identity = fs::read(root.join(path))
            .unwrap_or_else(|e| panic!("cannot read dist file {path:?}: {e}"));
        assert!(
            asset.identity == identity.as_slice(),
            "identity bytes of {path:?} differ from the dist file"
        );
        assert_variant(&root, path, BR_SUFFIX, asset.br, &files);
        assert_variant(&root, path, GZ_SUFFIX, asset.gz, &files);
        looked_up.insert(*path);
    }

    for variant in files.iter().filter(|p| strip_variant(p.as_str()).is_some()) {
        let base = strip_variant(variant).expect("variant has a suffix");
        assert!(
            identities.contains(&base),
            "compressed variant {variant:?} has no identity file"
        );
        assert!(
            lookup(variant).is_none(),
            "compressed variant {variant:?} must not be an entry"
        );
    }

    assert_eq!(
        looked_up.len(),
        identities.len(),
        "every non-variant dist file must be looked up exactly once"
    );
    for reference in referenced() {
        assert!(
            looked_up.contains(&reference),
            "referenced asset {reference:?} is not a dist file"
        );
    }
}

/// Asserts a strong, lowercase-hex, quoted entity tag.
fn assert_strong_etag(path: &str, etag: &str) {
    assert_eq!(
        etag.len(),
        QUOTED_ETAG_LEN,
        "etag of {path:?} must be a quoted sha-256 hex digest: {etag:?}"
    );
    let inner = etag
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .unwrap_or_else(|| panic!("etag of {path:?} must be quoted: {etag:?}"));
    assert!(
        inner
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "etag of {path:?} must be lowercase hex: {etag:?}"
    );
}

/// Returns the identity path a `.br` or `.gz` variant belongs to.
fn strip_variant(path: &str) -> Option<&str> {
    path.strip_suffix(BR_SUFFIX)
        .or_else(|| path.strip_suffix(GZ_SUFFIX))
}

/// Asserts that the embedded variant matches the sibling file on disk, or that both are absent.
fn assert_variant(
    root: &Path,
    path: &str,
    suffix: &str,
    embedded: Option<&[u8]>,
    files: &BTreeSet<String>,
) {
    let variant = format!("{path}{suffix}");
    match (files.contains(&variant), embedded) {
        (true, Some(bytes)) => {
            let on_disk = fs::read(root.join(&variant))
                .unwrap_or_else(|e| panic!("cannot read dist file {variant:?}: {e}"));
            assert!(
                bytes == on_disk.as_slice(),
                "embedded {variant:?} differs from the dist file"
            );
        }
        (false, None) => {}
        (true, None) => panic!("{variant:?} exists in dist but is not embedded"),
        (false, Some(_)) => panic!("{variant:?} is embedded but not in dist"),
    }
}

/// Recursively collects every regular file under `dir` as a `/`-separated path relative to `root`.
fn walk(root: &Path, dir: &Path, out: &mut BTreeSet<String>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("cannot read {dir:?}: {e}"));
    for entry in entries {
        let path = entry
            .unwrap_or_else(|e| panic!("cannot read entry in {dir:?}: {e}"))
            .path();
        let meta = fs::metadata(&path).unwrap_or_else(|e| panic!("cannot stat {path:?}: {e}"));
        if meta.is_dir() {
            walk(root, &path, out);
        } else if meta.is_file() {
            out.insert(relative_slash(root, &path));
        }
    }
}

/// Renders `path` relative to `root` with `/` separators.
fn relative_slash(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .expect("walked path is under the dist root")
        .components()
        .map(|c| c.as_os_str().to_str().expect("dist paths are utf-8"))
        .collect::<Vec<_>>()
        .join("/")
}
