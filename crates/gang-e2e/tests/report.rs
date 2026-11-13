//! Behaviour of the probe report parser: keys, values, blank lines, and malformed text.

use gang_e2e::{
    DiskNeighbourReport, E2eError, InputRootReport, IsolationReport, Namespaces, PoisonReport,
    ProbeReport, VictimReport,
};

fn parse_error_detail(error: &E2eError) -> Option<String> {
    match error {
        E2eError::Parse {
            subject: "probe report",
            source,
        } => Some(source.to_string()),
        _ => None,
    }
}

#[test]
fn parses_keys_and_values() {
    let report: ProbeReport = "net=net:[1]\nmnt=mnt:[2]\n".parse().expect("valid report");

    assert_eq!(report.get("net").expect("net"), "net:[1]");
    assert_eq!(report.get("mnt").expect("mnt"), "mnt:[2]");
}

#[test]
fn splits_at_the_first_equals_sign() {
    let report: ProbeReport = "url=http://h/?a=b=c\n".parse().expect("valid report");

    assert_eq!(report.get("url").expect("url"), "http://h/?a=b=c");
}

#[test]
fn accepts_an_empty_value() {
    let report: ProbeReport = "cwd=\n".parse().expect("valid report");

    assert_eq!(report.get("cwd").expect("cwd"), "");
}

#[test]
fn ignores_blank_lines() {
    let report: ProbeReport = "\n\na=1\n   \n\t\nb=2\n\n".parse().expect("valid report");

    assert_eq!(report.get("a").expect("a"), "1");
    assert_eq!(report.get("b").expect("b"), "2");
}

#[test]
fn rejects_a_repeated_key() {
    let error = "a=1\nb=2\na=3\n"
        .parse::<ProbeReport>()
        .expect_err("a repeated key is malformed");

    let detail = parse_error_detail(&error).expect("a parse error on the probe report");
    assert!(detail.contains("`a`"), "detail names the key: {detail}");
}

#[test]
fn rejects_a_line_without_equals() {
    let error = "a=1\nnot a pair\n"
        .parse::<ProbeReport>()
        .expect_err("a line without `=` is malformed");

    let detail = parse_error_detail(&error).expect("a parse error on the probe report");
    assert!(
        detail.contains("not a pair"),
        "detail quotes the line: {detail}"
    );
}

#[test]
fn reports_a_missing_key() {
    let report: ProbeReport = "a=1\n".parse().expect("valid report");

    let error = report.get("absent").expect_err("the key is missing");

    let detail = parse_error_detail(&error).expect("a parse error on the probe report");
    assert_eq!(detail, "missing key `absent`");
}

#[test]
fn reads_a_report_from_a_file() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("probe.txt");
    std::fs::write(
        &path,
        "cwd_name=action\nparent_entries=action,\ncwd_writable=yes\n",
    )
    .expect("write the report");

    let report = ProbeReport::from_file(&path).expect("the file parses");

    assert_eq!(report.get("cwd_name").expect("cwd_name"), "action");
    assert_eq!(
        report.get("parent_entries").expect("parent_entries"),
        "action,"
    );
    assert_eq!(report.get("cwd_writable").expect("cwd_writable"), "yes");
}

#[test]
fn a_missing_file_is_an_io_error() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("never-written.txt");

    let error = ProbeReport::from_file(&path).expect_err("the file does not exist");

    assert!(
        matches!(error, E2eError::Io { .. }),
        "got {}",
        gang_types::error_chain(&error)
    );
}

/// What the isolation probe prints when it runs in the action directory `/work/a/b/c`.
const ISOLATION_TEXT: &str = "net=net:[3]\nmnt=mnt:[4]\ncwd=/work/a/b/c\ncwd_name=c\nparent_name=b\nparent_entries=c,\ngrandparent_name=a\ngrandparent_entries=b,\ngreat_grandparent_entries=a,cas,\ncwd_writable=yes\n";

fn isolation() -> IsolationReport {
    IsolationReport {
        namespaces: Namespaces {
            net: "net:[3]".to_owned(),
            mnt: "mnt:[4]".to_owned(),
        },
        cwd: "/work/a/b/c".to_owned(),
        cwd_name: "c".to_owned(),
        parent_name: "b".to_owned(),
        parent_entries: vec!["c".to_owned()],
        grandparent_name: "a".to_owned(),
        grandparent_entries: vec!["b".to_owned()],
        great_grandparent_entries: vec!["a".to_owned(), "cas".to_owned()],
        cwd_writable: true,
    }
}

fn parsed(text: &str) -> gang_e2e::Result<ProbeReport> {
    text.parse()
}

#[test]
fn the_isolation_report_reads_what_the_probe_prints() -> gang_e2e::Result<()> {
    let report = IsolationReport::try_from(&parsed(ISOLATION_TEXT)?).expect("complete report");

    assert_eq!(report, isolation());
    Ok(())
}

#[test]
fn the_isolation_report_prints_what_the_probe_prints() {
    assert_eq!(isolation().to_string(), ISOLATION_TEXT);
}

#[test]
fn an_empty_listing_has_no_entries() -> gang_e2e::Result<()> {
    let text = ISOLATION_TEXT.replace("parent_entries=c,", "parent_entries=");

    let report = IsolationReport::try_from(&parsed(&text)?).expect("complete report");

    assert!(report.parent_entries.is_empty());
    Ok(())
}

#[test]
fn the_isolation_report_names_the_missing_key() -> gang_e2e::Result<()> {
    for line in ISOLATION_TEXT.lines() {
        let key = line.split_once('=').expect("a key=value line").0;
        let text = ISOLATION_TEXT.replace(&format!("{line}\n"), "");

        let error = IsolationReport::try_from(&parsed(&text)?).expect_err("a key is missing");

        let detail = parse_error_detail(&error).expect("a parse error on the probe report");
        assert_eq!(detail, format!("missing key `{key}`"));
    }
    Ok(())
}

#[test]
fn the_isolation_report_rejects_a_writable_flag_that_is_not_yes_or_no() -> gang_e2e::Result<()> {
    let text = ISOLATION_TEXT.replace("cwd_writable=yes", "cwd_writable=maybe");

    let error = IsolationReport::try_from(&parsed(&text)?).expect_err("the flag is invalid");

    let detail = parse_error_detail(&error).expect("a parse error on the probe report");
    assert!(detail.contains("`cwd_writable`"), "{detail}");
    assert!(detail.contains("`maybe`"), "{detail}");
    Ok(())
}

#[test]
fn namespaces_round_trip() -> gang_e2e::Result<()> {
    let namespaces = Namespaces {
        net: "net:[1]".to_owned(),
        mnt: "unavailable".to_owned(),
    };

    let read = Namespaces::try_from(&parsed(&namespaces.to_string())?).expect("complete report");

    assert_eq!(namespaces.to_string(), "net=net:[1]\nmnt=unavailable\n");
    assert_eq!(read, namespaces);
    Ok(())
}

#[test]
fn namespaces_name_the_missing_key() -> gang_e2e::Result<()> {
    let error = Namespaces::try_from(&parsed("net=net:[1]\n")?).expect_err("mnt is missing");

    let detail = parse_error_detail(&error).expect("a parse error on the probe report");
    assert_eq!(detail, "missing key `mnt`");
    Ok(())
}

#[test]
fn the_poison_report_reads_both_attempts() -> gang_e2e::Result<()> {
    for (text, modified) in [("attempt=modified\n", true), ("attempt=refused\n", false)] {
        let report = PoisonReport::try_from(&parsed(text)?).expect("a valid attempt");

        assert_eq!(report, PoisonReport { modified });
        assert_eq!(report.to_string(), text);
    }
    Ok(())
}

#[test]
fn the_poison_report_rejects_another_attempt() -> gang_e2e::Result<()> {
    let error = PoisonReport::try_from(&parsed("attempt=skipped\n")?).expect_err("not an attempt");

    let detail = parse_error_detail(&error).expect("a parse error on the probe report");
    assert!(detail.contains("`attempt`"), "{detail}");
    Ok(())
}

#[test]
fn the_victim_report_reads_both_answers() -> gang_e2e::Result<()> {
    for (text, poisoned) in [("poisoned=yes\n", true), ("poisoned=no\n", false)] {
        let report = VictimReport::try_from(&parsed(text)?).expect("a valid answer");

        assert_eq!(report, VictimReport { poisoned });
        assert_eq!(report.to_string(), text);
    }
    Ok(())
}

#[test]
fn the_victim_report_names_the_missing_key() {
    let error = VictimReport::try_from(&ProbeReport::default()).expect_err("poisoned is missing");

    let detail = parse_error_detail(&error).expect("a parse error on the probe report");
    assert_eq!(detail, "missing key `poisoned`");
}

#[test]
fn input_root_report_round_trips_link_counts() -> gang_e2e::Result<()> {
    for sibling_links in [Some(1), None] {
        let report = InputRootReport {
            shared_path: "../shared/input".to_owned(),
            sibling: "shared input".to_owned(),
            symlink: "shared input".to_owned(),
            sibling_links,
        };
        let text = report.to_string();
        if sibling_links.is_none() {
            assert!(text.contains("sibling_links=unreadable\n"));
        }
        assert_eq!(InputRootReport::try_from(&parsed(&text)?)?, report);
    }
    Ok(())
}

#[test]
fn input_root_report_rejects_an_invalid_link_count() -> gang_e2e::Result<()> {
    let report = parsed("shared_path=../input\nsibling=x\nsymlink=x\nsibling_links=invalid\n")?;
    let error = InputRootReport::try_from(&report).expect_err("invalid link count");
    let detail = parse_error_detail(&error).expect("a parse error on the probe report");
    assert!(detail.contains("`sibling_links`"), "{detail}");
    assert!(detail.contains("`invalid`"), "{detail}");
    Ok(())
}

#[test]
fn the_disk_neighbour_report_reads_both_states() -> gang_e2e::Result<()> {
    for (text, intact) in [("neighbour=ok\n", true), ("neighbour=bad\n", false)] {
        let report = DiskNeighbourReport::try_from(&parsed(text)?).expect("a valid state");

        assert_eq!(report, DiskNeighbourReport { intact });
        assert_eq!(report.to_string(), text);
    }
    Ok(())
}
