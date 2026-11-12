//! Child tests: a test of the running test binary that another test re-executes in a child process.
//!
//! libtest exits successfully when its filter matches no test, so a parent that checks only the
//! child's exit status keeps passing once the child test is renamed or moved. A [`ChildTest`] names
//! the child once for both sides: the child calls [`ChildTest::complete`] after its last assertion,
//! and the parent requires that line with [`ChildTest::assert_completed`] or
//! [`ChildTest::completed_in`]. A child the parent kills never completes, and its parent checks
//! what the child did instead.

use std::process::{Command, Output};

/// A test of the running test binary that a parent test re-executes in a child process.
#[derive(Debug, Clone, Copy)]
pub struct ChildTest(&'static str);

impl ChildTest {
    /// Names the child test by its path from the test binary's crate root, as `--list` prints it.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// The libtest arguments that run exactly this test, ignored or not, with its output shown.
    #[must_use]
    pub const fn arguments(self) -> [&'static str; 4] {
        ["--include-ignored", "--exact", self.0, "--nocapture"]
    }

    /// A command that runs this test in a child process of the running test binary.
    ///
    /// # Errors
    ///
    /// When the path of the running executable is unknown.
    pub fn command(self) -> std::io::Result<Command> {
        let mut command = Command::new(std::env::current_exe()?);
        command.args(self.arguments());
        Ok(command)
    }

    /// Prints the completion line on stdout. The child calls it after its last assertion.
    pub fn complete(self) {
        println!("{}", self.marker());
    }

    /// Whether `stdout` holds the completion line of this test.
    #[must_use]
    pub fn completed_in(self, stdout: &str) -> bool {
        stdout.contains(&self.marker())
    }

    /// Asserts that the child exited successfully after printing the completion line.
    ///
    /// # Panics
    ///
    /// When the child failed, or exited without completing: it matched no test.
    pub fn assert_completed(self, output: &Output) {
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && self.completed_in(&stdout),
            "child test {} did not complete ({})\nstdout:\n{stdout}\nstderr:\n{}",
            self.0,
            output.status,
            String::from_utf8_lossy(&output.stderr),
        );
    }

    /// The completion line. libtest may print its own `test <name> ... ` before it on the same
    /// line, so it is delimited rather than a whole line.
    fn marker(self) -> String {
        format!("<child test {} completed>", self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::process::Output;

    use super::ChildTest;

    const COMPLETING: ChildTest = ChildTest::new("child_test::tests::completing_child");
    const FAILING: ChildTest = ChildTest::new("child_test::tests::failing_child");
    const MISSING: ChildTest = ChildTest::new("child_test::tests::no_such_child");

    #[test]
    #[ignore = "child test: run by a_child_that_completes_is_accepted"]
    fn completing_child() {
        COMPLETING.complete();
    }

    #[test]
    #[ignore = "child test: run by a_child_that_fails_its_assertions_does_not_complete"]
    fn failing_child() {
        let fails = std::hint::black_box(true);
        assert!(!fails, "the child's assertion fails");
        FAILING.complete();
    }

    fn run(child: ChildTest) -> Output {
        child
            .command()
            .and_then(|mut command| command.output())
            .expect("the child test runs")
    }

    fn stdout(output: &Output) -> String {
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    #[test]
    fn a_child_that_completes_is_accepted() {
        COMPLETING.assert_completed(&run(COMPLETING));
    }

    #[test]
    fn a_child_that_matches_no_test_exits_successfully_without_completing() {
        let output = run(MISSING);
        assert!(output.status.success());
        assert!(!MISSING.completed_in(&stdout(&output)));
    }

    #[test]
    fn a_child_that_fails_its_assertions_does_not_complete() {
        let output = run(FAILING);
        assert!(!output.status.success());
        assert!(!FAILING.completed_in(&stdout(&output)));
    }

    #[test]
    fn a_completion_counts_only_for_its_own_child() {
        let output = run(COMPLETING);
        assert!(!MISSING.completed_in(&stdout(&output)));
    }

    #[test]
    #[should_panic(expected = "did not complete")]
    fn assert_completed_rejects_a_child_that_matched_no_test() {
        MISSING.assert_completed(&run(MISSING));
    }
}
