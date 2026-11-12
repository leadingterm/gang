//! The result type that test functions return, so they can propagate errors with `?`.

/// A test or fixture result. An `Err` fails the test and prints the error.
pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
