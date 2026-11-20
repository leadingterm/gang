//! Evidence read from Bazel's build event stream and remote gRPC log.

use std::collections::HashSet;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

use prost::Message;
use serde_json::Value;

use crate::errors::{E2eError, Result};

/// Observed actions and transport calls. RPC failures and retries are diagnostics, not verdicts.
///
/// A retry is a call with the same method and request as an earlier failed call, until a
/// matching call succeeds. Successful repetitions alone are not retries. Write identity uses
/// nonempty resource names, offsets and finish flags recorded in the log. `Execute` and
/// `WaitExecution` use separate method keys; operation correlation does not affect retry counts.
#[derive(Debug, Default)]
pub(crate) struct BuildEvidence {
    pub(crate) successful_actions: usize,
    pub(crate) failed_actions: usize,
    pub(crate) finished_success: bool,
    pub(crate) rpc_calls: usize,
    pub(crate) rpc_failures: usize,
    pub(crate) rpc_retry_attempts: usize,
    /// `WaitExecution` calls whose operation was previously named in an `Execute` response.
    pub(crate) execute_wait_correlations: usize,
}

// These projections use Bazel 8.7's remote execution log wire numbers. Unknown fields are skipped.
#[derive(Clone, PartialEq, Message)]
struct LogEntry {
    #[prost(message, optional, tag = "2")]
    status: Option<Status>,
    #[prost(string, tag = "3")]
    method_name: String,
    #[prost(message, optional, tag = "4")]
    details: Option<Details>,
}

#[derive(Clone, PartialEq, Message)]
struct Status {
    #[prost(int32, tag = "1")]
    code: i32,
}

#[derive(Clone, PartialEq, Message)]
struct Details {
    #[prost(
        oneof = "details::Call",
        tags = "5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16"
    )]
    call: Option<details::Call>,
}

mod details {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub(super) enum Call {
        #[prost(bytes, tag = "5")]
        Read(Vec<u8>),
        #[prost(bytes, tag = "6")]
        Write(Vec<u8>),
        #[prost(bytes, tag = "7")]
        Execute(Vec<u8>),
        #[prost(bytes, tag = "8")]
        GetActionResult(Vec<u8>),
        #[prost(bytes, tag = "9")]
        WaitExecution(Vec<u8>),
        #[prost(bytes, tag = "10")]
        FindMissingBlobs(Vec<u8>),
        #[prost(bytes, tag = "12")]
        GetCapabilities(Vec<u8>),
        #[prost(bytes, tag = "13")]
        UpdateActionResult(Vec<u8>),
        #[prost(bytes, tag = "14")]
        QueryWriteStatus(Vec<u8>),
        #[prost(bytes, tag = "15")]
        Split(Vec<u8>),
        #[prost(bytes, tag = "16")]
        Splice(Vec<u8>),
    }
}

#[derive(Clone, PartialEq, Message)]
struct CallDetail {
    #[prost(bytes, optional, tag = "1")]
    request: Option<Vec<u8>>,
    #[prost(message, repeated, tag = "2")]
    responses: Vec<Operation>,
}

#[derive(Clone, PartialEq, Message)]
struct RequestDetail {
    #[prost(bytes, optional, tag = "1")]
    request: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct WriteDetail {
    #[prost(string, repeated, tag = "1")]
    resource_names: Vec<String>,
    #[prost(int64, repeated, tag = "5")]
    offsets: Vec<i64>,
    #[prost(int64, repeated, tag = "6")]
    finish_writes: Vec<i64>,
}

#[derive(Clone, PartialEq, Message)]
struct Operation {
    #[prost(string, tag = "1")]
    name: String,
}

impl BuildEvidence {
    /// Reads complete JSON-lines BEP and varint-length-delimited Bazel gRPC log records.
    pub(crate) fn read(bep: &Path, rpc: &Path) -> Result<Self> {
        let mut evidence = Self::default();
        evidence.read_bep(bep)?;
        evidence.read_rpc(rpc)?;
        Ok(evidence)
    }

    fn read_bep(&mut self, path: &Path) -> Result<()> {
        let file =
            fs::File::open(path).map_err(E2eError::io(format!("opening {}", path.display())))?;
        let mut finished = false;
        for line in BufReader::new(file).lines() {
            let line = line.map_err(E2eError::io("reading build events"))?;
            let event: Value = serde_json::from_str(&line)
                .map_err(E2eError::system_from("decoding build event"))?;
            if event.pointer("/id/actionCompleted").is_some() {
                let action = event
                    .get("action")
                    .and_then(Value::as_object)
                    .ok_or_else(|| E2eError::system("action completion has no action payload"))?;
                match action.get("success") {
                    Some(Value::Bool(true)) => self.successful_actions += 1,
                    Some(Value::Bool(false)) | None => self.failed_actions += 1,
                    _ => return Err(E2eError::system("invalid action success")),
                }
            }
            if event.pointer("/id/buildFinished").is_some() {
                let result = event
                    .pointer("/finished/exitCode")
                    .and_then(Value::as_object)
                    .ok_or_else(|| E2eError::system("build finished without an exit code"))?;
                let code = match result.get("code") {
                    None => 0,
                    Some(value) => value
                        .as_i64()
                        .ok_or_else(|| E2eError::system("invalid build exit code"))?,
                };
                let name = result
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| E2eError::system("missing build exit name"))?;
                self.finished_success = code == 0 && name == "SUCCESS";
                finished = true;
            }
        }
        if !finished {
            return Err(E2eError::system("build events lack a finished build"));
        }
        Ok(())
    }

    fn read_rpc(&mut self, path: &Path) -> Result<()> {
        let bytes = fs::read(path).map_err(E2eError::io(format!("reading {}", path.display())))?;
        let mut remaining = bytes.as_slice();
        let mut failed = HashSet::<(String, Vec<u8>)>::new();
        let mut operations = HashSet::<String>::new();
        while !remaining.is_empty() {
            let entry = LogEntry::decode_length_delimited(&mut remaining)
                .map_err(E2eError::system_from("decoding Bazel gRPC log"))?;
            self.rpc_calls += 1;
            let failure = entry.status.as_ref().is_some_and(|status| status.code != 0);
            self.rpc_failures += usize::from(failure);
            let Some(call) = entry.details.and_then(|details| details.call) else {
                continue;
            };
            let (raw, streaming, wait, write) = match call {
                details::Call::Execute(raw) => (raw, true, false, false),
                details::Call::WaitExecution(raw) => (raw, true, true, false),
                details::Call::Write(raw) => (raw, false, false, true),
                details::Call::Read(raw)
                | details::Call::GetActionResult(raw)
                | details::Call::FindMissingBlobs(raw)
                | details::Call::GetCapabilities(raw)
                | details::Call::UpdateActionResult(raw)
                | details::Call::QueryWriteStatus(raw)
                | details::Call::Split(raw)
                | details::Call::Splice(raw) => (raw, false, false, false),
            };
            let (request, responses) = if streaming {
                let detail = CallDetail::decode(raw.as_slice())
                    .map_err(E2eError::system_from("decoding streaming gRPC detail"))?;
                (detail.request, detail.responses)
            } else if write {
                let mut detail = WriteDetail::decode(raw.as_slice())
                    .map_err(E2eError::system_from("decoding write gRPC detail"))?;
                detail.resource_names.retain(|name| !name.is_empty());
                let request = if detail.resource_names.is_empty() {
                    None
                } else {
                    Some(detail.encode_to_vec())
                };
                (request, Vec::new())
            } else {
                let detail = RequestDetail::decode(raw.as_slice())
                    .map_err(E2eError::system_from("decoding gRPC detail"))?;
                (detail.request, Vec::new())
            };
            let Some(request) = request else { continue };
            // WaitExecution is its own method: following an Execute is normal polling. Its
            // operation name is associated with the original Execute request when observed.
            if streaming && !wait {
                for operation in responses {
                    if !operation.name.is_empty() {
                        operations.insert(operation.name);
                    }
                }
            } else if wait {
                let operation = Operation::decode(request.as_slice())
                    .map_err(E2eError::system_from("decoding WaitExecution request"))?;
                if operations.contains(&operation.name) {
                    self.execute_wait_correlations += 1;
                }
            }
            let key = (entry.method_name, request);
            if failed.contains(&key) {
                self.rpc_retry_attempts += 1;
            }
            if failure {
                failed.insert(key);
            } else {
                failed.remove(&key);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ACTION: &str = r#"{"id":{"actionCompleted":{}},"action":{"success":true}}"#;
    const FINISHED: &str =
        r#"{"id":{"buildFinished":{}},"finished":{"exitCode":{"name":"SUCCESS"}}}"#;

    fn read(bep: &str, rpc: &[u8]) -> Result<BuildEvidence> {
        let dir = tempfile::tempdir().expect("temporary directory");
        let bep_path = dir.path().join("bep");
        let rpc_path = dir.path().join("rpc");
        fs::write(&bep_path, bep).expect("write BEP");
        fs::write(&rpc_path, rpc).expect("write RPC log");
        BuildEvidence::read(&bep_path, &rpc_path)
    }

    fn record(method: &str, status: i32, call: details::Call, output: &mut Vec<u8>) {
        LogEntry {
            status: Some(Status { code: status }),
            method_name: method.to_owned(),
            details: Some(Details { call: Some(call) }),
        }
        .encode_length_delimited(output)
        .expect("encode record");
    }

    #[test]
    fn rejects_missing_empty_and_malformed_bep() {
        assert!(read("", &[]).is_err());
        assert!(read(ACTION, &[]).is_err());
        assert!(read("{", &[]).is_err());
        let dir = tempfile::tempdir().expect("temporary directory");
        assert!(BuildEvidence::read(&dir.path().join("missing"), &dir.path().join("rpc")).is_err());
    }

    #[test]
    fn failed_actions_and_failed_finished_result_are_observable() {
        let failed = r#"{"id":{"actionCompleted":{}},"action":{"success":false}}"#;
        let evidence = read(&format!("{ACTION}\n{failed}\n{FINISHED}\n"), &[]).expect("evidence");
        assert_eq!(
            (evidence.successful_actions, evidence.failed_actions),
            (1, 1)
        );
        assert!(evidence.finished_success);
        let failed_finish = r#"{"id":{"buildFinished":{}},"finished":{"exitCode":{"name":"BUILD_FAILURE","code":1}}}"#;
        let evidence =
            read(&format!("{failed}\n{failed_finish}\n"), &[]).expect("failed build evidence");
        assert!(!evidence.finished_success);
        assert_eq!(evidence.failed_actions, 1);
    }

    #[test]
    fn counts_retries_and_correlates_wait_without_counting_polling() {
        let request = RequestDetail {
            request: Some(vec![8, 1]),
        }
        .encode_to_vec();
        let mut rpc = Vec::new();
        record(
            "cache",
            14,
            details::Call::GetActionResult(request.clone()),
            &mut rpc,
        );
        record(
            "cache",
            0,
            details::Call::GetActionResult(request.clone()),
            &mut rpc,
        );
        record(
            "cache",
            0,
            details::Call::GetActionResult(request),
            &mut rpc,
        );
        let distinct = RequestDetail {
            request: Some(vec![8, 2]),
        }
        .encode_to_vec();
        record(
            "cache",
            0,
            details::Call::GetActionResult(distinct.clone()),
            &mut rpc,
        );
        record(
            "cache",
            0,
            details::Call::GetActionResult(distinct),
            &mut rpc,
        );
        let execute = CallDetail {
            request: Some(vec![8, 3]),
            responses: vec![Operation {
                name: "operation/1".to_owned(),
            }],
        }
        .encode_to_vec();
        record("execute", 0, details::Call::Execute(execute), &mut rpc);
        let wait = CallDetail {
            request: Some(
                Operation {
                    name: "operation/1".to_owned(),
                }
                .encode_to_vec(),
            ),
            responses: Vec::new(),
        }
        .encode_to_vec();
        record(
            "wait",
            0,
            details::Call::WaitExecution(wait.clone()),
            &mut rpc,
        );
        record("wait", 0, details::Call::WaitExecution(wait), &mut rpc);
        let evidence = read(&format!("{ACTION}\n{FINISHED}\n"), &rpc).expect("evidence");
        assert_eq!(
            (
                evidence.rpc_calls,
                evidence.rpc_failures,
                evidence.rpc_retry_attempts
            ),
            (8, 1, 1)
        );
        assert_eq!(evidence.execute_wait_correlations, 2);
        rpc.pop();
        assert!(read(&format!("{ACTION}\n{FINISHED}\n"), &rpc).is_err());
    }

    #[test]
    fn write_retries_preserve_upload_identity() {
        let detail = |name: &str| {
            WriteDetail {
                resource_names: vec![name.to_owned(), String::new()],
                offsets: vec![0, 4],
                finish_writes: vec![0, 1],
            }
            .encode_to_vec()
        };
        let mut rpc = Vec::new();
        record(
            "write",
            14,
            details::Call::Write(detail("uploads/a")),
            &mut rpc,
        );
        record(
            "write",
            0,
            details::Call::Write(detail("uploads/b")),
            &mut rpc,
        );
        record(
            "write",
            0,
            details::Call::Write(detail("uploads/a")),
            &mut rpc,
        );
        record(
            "write",
            0,
            details::Call::Write(detail("uploads/a")),
            &mut rpc,
        );
        let evidence = read(&format!("{ACTION}\n{FINISHED}\n"), &rpc).expect("write evidence");
        assert_eq!(evidence.rpc_retry_attempts, 1);
    }
}
