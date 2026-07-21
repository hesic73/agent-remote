use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperationKind {
    Write,
    Patch,
    Delete,
    Undo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsOperationRecord {
    pub operation_id: String,
    pub request_id: String,
    pub kind: OperationKind,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_hash: Option<String>,
    pub after_hash: String,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecDisposition {
    /// The command ran to completion and produced an exit code.
    Completed,
    /// The command was killed because it exceeded timeout_ms. It DID run, so
    /// duration/captured output are meaningful and a (signal-derived) exit code
    /// is recorded where available.
    TimedOut,
    /// The command was never started, e.g. bad profile, invalid cwd, empty argv.
    Rejected,
}

/// Authoritative record of an exec invocation: argv, profile, exit code,
/// duration. Stdout/stderr are intentionally NOT stored inline (they may be
/// large); they are kept as blobs under `.agent-remote/blobs/<op>.{stdout,stderr}`
/// when present.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecOperationRecord {
    pub operation_id: String,
    pub request_id: String,
    pub argv: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    pub disposition: ExecDisposition,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
    pub timestamp_ms: u64,
    /// Human-readable error message for Rejected/TimedOut execs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Machine-readable error code for Rejected/TimedOut execs, so a replayed
    /// result after recovery returns the SAME type of error as the original
    /// invocation (preserving idempotency semantics).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<crate::error::ErrorCode>,
    /// True if the captured stdout/stderr blobs were truncated at the
    /// CAPTURE_LIMIT. Lets a consumer know the blob is not the full output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_truncated: Option<bool>,
}

/// A "prepared" (write-ahead) marker for an fs mutation, written before the
/// workspace is touched so startup recovery can detect a crash between the
/// rename and the commit-log write. This is an internal bookkeeping record;
/// it is reconciled away once the matching committed record is appended, and
/// it never appears in `history`/`operation.get` results seen by clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedRecord {
    pub operation_id: String,
    pub request_id: String,
    pub kind: OperationKind,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_hash: Option<String>,
    pub expected_after_hash: String,
    pub timestamp_ms: u64,
}

/// Union of the record kinds. The `Prepared` and `Aborted` variants are
/// internal (WAL only) and are stripped before serving `history`/`operation.get`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "record_kind", rename_all = "snake_case")]
pub enum AnyOperationRecord {
    Fs(FsOperationRecord),
    Exec(ExecOperationRecord),
    Prepared(PreparedRecord),
    /// Marks a prepared record as definitively rolled back. Written durably so
    /// a future restart does not resurrect the orphaned prepared marker.
    Aborted(AbortedRecord),
}

/// A durable "rolled back" marker that supersedes a prepared record for the
/// same operation_id but does not create a change record. It exists solely to
/// prevent a stale prepared line in the append-only log from being mistaken for
/// an uncommitted operation on a future restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbortedRecord {
    pub operation_id: String,
    pub timestamp_ms: u64,
}

impl AnyOperationRecord {
    pub fn operation_id(&self) -> &str {
        match self {
            AnyOperationRecord::Fs(r) => &r.operation_id,
            AnyOperationRecord::Exec(r) => &r.operation_id,
            AnyOperationRecord::Prepared(r) => &r.operation_id,
            AnyOperationRecord::Aborted(r) => &r.operation_id,
        }
    }

    pub fn timestamp_ms(&self) -> u64 {
        match self {
            AnyOperationRecord::Fs(r) => r.timestamp_ms,
            AnyOperationRecord::Exec(r) => r.timestamp_ms,
            AnyOperationRecord::Prepared(r) => r.timestamp_ms,
            AnyOperationRecord::Aborted(r) => r.timestamp_ms,
        }
    }

    /// True for client-facing records only (i.e. not WAL-internal markers).
    pub fn is_committed(&self) -> bool {
        matches!(
            self,
            AnyOperationRecord::Fs(_) | AnyOperationRecord::Exec(_)
        )
    }
}
