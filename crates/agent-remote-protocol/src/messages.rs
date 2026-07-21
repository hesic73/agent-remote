use serde::{Deserialize, Serialize};

use crate::error::ProtocolError;

pub type RequestId = String;
pub type OperationId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub request_id: RequestId,
    #[serde(flatten)]
    pub body: RequestBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum RequestBody {
    List {
        path: String,
    },
    Stat {
        path: String,
    },
    Read {
        path: String,
        #[serde(default)]
        offset: Option<u64>,
        #[serde(default)]
        limit: Option<u64>,
    },
    Write {
        path: String,
        content: String,
        #[serde(default)]
        base_hash: Option<String>,
    },
    Patch {
        path: String,
        base_hash: String,
        patch: String,
    },
    Exec {
        argv: Vec<String>,
        #[serde(default)]
        cwd: Option<String>,
        #[serde(default)]
        profile: Option<String>,
        #[serde(default)]
        timeout_ms: Option<u64>,
    },
    Delete {
        path: String,
    },
    Undo {
        operation_id: OperationId,
    },
    History {
        #[serde(default)]
        limit: Option<usize>,
    },
    OperationGet {
        operation_id: OperationId,
    },
    RequestStatus {
        #[serde(rename = "target_request_id")]
        target: RequestId,
    },
    /// Prune stored history: keep only the `keep` most recent operations
    /// (dropping their blobs) and the request entries they reference. `None`
    /// uses the server's configured history limit.
    Gc {
        #[serde(default)]
        keep: Option<usize>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ServerMessage {
    Result {
        request_id: RequestId,
        #[serde(flatten)]
        result: ResultBody,
    },
    Error {
        request_id: RequestId,
        #[serde(flatten)]
        error: ProtocolError,
    },
    ExecEvent(ExecEvent),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ResultBody {
    #[serde(rename = "list")]
    List { entries: Vec<ListEntry> },
    #[serde(rename = "stat")]
    Stat { stat: FileEntry },
    #[serde(rename = "read")]
    Read(ReadResult),
    #[serde(rename = "write")]
    WriteOrPatch(WriteOrPatchResult),
    #[serde(rename = "exit")]
    Exit {
        exit_code: i32,
        operation_id: OperationId,
    },
    #[serde(rename = "undo")]
    Undo(UndoResult),
    #[serde(rename = "history")]
    History {
        operations: Vec<crate::record::AnyOperationRecord>,
    },
    #[serde(rename = "operation")]
    Operation(OperationDetails),
    #[serde(rename = "request_status")]
    RequestStatus(RequestStatusResult),
    #[serde(rename = "gc")]
    Gc(GcResult),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListEntry {
    pub name: String,
    pub kind: ListKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ListKind {
    File,
    Dir,
    Symlink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: String,
    pub kind: ListKind,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<FileMode>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct FileMode {
    pub readable: bool,
    pub writable: bool,
    pub executable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadResult {
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteOrPatchResult {
    pub operation_id: OperationId,
    pub old_hash: Option<String>,
    pub new_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecEvent {
    pub request_id: RequestId,
    #[serde(flatten)]
    pub event: ExecEventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ExecEventKind {
    #[serde(rename = "stdout")]
    Stdout { data: String },
    #[serde(rename = "stderr")]
    Stderr { data: String },
    #[serde(rename = "exit")]
    Exit {
        exit_code: i32,
        operation_id: OperationId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UndoResult {
    pub operation_id: OperationId,
    pub restored_hash: Option<String>,
    pub new_hash: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct GcResult {
    pub removed_operations: usize,
    pub removed_requests: usize,
    pub retained_operations: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationDetails {
    pub record: crate::record::AnyOperationRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestStatusResult {
    #[serde(rename = "target_request_id")]
    pub target: RequestId,
    pub status: RequestStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ProtocolError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RequestStatus {
    Unknown,
    InProgress,
    Done,
    Error,
}
