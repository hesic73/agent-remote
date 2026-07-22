mod error;
mod messages;
mod record;

pub use error::{ErrorCode, ProtocolError};
pub use messages::{
    ExecOutput, ExecResult, ExecTermination, FileEntry, FileMode, GcResult, ListEntry, ListKind,
    ListResult, OperationDetails, OperationId, ReadResult, Request, RequestBody, RequestId,
    RequestStatus, RequestStatusResult, ResultBody, ServerMessage, UndoResult, WriteOrPatchResult,
};
pub use record::{
    AbortedRecord, AnyOperationRecord, ExecDisposition, ExecOperationRecord, FsOperationRecord,
    OperationKind, PreparedRecord,
};

pub const PROTOCOL_VERSION: u32 = 1;
