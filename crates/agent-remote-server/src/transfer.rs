use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use agent_remote_protocol::{
    ErrorCode, ProtocolError, ResultBody, TransferDirection, TransferOperationRecord,
    TransferResult, UploadPrepareResult,
};
use sha2::{Digest, Sha256};
use tokio::sync::MutexGuard;

use crate::store::OperationStore;
use crate::workspace::Workspace;

pub const TRANSFER_BUF_SIZE: usize = 64 * 1024;

/// A staged upload awaiting commit. Lives only in memory: the staging file and
/// this entry die with the server process, and a client whose connection
/// dropped mid-transfer simply re-uploads.
#[derive(Clone)]
pub struct PendingUpload {
    pub staging: PathBuf,
    pub target: PathBuf,
    pub logical_path: String,
    pub overwrite: bool,
}

pub type UploadRegistry = parking_lot::Mutex<HashMap<String, PendingUpload>>;

fn new_transfer_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("xfer-{ts:016x}-{n:04x}")
}

pub fn upload_prepare(
    ws: &Workspace,
    registry: &UploadRegistry,
    path: &str,
    overwrite: bool,
) -> Result<ResultBody, ProtocolError> {
    let abs = ws.resolve(path)?;
    let parent = abs
        .parent()
        .ok_or_else(|| ProtocolError::new(ErrorCode::InvalidRequest, "path has no parent"))?;
    match std::fs::metadata(parent) {
        Ok(m) if m.is_dir() => {}
        Ok(_) => {
            return Err(ProtocolError::new(
                ErrorCode::NotADirectory,
                format!("parent of {path} is not a directory"),
            ))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(ProtocolError::new(
                ErrorCode::NotFound,
                format!("parent directory of {path} does not exist; create it first"),
            ))
        }
        Err(e) => return Err(e.into()),
    }
    match std::fs::symlink_metadata(&abs) {
        Ok(m) if m.is_dir() => {
            return Err(ProtocolError::new(
                ErrorCode::IsADirectory,
                format!("is a directory: {path}"),
            ))
        }
        Ok(m) if !m.is_file() => {
            return Err(ProtocolError::new(
                ErrorCode::NotAFile,
                format!("target exists and is not a regular file: {path}"),
            ))
        }
        Ok(_) if !overwrite => {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!("target already exists: {path}; pass overwrite=true to replace it"),
            ))
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let file_name = abs
        .file_name()
        .ok_or_else(|| ProtocolError::new(ErrorCode::InvalidRequest, "path has no file name"))?
        .to_string_lossy()
        .into_owned();
    // O_EXCL random name in the target's directory, so commit can link/rename
    // within one filesystem. Persisted (not auto-deleted): cleanup is owned by
    // commit/abort; a hard-killed server may leave a .part file behind.
    let staging = tempfile::Builder::new()
        .prefix(&format!(".{file_name}."))
        .suffix(".part")
        .tempfile_in(parent)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("create staging file: {e}")))?
        .into_temp_path()
        .keep()
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("keep staging file: {e}")))?;
    let transfer_id = new_transfer_id();
    registry.lock().insert(
        transfer_id.clone(),
        PendingUpload {
            staging: staging.clone(),
            target: abs,
            logical_path: path.to_string(),
            overwrite,
        },
    );
    Ok(ResultBody::UploadPrepare(UploadPrepareResult {
        transfer_id,
        staging_path: staging.to_string_lossy().into_owned(),
    }))
}

#[allow(clippy::too_many_arguments)]
pub fn upload_commit(
    store: &OperationStore,
    _guard: &MutexGuard<'_, ()>,
    request_id: &str,
    registry: &UploadRegistry,
    transfer_id: &str,
    size: u64,
    sha256: &str,
    duration_ms: u64,
) -> Result<ResultBody, ProtocolError> {
    let entry = registry.lock().get(transfer_id).cloned().ok_or_else(|| {
        ProtocolError::new(
            ErrorCode::OperationNotFound,
            format!("unknown transfer id: {transfer_id}"),
        )
    })?;
    let meta = std::fs::metadata(&entry.staging).map_err(|e| {
        ProtocolError::new(ErrorCode::IoError, format!("staging file unreadable: {e}"))
    })?;
    if !meta.is_file() {
        return Err(ProtocolError::new(
            ErrorCode::NotAFile,
            "staging path is not a regular file",
        ));
    }
    if meta.len() != size {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!(
                "staging file has {} bytes but client declared {size}",
                meta.len()
            ),
        ));
    }
    if entry.overwrite {
        std::fs::rename(&entry.staging, &entry.target)
            .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("rename failed: {e}")))?;
    } else {
        // Race-free no-replace install: hard_link fails if the target appeared
        // since prepare, and never clobbers it.
        std::fs::hard_link(&entry.staging, &entry.target).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                ProtocolError::new(
                    ErrorCode::InvalidRequest,
                    format!(
                        "target was created while the upload ran: {}; pass overwrite=true to replace it",
                        entry.logical_path
                    ),
                )
            } else {
                ProtocolError::new(ErrorCode::IoError, format!("link into place failed: {e}"))
            }
        })?;
        std::fs::remove_file(&entry.staging).map_err(|e| {
            ProtocolError::new(ErrorCode::IoError, format!("remove staging failed: {e}"))
        })?;
    }
    crate::fsync::fsync_file_or_dir(&entry.target)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("fsync target: {e}")))?;
    let operation_id = store.next_operation_id();
    store.append_transfer_record(TransferOperationRecord {
        operation_id: operation_id.clone(),
        request_id: request_id.to_string(),
        direction: TransferDirection::Upload,
        path: entry.logical_path.clone(),
        size,
        sha256: sha256.to_string(),
        duration_ms,
        timestamp_ms: now_ms(),
    })?;
    registry.lock().remove(transfer_id);
    Ok(ResultBody::Transfer(TransferResult {
        operation_id,
        direction: TransferDirection::Upload,
        path: entry.logical_path,
        size,
        sha256: sha256.to_string(),
        duration_ms,
    }))
}

pub fn upload_abort(
    registry: &UploadRegistry,
    transfer_id: &str,
) -> Result<ResultBody, ProtocolError> {
    let entry = registry.lock().remove(transfer_id).ok_or_else(|| {
        ProtocolError::new(
            ErrorCode::OperationNotFound,
            format!("unknown transfer id: {transfer_id}"),
        )
    })?;
    match std::fs::remove_file(&entry.staging) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(ProtocolError::new(
                ErrorCode::IoError,
                format!("remove staging failed: {e}"),
            ))
        }
    }
    Ok(ResultBody::UploadAbort {
        transfer_id: transfer_id.to_string(),
    })
}

#[allow(clippy::too_many_arguments)]
pub fn download_record(
    ws: &Workspace,
    store: &OperationStore,
    _guard: &MutexGuard<'_, ()>,
    request_id: &str,
    path: &str,
    size: u64,
    sha256: &str,
    duration_ms: u64,
) -> Result<ResultBody, ProtocolError> {
    ws.resolve(path)?;
    let operation_id = store.next_operation_id();
    store.append_transfer_record(TransferOperationRecord {
        operation_id: operation_id.clone(),
        request_id: request_id.to_string(),
        direction: TransferDirection::Download,
        path: path.to_string(),
        size,
        sha256: sha256.to_string(),
        duration_ms,
        timestamp_ms: now_ms(),
    })?;
    Ok(ResultBody::Transfer(TransferResult {
        operation_id,
        direction: TransferDirection::Download,
        path: path.to_string(),
        size,
        sha256: sha256.to_string(),
        duration_ms,
    }))
}

// ---- raw data plane (hidden CLI modes; no OperationStore, no state lock) ----

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ReceiveMetadata {
    pub size: u64,
    pub sha256: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SendHeader {
    pub size: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SendTrailer {
    pub sha256: String,
}

/// `--transfer-receive`: stream stdin into an existing staging file, then
/// report `{size, sha256}` on stdout. Fails if the byte count differs from
/// the caller-declared size.
pub fn run_transfer_receive(staging: &Path, expect_size: u64) -> anyhow::Result<()> {
    // The staging file must already exist (created by upload_prepare); its
    // absence means the prepare/commit lifecycle is being bypassed or raced.
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(staging)
        .map_err(|e| anyhow::anyhow!("open staging file {staging:?}: {e}"))?;
    let mut stdin = std::io::stdin().lock();
    let mut buf = vec![0u8; TRANSFER_BUF_SIZE];
    let mut hasher = Sha256::new();
    let mut total: u64 = 0;
    loop {
        let n = stdin.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        file.write_all(&buf[..n])?;
        total += n as u64;
    }
    if total != expect_size {
        anyhow::bail!("received {total} bytes but caller declared {expect_size}");
    }
    file.sync_all()?;
    let meta = ReceiveMetadata {
        size: total,
        sha256: format!("sha256:{}", hex::encode(hasher.finalize())),
    };
    println!("{}", serde_json::to_string(&meta)?);
    Ok(())
}

/// `--transfer-send`: re-validate the path against the workspace boundary,
/// then write to stdout: one JSON header line with the size, exactly that
/// many raw bytes, and one JSON trailer line with the SHA-256.
pub fn run_transfer_send(root: &Path, state_base: &Path, path: &str) -> anyhow::Result<()> {
    let state_dir = crate::state_dir_under(state_base, root)?;
    let ws = Workspace::new(root.to_path_buf(), state_dir.join("scratch"))?;
    let abs = ws.resolve(path).map_err(|e| anyhow::anyhow!("{e}"))?;
    let meta =
        std::fs::symlink_metadata(&abs).map_err(|e| anyhow::anyhow!("cannot stat {path}: {e}"))?;
    if !meta.is_file() {
        anyhow::bail!("not a regular file: {path}");
    }
    let size = meta.len();
    let mut file = std::fs::File::open(&abs)?;
    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut out, &SendHeader { size })?;
    out.write_all(b"\n")?;
    let mut buf = vec![0u8; TRANSFER_BUF_SIZE];
    let mut hasher = Sha256::new();
    let mut remaining = size;
    while remaining > 0 {
        let want = (remaining as usize).min(buf.len());
        let n = file.read(&mut buf[..want])?;
        if n == 0 {
            anyhow::bail!("file shrank during send: {path}");
        }
        hasher.update(&buf[..n]);
        out.write_all(&buf[..n])?;
        remaining -= n as u64;
    }
    let trailer = SendTrailer {
        sha256: format!("sha256:{}", hex::encode(hasher.finalize())),
    };
    serde_json::to_writer(&mut out, &trailer)?;
    out.write_all(b"\n")?;
    out.flush()?;
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
