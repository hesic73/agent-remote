use std::path::Path;

use agent_remote_protocol::{
    ErrorCode, FileEntry, ListEntry, ListKind, OperationKind, ProtocolError, ReadResult,
    ResultBody, WriteOrPatchResult,
};
use tokio::sync::MutexGuard;

use crate::hash::hash_file;
use crate::patch::apply_patch;
use crate::store::OperationStore;
use crate::workspace::Workspace;

pub const LIST_DEFAULT_LIMIT: usize = 1000;
pub const LIST_MAX_LIMIT: usize = 1000;

pub fn list(
    ws: &Workspace,
    path: &str,
    offset: Option<usize>,
    limit: Option<usize>,
) -> Result<ResultBody, ProtocolError> {
    let offset = offset.unwrap_or(0);
    let limit = limit.unwrap_or(LIST_DEFAULT_LIMIT);
    if limit == 0 || limit > LIST_MAX_LIMIT {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("list limit must be between 1 and {LIST_MAX_LIMIT} entries"),
        ));
    }
    let abs = ws.resolve(path)?;
    let meta = std::fs::symlink_metadata(&abs).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ProtocolError::new(ErrorCode::NotFound, format!("not found: {path}"))
        } else {
            ProtocolError::new(ErrorCode::IoError, format!("list failed: {e}"))
        }
    })?;
    if !meta.is_dir() {
        return Err(ProtocolError::new(
            ErrorCode::NotADirectory,
            format!("not a directory: {path}"),
        ));
    }
    let mut entries: Vec<ListEntry> = std::fs::read_dir(&abs)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("read_dir failed: {e}")))?
        .filter_map(|e| e.ok())
        .map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            let m = e.metadata().ok();
            let kind = file_kind(&e.path());
            let size = m.as_ref().map(|m| m.len());
            ListEntry { name, kind, size }
        })
        .filter(|e| e.name != ".agent-remote")
        .collect();
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    let end = offset.saturating_add(limit).min(entries.len());
    let page = if offset >= entries.len() {
        Vec::new()
    } else {
        entries[offset..end].to_vec()
    };
    Ok(ResultBody::List(agent_remote_protocol::ListResult {
        entries: page,
        next_offset: (end < entries.len()).then_some(end),
    }))
}

pub fn stat(ws: &Workspace, path: &str) -> Result<ResultBody, ProtocolError> {
    let abs = ws.resolve(path)?;
    let meta = std::fs::symlink_metadata(&abs).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ProtocolError::new(ErrorCode::NotFound, format!("not found: {path}"))
        } else {
            ProtocolError::new(ErrorCode::IoError, format!("stat failed: {e}"))
        }
    })?;
    let entry = entry_for(path, &abs, &meta);
    Ok(ResultBody::Stat { stat: entry })
}

pub fn read(
    ws: &Workspace,
    path: &str,
    offset: Option<u64>,
    limit: Option<u64>,
) -> Result<ResultBody, ProtocolError> {
    let abs = ws.resolve(path)?;
    let meta = std::fs::symlink_metadata(&abs).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ProtocolError::new(ErrorCode::NotFound, format!("not found: {path}"))
        } else {
            ProtocolError::new(ErrorCode::IoError, format!("read failed: {e}"))
        }
    })?;
    if meta.is_dir() {
        return Err(ProtocolError::new(
            ErrorCode::IsADirectory,
            format!("is a directory: {path}"),
        ));
    }
    let bytes = std::fs::read(&abs)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("read failed: {e}")))?;
    // Hash the raw bytes so the returned hash matches what mutations compute
    // (they hash raw bytes too). Reject non-UTF-8 content rather than silently
    // returning a lossy conversion: a coding agent should not edit a binary
    // file through a text-oriented API.
    let full_content = String::from_utf8(bytes.clone()).map_err(|_| {
        ProtocolError::new(
            ErrorCode::InvalidRequest,
            "file is not valid UTF-8; binary reads are not supported",
        )
    })?;
    let full_hash = crate::hash::hash_bytes(&bytes);
    // offset/limit are BYTE positions but must land on UTF-8 char boundaries,
    // otherwise indexing panics. Reject (not truncate) a non-boundary offset so
    // a bad request can never crash the handler. Use checked arithmetic so huge
    // values cannot overflow.
    let offset_u64 = offset.unwrap_or(0);
    let limit_u64 = limit.unwrap_or(READ_DEFAULT_LIMIT);
    if limit_u64 == 0 || limit_u64 > READ_MAX_LIMIT {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("read limit must be between 1 and {READ_MAX_LIMIT} bytes"),
        ));
    }
    if offset_u64 > full_content.len() as u64 {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!(
                "offset {offset_u64} is past end of file ({} bytes)",
                full_content.len()
            ),
        ));
    }
    let start = offset_u64 as usize;
    if !full_content.is_char_boundary(start) {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("offset {start} is not on a UTF-8 character boundary"),
        ));
    }
    let end = match start.checked_add(limit_u64 as usize) {
        Some(e) => e.min(full_content.len()),
        None => full_content.len(),
    };
    // Walk FORWARD to the next char boundary at or after `end`. If we instead
    // rounded down, a multi-byte first codepoint with a tiny limit would
    // produce an empty page with truncated=true forever (the caller could never
    // make progress). Rounding up guarantees at least one codepoint is returned
    // whenever data remains and limit > 0.
    let end = if end >= full_content.len() {
        full_content.len()
    } else {
        nearest_char_boundary_at_or_after(&full_content, end)
    };
    let truncated = end < full_content.len();
    let content = full_content[start..end].to_string();
    Ok(ResultBody::Read(ReadResult {
        content,
        hash: Some(full_hash),
        truncated,
        next_offset: truncated.then_some(end as u64),
    }))
}

pub const READ_DEFAULT_LIMIT: u64 = 65536;
pub const READ_MAX_LIMIT: u64 = 64 * 1024;

/// Validate base_hash and return the current hash. If `base_hash` is given and
/// does not match the file's current content, returns StaleFile.
fn check_base_hash(
    abs: &Path,
    base_hash: &Option<String>,
) -> Result<Option<String>, ProtocolError> {
    let current = hash_file(abs)?;
    if let Some(expected) = base_hash {
        match &current {
            Some(actual) if actual == expected => Ok(current),
            Some(actual) => Err(ProtocolError::new(
                ErrorCode::StaleFile,
                "file changed since base_hash",
            )
            .with_hashes(expected.clone(), actual.clone())),
            None => Err(ProtocolError::new(
                ErrorCode::StaleFile,
                "file does not exist but base_hash was given",
            )
            .with_hashes(expected.clone(), "sha256:".into())),
        }
    } else {
        Ok(current)
    }
}

/// Atomically write bytes to abs: temp file in the same directory, then
/// persist via rename. Preserves the original file's mode when overwriting an
/// existing file (so a 0755 script stays 0755). Returns (old_hash, new_hash).
fn atomic_write_bytes(
    abs: &Path,
    content: &[u8],
) -> Result<(Option<String>, String), ProtocolError> {
    let parent = abs
        .parent()
        .ok_or_else(|| ProtocolError::new(ErrorCode::InvalidRequest, "path has no parent"))?;
    std::fs::create_dir_all(parent)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("mkdir failed: {e}")))?;
    let old_hash = hash_file(abs)?;
    let new_hash = crate::hash::hash_bytes(content);
    let tmp = tempfile::NamedTempFile::new_in(parent).map_err(|e| {
        ProtocolError::new(ErrorCode::IoError, format!("temp file create failed: {e}"))
    })?;
    std::fs::write(tmp.path(), content)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("temp write failed: {e}")))?;
    // Preserve the original file's permissions (and best-effort ownership) so
    // a write does not silently strip an executable bit or chmod.
    if let Ok(orig_meta) = std::fs::metadata(abs) {
        use std::os::unix::fs::PermissionsExt;
        let mode = orig_meta.permissions().mode();
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(mode)).map_err(
            |e| ProtocolError::new(ErrorCode::IoError, format!("chmod temp failed: {e}")),
        )?;
    }
    tmp.persist(abs).map_err(|e| {
        ProtocolError::new(ErrorCode::IoError, format!("atomic persist failed: {e}"))
    })?;
    // Sync the parent directory so the rename is durable on journaling/COW
    // filesystems.
    crate::fsync::fsync_file_or_dir(abs).map_err(|e| {
        ProtocolError::new(ErrorCode::IoError, format!("fsync after write failed: {e}"))
    })?;
    Ok((old_hash, new_hash))
}

#[allow(clippy::too_many_arguments)]
pub fn write(
    ws: &Workspace,
    store: &OperationStore,
    _guard: &MutexGuard<'_, ()>,
    request_id: &str,
    path: &str,
    content: &str,
    base_hash: &Option<String>,
) -> Result<ResultBody, ProtocolError> {
    let abs = ws.resolve(path)?;
    check_base_hash(&abs, base_hash)?;
    let before_hash = hash_file(&abs)?;
    let before_blob = std::fs::read(&abs).ok();
    let expected_after_hash = crate::hash::hash_bytes(content.as_bytes());
    // WAL step 1: durably record the prepared marker (with before blob) BEFORE
    // touching the workspace, so a crash mid-mutation is recoverable on startup.
    let op_id = store.prepare_fs_record(
        request_id,
        OperationKind::Write,
        path,
        before_hash.clone(),
        expected_after_hash,
        before_blob.as_deref(),
    )?;
    // WAL step 2: perform the atomic rename.
    let (old_hash, new_hash) = atomic_write_bytes(&abs, content.as_bytes())?;
    // WAL step 3: commit the real hashes (before blob already on disk).
    store.commit_fs_record(
        &op_id,
        request_id,
        OperationKind::Write,
        path,
        old_hash.clone(),
        new_hash.clone(),
    )?;
    Ok(ResultBody::WriteOrPatch(WriteOrPatchResult {
        operation_id: op_id,
        old_hash,
        new_hash,
    }))
}

#[allow(clippy::too_many_arguments)]
pub fn patch(
    ws: &Workspace,
    store: &OperationStore,
    _guard: &MutexGuard<'_, ()>,
    request_id: &str,
    path: &str,
    base_hash: &str,
    patch_text: &str,
) -> Result<ResultBody, ProtocolError> {
    let abs = ws.resolve(path)?;
    let current = check_base_hash(&abs, &Some(base_hash.to_string()))?;
    if current.is_none() {
        return Err(ProtocolError::new(
            ErrorCode::NotFound,
            format!("not found: {path}"),
        ));
    }
    let original = std::fs::read(&abs)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("read failed: {e}")))?;
    let original_str = String::from_utf8(original.clone()).map_err(|_| {
        ProtocolError::new(
            ErrorCode::InvalidRequest,
            "patch target is not valid UTF-8; patching binary files is unsupported",
        )
    })?;
    let new_content = apply_patch(&original_str, patch_text)?;
    let new_hash = crate::hash::hash_bytes(new_content.as_bytes());
    let op_id = store.prepare_fs_record(
        request_id,
        OperationKind::Patch,
        path,
        current.clone(),
        new_hash.clone(),
        Some(&original),
    )?;
    let old_hash = current.clone();
    atomic_write_bytes(&abs, new_content.as_bytes())?;
    store.commit_fs_record(
        &op_id,
        request_id,
        OperationKind::Patch,
        path,
        old_hash.clone(),
        new_hash.clone(),
    )?;
    Ok(ResultBody::WriteOrPatch(WriteOrPatchResult {
        operation_id: op_id,
        old_hash,
        new_hash,
    }))
}

#[allow(clippy::too_many_arguments)]
pub fn delete(
    ws: &Workspace,
    store: &OperationStore,
    _guard: &MutexGuard<'_, ()>,
    request_id: &str,
    path: &str,
) -> Result<ResultBody, ProtocolError> {
    let abs = ws.resolve(path)?;
    if abs.is_dir() {
        return Err(ProtocolError::new(
            ErrorCode::IsADirectory,
            format!("not a file: {path}"),
        ));
    }
    let before_blob = std::fs::read(&abs).ok();
    let before_hash = hash_file(&abs)?;
    if before_hash.is_none() {
        return Err(ProtocolError::new(
            ErrorCode::NotFound,
            format!("not found: {path}"),
        ));
    }
    // For a delete, "expected after" is the empty-file hash sentinel, matching
    // the deleted state (file absent).
    let op_id = store.prepare_fs_record(
        request_id,
        OperationKind::Delete,
        path,
        before_hash.clone(),
        "sha256:".into(),
        before_blob.as_deref(),
    )?;
    std::fs::remove_file(&abs)
        .map_err(|e| ProtocolError::new(ErrorCode::IoError, format!("remove failed: {e}")))?;
    if let Some(parent) = abs.parent() {
        crate::fsync::fsync_dir(parent).map_err(|e| {
            ProtocolError::new(ErrorCode::IoError, format!("fsync after delete: {e}"))
        })?;
    }
    store.commit_fs_record(
        &op_id,
        request_id,
        OperationKind::Delete,
        path,
        before_hash.clone(),
        "sha256:".into(),
    )?;
    Ok(ResultBody::WriteOrPatch(WriteOrPatchResult {
        operation_id: op_id,
        old_hash: before_hash,
        new_hash: "sha256:".into(),
    }))
}

fn file_kind(path: &Path) -> ListKind {
    let ft = match std::fs::symlink_metadata(path) {
        Ok(m) => m.file_type(),
        Err(_) => return ListKind::File,
    };
    if ft.is_symlink() {
        ListKind::Symlink
    } else if ft.is_dir() {
        ListKind::Dir
    } else {
        ListKind::File
    }
}

fn entry_for(client_path: &str, abs: &Path, meta: &std::fs::Metadata) -> FileEntry {
    use std::os::unix::fs::PermissionsExt;
    let kind = if meta.file_type().is_symlink() {
        ListKind::Symlink
    } else if meta.is_dir() {
        ListKind::Dir
    } else {
        ListKind::File
    };
    let mode = meta.permissions().mode();
    FileEntry {
        path: client_path.to_string(),
        kind,
        size: meta.len(),
        hash: if meta.is_file() {
            hash_file(abs).ok().flatten()
        } else {
            None
        },
        mode: Some(agent_remote_protocol::FileMode {
            readable: mode & 0o400 != 0,
            writable: mode & 0o200 != 0,
            executable: mode & 0o111 != 0,
        }),
    }
}

/// Smallest index >= `target` (and <= `s.len()`) that is a UTF-8 char
/// boundary. Used so a byte-based limit that lands inside a codepoint still
/// returns at least one codepoint, guaranteeing pagination makes progress.
fn nearest_char_boundary_at_or_after(s: &str, target: usize) -> usize {
    let target = target.min(s.len());
    let mut i = target;
    while i < s.len() && !s.is_char_boundary(i) {
        i += 1;
    }
    i
}

#[cfg(test)]
mod read_tests {
    use super::*;
    use tempfile::tempdir;

    fn ws_with(path: &str, content: &str) -> (tempfile::TempDir, Workspace) {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join(path), content).unwrap();
        let w = Workspace {
            root: dir.path().to_path_buf(),
            scratch_root: dir.path().join("scratch"),
        };
        (dir, w)
    }

    #[test]
    fn read_multibyte_offset_on_boundary_works() {
        // "éx" as UTF-8 is [0xC3,0xA9,0x78]; offset 2 lands on 'x'.
        let (_d, w) = ws_with("f.txt", "éx");
        let r = read(&w, "f.txt", Some(2), Some(1)).unwrap();
        match r {
            ResultBody::Read(r) => assert_eq!(r.content, "x"),
            _ => panic!("wrong body"),
        }
    }

    #[test]
    fn read_multibyte_offset_off_boundary_rejected_not_panic() {
        let (_d, w) = ws_with("f.txt", "éx");
        // offset 1 is mid-codepoint; must return an error, NOT panic.
        let res = read(&w, "f.txt", Some(1), Some(1));
        match res {
            Err(ProtocolError {
                code: ErrorCode::InvalidRequest,
                ..
            }) => {}
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    #[test]
    fn read_limit_mid_codepoint_snaps_up_to_boundary() {
        // "aébc": bytes a=1, é=2, b=1, c=1 -> total 5. limit 2 from offset 0
        // would end mid-é (byte 2); round UP to the next boundary (byte 3,
        // after 'é') so the page makes progress and never returns empty.
        let (_d, w) = ws_with("f.txt", "aébc");
        let r = read(&w, "f.txt", Some(0), Some(2)).unwrap();
        match r {
            ResultBody::Read(r) => {
                assert_eq!(r.content, "aé");
                assert!(r.truncated, "more content remains");
                assert_eq!(r.next_offset, Some(3));
            }
            _ => panic!("wrong body"),
        }
    }

    #[test]
    fn read_huge_offset_rejected_without_overflow() {
        let (_d, w) = ws_with("f.txt", "hi");
        let res = read(&w, "f.txt", Some(u64::MAX), Some(u64::MAX));
        assert!(matches!(
            res,
            Err(ProtocolError {
                code: ErrorCode::InvalidRequest,
                ..
            })
        ));
    }

    #[test]
    fn read_rejects_limit_above_hard_maximum() {
        let (_d, w) = ws_with("f.txt", "hi");
        let result = read(&w, "f.txt", None, Some(READ_MAX_LIMIT + 1));
        assert!(matches!(
            result,
            Err(ProtocolError {
                code: ErrorCode::InvalidRequest,
                ..
            })
        ));
    }
}
