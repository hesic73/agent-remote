pub mod config;
pub mod exec;
pub mod fs_ops;
pub mod fsync;
pub mod hash;
pub mod patch;
pub mod server;
pub mod store;
pub mod undo;
pub mod workspace;

pub use server::{Server, ServerOptions};

/// Default state directory for a workspace root: `<home>/.agent-remote/state/
/// <name>-<hash12>`, where `name` is the root's final path component and
/// `hash12` is the first 12 hex chars of sha256 over the canonical root path.
/// Keyed by path so every workspace gets its own isolated store, outside the
/// workspace itself.
pub fn default_state_dir(
    home: &std::path::Path,
    root: &std::path::Path,
) -> anyhow::Result<std::path::PathBuf> {
    let canonical = root
        .canonicalize()
        .map_err(|e| anyhow::anyhow!("cannot canonicalize workspace root {root:?}: {e}"))?;
    use sha2::Digest;
    let digest = sha2::Sha256::digest(canonical.as_os_str().as_encoded_bytes());
    let hash12 = hex::encode(&digest[..6]);
    let name = canonical
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "root".into());
    Ok(home
        .join(".agent-remote")
        .join("state")
        .join(format!("{name}-{hash12}")))
}

#[cfg(test)]
mod state_dir_tests {
    use super::default_state_dir;

    #[test]
    fn keyed_by_canonical_root() {
        let home = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let a = default_state_dir(home.path(), root.path()).unwrap();
        // A relative-ish spelling of the same root maps to the same directory.
        let dotted = root.path().join(".");
        let b = default_state_dir(home.path(), &dotted).unwrap();
        assert_eq!(a, b);
        assert!(a.starts_with(home.path().join(".agent-remote/state")));
        let leaf = a.file_name().unwrap().to_string_lossy().into_owned();
        let root_name = root.path().file_name().unwrap().to_string_lossy();
        assert!(leaf.starts_with(&format!("{root_name}-")));
    }

    #[test]
    fn missing_root_is_an_error() {
        let home = tempfile::tempdir().unwrap();
        assert!(default_state_dir(home.path(), std::path::Path::new("/nonexistent-xyz")).is_err());
    }
}
