use agent_remote_protocol::{ErrorCode, ProtocolError};

/// Apply a patch to `original`, returning the new content.
///
/// Patch format is a simple line-based script. Each line is one of:
///
///   `<lineno>a <text>`      append `<text>` after line `lineno` (1-based, 0 = at start)
///   `<lineno>d`             delete line `lineno` (1-based)
///   `<lineno>c <text>`      change line `lineno` to `<text>`
///   empty line             ignored
///   line starting with `#` ignored (comment)
///
/// Edits are applied in line-number order after sorting; line numbers refer to
/// the ORIGINAL content. When multiple edits target overlapping line numbers,
/// the patch is rejected as invalid.
///
/// This is intentionally simple and human-auditable; it is not the unified
/// diff format. The `<text>` is taken literally up to the end of the patch
/// line, so it may contain spaces but cannot contain a newline.
pub fn apply_patch(original: &str, patch: &str) -> Result<String, ProtocolError> {
    let mut lines: Vec<String> = original.split('\n').map(|s| s.to_string()).collect();
    // The split-on-'\n' trick produces a trailing empty element when the
    // original ends with '\n'. Drop it so line numbering matches a normal
    // text editor, then re-append a trailing newline at the end if the
    // original had one.
    let had_trailing_newline = original.ends_with('\n');
    if had_trailing_newline {
        lines.pop();
    }

    enum Edit {
        Append(usize, String),
        Delete(usize),
        Change(usize, String),
    }

    let mut edits: Vec<Edit> = Vec::new();
    for raw in patch.split('\n') {
        let line = if let Some(stripped) = raw.strip_prefix('#') {
            // Comments must still be syntactically `# <something>`. An empty
            // comment or whitespace-only is allowed.
            let _ = stripped;
            continue;
        } else {
            raw
        };
        if line.is_empty() {
            continue;
        }
        // Split into "<num><op>" and the remainder. The op char is one of
        // a/d/c. Find the first non-digit.
        let (num_str, rest) = match line.char_indices().find(|(_, c)| !c.is_ascii_digit()) {
            Some((idx, _)) => (&line[..idx], &line[idx..]),
            None => {
                return Err(ProtocolError::new(
                    ErrorCode::PatchFailed,
                    format!("invalid patch line (no operator): {line:?}"),
                ))
            }
        };
        let num: usize = num_str.parse().map_err(|_| {
            ProtocolError::new(
                ErrorCode::PatchFailed,
                format!("invalid line number in patch: {num_str:?}"),
            )
        })?;
        let op = rest.chars().next().ok_or_else(|| {
            ProtocolError::new(ErrorCode::PatchFailed, "missing patch operator".to_string())
        })?;
        let arg = &rest[op.len_utf8()..];
        match op {
            'a' => {
                let text = arg.strip_prefix(' ').unwrap_or(arg).to_string();
                edits.push(Edit::Append(num, text));
            }
            'd' => {
                if !arg.is_empty() {
                    return Err(ProtocolError::new(
                        ErrorCode::PatchFailed,
                        "delete edit must not carry text".to_string(),
                    ));
                }
                edits.push(Edit::Delete(num));
            }
            'c' => {
                let text = arg.strip_prefix(' ').unwrap_or(arg).to_string();
                edits.push(Edit::Change(num, text));
            }
            other => {
                return Err(ProtocolError::new(
                    ErrorCode::PatchFailed,
                    format!("unknown patch operator {other:?}"),
                ))
            }
        }
    }

    // Sort by target line number, stable so equal line numbers keep insertion
    // order. Then detect overlapping targets.
    edits.sort_by_key(|e| match e {
        Edit::Append(n, _) | Edit::Delete(n) | Edit::Change(n, _) => *n,
    });
    let mut seen: std::collections::HashMap<usize, ()> = std::collections::HashMap::new();
    for e in &edits {
        let n = match e {
            Edit::Append(n, _) | Edit::Delete(n) | Edit::Change(n, _) => *n,
        };
        if seen.insert(n, ()).is_some() {
            return Err(ProtocolError::new(
                ErrorCode::PatchFailed,
                format!("conflicting edits at line {n}"),
            ));
        }
    }

    // Apply highest-line-number-first so earlier indices stay valid.
    for e in edits.into_iter().rev() {
        match e {
            Edit::Append(0, text) => lines.insert(0, text),
            Edit::Append(n, text) => {
                if n > lines.len() {
                    return Err(ProtocolError::new(
                        ErrorCode::PatchFailed,
                        format!("append position {n} out of range (len {})", lines.len()),
                    ));
                }
                lines.insert(n, text);
            }
            Edit::Delete(n) => {
                if n == 0 || n > lines.len() {
                    return Err(ProtocolError::new(
                        ErrorCode::PatchFailed,
                        format!("delete position {n} out of range (len {})", lines.len()),
                    ));
                }
                lines.remove(n - 1);
            }
            Edit::Change(n, text) => {
                if n == 0 || n > lines.len() {
                    return Err(ProtocolError::new(
                        ErrorCode::PatchFailed,
                        format!("change position {n} out of range (len {})", lines.len()),
                    ));
                }
                lines[n - 1] = text;
            }
        }
    }

    let mut out = lines.join("\n");
    if had_trailing_newline {
        out.push('\n');
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn change_line() {
        let original = "a\nb\nc\n";
        let patched = apply_patch(original, "2c BEE").unwrap();
        assert_eq!(patched, "a\nBEE\nc\n");
    }

    #[test]
    fn delete_line() {
        let original = "a\nb\nc\n";
        let patched = apply_patch(original, "2d").unwrap();
        assert_eq!(patched, "a\nc\n");
    }

    #[test]
    fn append_after() {
        let original = "a\nb\n";
        let patched = apply_patch(original, "1a INSERTED").unwrap();
        assert_eq!(patched, "a\nINSERTED\nb\n");
    }

    #[test]
    fn append_at_start() {
        let original = "a\nb\n";
        let patched = apply_patch(original, "0a FIRST").unwrap();
        assert_eq!(patched, "FIRST\na\nb\n");
    }

    #[test]
    fn comment_and_blank_ignored() {
        let original = "a\nb\n";
        let patched = apply_patch(original, "# hello\n\n1c ALPHA\n").unwrap();
        assert_eq!(patched, "ALPHA\nb\n");
    }

    #[test]
    fn conflicting_edits_rejected() {
        let original = "a\nb\nc\n";
        let err = apply_patch(original, "2c X\n2d").unwrap_err();
        assert_eq!(err.code, ErrorCode::PatchFailed);
    }

    #[test]
    fn out_of_range_rejected() {
        let original = "a\n";
        let err = apply_patch(original, "9d").unwrap_err();
        assert_eq!(err.code, ErrorCode::PatchFailed);
    }

    #[test]
    fn preserves_no_trailing_newline() {
        let original = "a\nb";
        let patched = apply_patch(original, "1c X").unwrap();
        assert_eq!(patched, "X\nb");
    }

    #[test]
    fn empty_patch_is_identity() {
        let original = "a\nb\nc\n";
        let patched = apply_patch(original, "").unwrap();
        assert_eq!(patched, original);
    }
}
