use agent_remote_protocol::*;
use serde_json::json;

fn round_trip(req: &Request) -> Request {
    let line = serde_json::to_string(req).unwrap();
    let back: Request = serde_json::from_str(&line).unwrap();
    assert_eq!(line, serde_json::to_string(&back).unwrap());
    back
}

#[test]
fn list_round_trips() {
    let req = Request {
        request_id: "r1".into(),
        body: RequestBody::List {
            path: "src".into(),
            offset: None,
            limit: None,
        },
    };
    let line = serde_json::to_string(&req).unwrap();
    assert_eq!(line, r#"{"request_id":"r1","op":"list","path":"src"}"#);
    round_trip(&req);
}

#[test]
fn read_round_trips() {
    let req = Request {
        request_id: "r2".into(),
        body: RequestBody::Read {
            path: "src/main.py".into(),
            offset: Some(0),
            limit: Some(65536),
        },
    };
    let line = serde_json::to_string(&req).unwrap();
    assert_eq!(
        line,
        r#"{"request_id":"r2","op":"read","path":"src/main.py","offset":0,"limit":65536}"#
    );
    round_trip(&req);
}

#[test]
fn patch_round_trips() {
    let req = Request {
        request_id: "r3".into(),
        body: RequestBody::Patch {
            path: "src/main.py".into(),
            base_hash: "sha256:abc123".into(),
            patch: "...".into(),
        },
    };
    let line = serde_json::to_string(&req).unwrap();
    assert_eq!(
        line,
        r#"{"request_id":"r3","op":"patch","path":"src/main.py","base_hash":"sha256:abc123","patch":"..."}"#
    );
    round_trip(&req);
}

#[test]
fn exec_round_trips() {
    let req = Request {
        request_id: "r4".into(),
        body: RequestBody::Exec {
            argv: vec!["pytest".into(), "-q".into()],
            cwd: Some(".".into()),
            profile: Some("robot".into()),
            timeout_ms: Some(300000),
        },
    };
    let line = serde_json::to_string(&req).unwrap();
    assert_eq!(
        line,
        r#"{"request_id":"r4","op":"exec","argv":["pytest","-q"],"cwd":".","profile":"robot","timeout_ms":300000}"#
    );
    round_trip(&req);
}

#[test]
fn delete_round_trips() {
    let req = Request {
        request_id: "r5".into(),
        body: RequestBody::Delete {
            path: "old.txt".into(),
        },
    };
    let line = serde_json::to_string(&req).unwrap();
    assert_eq!(
        line,
        r#"{"request_id":"r5","op":"delete","path":"old.txt"}"#
    );
    round_trip(&req);
}

#[test]
fn result_message_serializes() {
    let msg = ServerMessage::Result {
        request_id: "r1".into(),
        result: ResultBody::Read(ReadResult {
            content: "...".into(),
            hash: Some("sha256:abc123".into()),
            truncated: false,
            next_offset: None,
        }),
    };
    let line = serde_json::to_string(&msg).unwrap();
    let v: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(v["request_id"], "r1");
    assert_eq!(v["type"], "read");
    assert_eq!(v["content"], "...");
    assert_eq!(v["hash"], "sha256:abc123");
    assert_eq!(v["truncated"], false);
}

#[test]
fn error_message_serializes_with_hashes() {
    let msg = ServerMessage::Error {
        request_id: "r3".into(),
        error: ProtocolError::new(ErrorCode::StaleFile, "file changed")
            .with_hashes("sha256:abc123".into(), "sha256:def456".into()),
    };
    let line = serde_json::to_string(&msg).unwrap();
    let v: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(v["request_id"], "r3");
    assert_eq!(v["code"], "STALE_FILE");
    assert_eq!(v["expected_hash"], "sha256:abc123");
    assert_eq!(v["actual_hash"], "sha256:def456");
}

#[test]
fn exec_result_message() {
    let result = ServerMessage::Result {
        request_id: "r4".into(),
        result: ResultBody::Exec(ExecResult {
            operation_id: "op-43".into(),
            termination: ExecTermination::Exited { code: 0 },
            duration_ms: 12,
            stdout: ExecOutput {
                prefix: "collecting tests...\n".into(),
                suffix: String::new(),
                total_bytes: 20,
                omitted_bytes: 0,
            },
            stderr: ExecOutput::default(),
        }),
    };
    let line = serde_json::to_string(&result).unwrap();
    let v: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(v["type"], "exec");
    assert_eq!(v["termination"]["kind"], "exited");
    assert_eq!(v["termination"]["code"], 0);
    assert_eq!(v["operation_id"], "op-43");
    assert_eq!(v["stdout"]["prefix"], "collecting tests...\n");
}

#[test]
fn unknown_op_returns_invalid_request_on_deserialize() {
    let bad = r#"{"request_id":"r","op":"frobnicate","path":"x"}"#;
    let err = serde_json::from_str::<Request>(bad);
    assert!(err.is_err());
}

#[test]
fn write_result_message_matches_design() {
    let msg = ServerMessage::Result {
        request_id: "r3".into(),
        result: ResultBody::WriteOrPatch(WriteOrPatchResult {
            operation_id: "op-42".into(),
            old_hash: Some("sha256:abc123".into()),
            new_hash: "sha256:def456".into(),
        }),
    };
    let v: serde_json::Value = serde_json::from_str(&serde_json::to_string(&msg).unwrap()).unwrap();
    assert_eq!(v["operation_id"], "op-42");
    assert_eq!(v["old_hash"], "sha256:abc123");
    assert_eq!(v["new_hash"], "sha256:def456");
    assert_eq!(v["type"], "write");
}

#[test]
fn enum_snake_case_tags() {
    // Ensure tag serialization stays lowercase snake_case as designed.
    let op = RequestBody::Stat { path: "x".into() };
    let v: serde_json::Value = serde_json::from_str(&serde_json::to_string(&op).unwrap()).unwrap();
    assert_eq!(v["op"], "stat");

    let op = RequestBody::OperationGet {
        operation_id: "op-1".into(),
    };
    let v: serde_json::Value = serde_json::from_str(&serde_json::to_string(&op).unwrap()).unwrap();
    assert_eq!(v["op"], "operation_get");
}

#[test]
fn json_literal_decodes_as_request() {
    let req: Request = serde_json::from_value(json!({
        "request_id": "r9",
        "op": "exec",
        "argv": ["echo", "hi"],
    }))
    .unwrap();
    match req.body {
        RequestBody::Exec {
            argv,
            cwd,
            profile,
            timeout_ms,
        } => {
            assert_eq!(argv, vec!["echo".to_string(), "hi".to_string()]);
            assert!(cwd.is_none());
            assert!(profile.is_none());
            assert!(timeout_ms.is_none());
        }
        _ => panic!("wrong body"),
    }
}

#[test]
fn every_result_variant_round_trips_through_server_message() {
    // Regression guard: ServerMessage::Result flattens both the envelope
    // request_id and the ResultBody. Any ResultBody field named request_id (or
    // any other duplicate) breaks serialization. Build each variant and verify
    // it round-trips.
    let cases: Vec<ResultBody> = vec![
        ResultBody::List(ListResult {
            entries: vec![],
            next_offset: None,
        }),
        ResultBody::Stat {
            stat: FileEntry {
                path: "x".into(),
                kind: ListKind::File,
                size: 1,
                hash: None,
                mode: None,
            },
        },
        ResultBody::Read(ReadResult {
            content: "c".into(),
            hash: None,
            truncated: false,
            next_offset: None,
        }),
        ResultBody::WriteOrPatch(WriteOrPatchResult {
            operation_id: "op-1".into(),
            old_hash: None,
            new_hash: "sha256:x".into(),
        }),
        ResultBody::Exec(ExecResult {
            operation_id: "op-2".into(),
            termination: ExecTermination::Exited { code: 0 },
            duration_ms: 1,
            stdout: ExecOutput::default(),
            stderr: ExecOutput::default(),
        }),
        ResultBody::Undo(UndoResult {
            operation_id: "op-3".into(),
            restored_hash: None,
            new_hash: "sha256:y".into(),
        }),
        ResultBody::History { operations: vec![] },
        ResultBody::Operation(OperationDetails {
            record: agent_remote_protocol::AnyOperationRecord::Fs(
                agent_remote_protocol::FsOperationRecord {
                    operation_id: "op-4".into(),
                    request_id: "r".into(),
                    kind: agent_remote_protocol::OperationKind::Write,
                    path: "p".into(),
                    before_hash: None,
                    after_hash: "sha256:z".into(),
                    timestamp_ms: 1,
                },
            ),
        }),
        ResultBody::RequestStatus(RequestStatusResult {
            target: "r".into(),
            status: RequestStatus::Done,
            error: None,
        }),
    ];
    for body in cases {
        let msg = ServerMessage::Result {
            request_id: "envelope".into(),
            result: body,
        };
        let s = serde_json::to_string(&msg).expect("must serialize without collision");
        let back: ServerMessage = serde_json::from_str(&s).expect("must deserialize round-trip");
        assert_eq!(s, serde_json::to_string(&back).unwrap());
    }
}
