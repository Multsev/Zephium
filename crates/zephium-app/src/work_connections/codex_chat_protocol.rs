//! Restricted chat messages for the Codex app-server protocol.
//! Policy admission and process confinement are the caller's responsibility.
use super::codex_protocol::{self, ProtocolError, MAX_MESSAGE_BYTES};
use serde_json::{json, Value};

const MAX_ID: usize = 320;
const MAX_PROMPT: usize = 64 * 1024;

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_ID && !id.chars().any(char::is_control)
}
fn encoded(value: Value) -> String {
    let mut line = value.to_string();
    line.push('\n');
    line
}

/// The caller must choose the model from the authenticated server's catalog.
pub fn start_thread(id: u64, model: &str) -> Result<String, ProtocolError> {
    if !valid_id(model) {
        return Err(ProtocolError::Malformed);
    }
    Ok(encoded(json!({"id":id,"method":"thread/start","params":{
        "model":model,"sandbox":"read-only","approvalPolicy":"on-request","approvalsReviewer":"user"
    }})))
}

pub fn resume_thread(id: u64, thread: &str) -> Result<String, ProtocolError> {
    if !valid_id(thread) {
        return Err(ProtocolError::Malformed);
    }
    Ok(encoded(json!({"id":id,"method":"thread/resume","params":{
        "threadId":thread,"sandbox":"read-only","approvalPolicy":"on-request","approvalsReviewer":"user"
    }})))
}

pub fn start_turn(id: u64, thread: &str, prompt: &str) -> Result<String, ProtocolError> {
    if !valid_id(thread)
        || prompt.trim().is_empty()
        || prompt.len() > MAX_PROMPT
        || prompt.contains('\0')
    {
        return Err(ProtocolError::Malformed);
    }
    let request = encoded(json!({"id":id,"method":"turn/start","params":{
        "threadId":thread,"input":[{"type":"text","text":prompt}]
    }}));
    if request.len() > MAX_MESSAGE_BYTES {
        return Err(ProtocolError::TooLarge);
    }
    Ok(request)
}

pub fn interrupt_turn(id: u64, thread: &str, turn: &str) -> Result<String, ProtocolError> {
    if !valid_id(thread) || !valid_id(turn) {
        return Err(ProtocolError::Malformed);
    }
    Ok(encoded(
        json!({"id":id,"method":"turn/interrupt","params":{"threadId":thread,"turnId":turn}}),
    ))
}

fn nested_id(line: &[u8], id: u64, field: &str) -> Result<String, ProtocolError> {
    let result = codex_protocol::response(line, id)?;
    result
        .get(field)
        .and_then(|value| value.get("id"))
        .and_then(Value::as_str)
        .filter(|value| valid_id(value))
        .map(str::to_owned)
        .ok_or(ProtocolError::Malformed)
}
pub fn thread_response(line: &[u8], id: u64) -> Result<String, ProtocolError> {
    let result = codex_protocol::response(line, id)?;
    let sandbox = result.get("sandbox").ok_or(ProtocolError::Malformed)?;
    if sandbox.get("type").and_then(Value::as_str) != Some("readOnly")
        || !matches!(
            sandbox.get("networkAccess"),
            None | Some(Value::Bool(false))
        )
        || !matches!(
            result.get("approvalPolicy").and_then(Value::as_str),
            Some("on-request" | "untrusted")
        )
        || result.get("approvalsReviewer").and_then(Value::as_str) != Some("user")
    {
        return Err(ProtocolError::Unexpected);
    }
    nested_id(line, id, "thread")
}
pub fn turn_response(line: &[u8], id: u64) -> Result<String, ProtocolError> {
    nested_id(line, id, "turn")
}

#[derive(Clone, PartialEq, Eq)]
pub enum TurnEvent {
    Delta(String),
    ApprovalRequested(super::codex_approval::Approval),
    Completed,
    Interrupted,
    Failed,
    Other,
}

impl std::fmt::Debug for TurnEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Delta(_) => f.write_str("Delta(<redacted>)"),
            Self::ApprovalRequested(approval) => {
                f.debug_tuple("ApprovalRequested").field(approval).finish()
            }
            Self::Completed => f.write_str("Completed"),
            Self::Interrupted => f.write_str("Interrupted"),
            Self::Failed => f.write_str("Failed"),
            Self::Other => f.write_str("Other"),
        }
    }
}

/// Parse notifications only for the active turn. Error details and unrelated
/// events are deliberately not exposed to the UI by this decoder.
pub fn event(line: &[u8], thread: &str, turn: &str) -> Result<TurnEvent, ProtocolError> {
    if line.len() > MAX_MESSAGE_BYTES {
        return Err(ProtocolError::TooLarge);
    }
    if !valid_id(thread) || !valid_id(turn) {
        return Err(ProtocolError::Malformed);
    }
    let value: Value = serde_json::from_slice(line).map_err(|_| ProtocolError::Malformed)?;
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .ok_or(ProtocolError::Malformed)?;
    if value.get("id").is_some() {
        return Err(ProtocolError::Unexpected);
    }
    if method != "item/agentMessage/delta" && method != "turn/completed" {
        return Ok(TurnEvent::Other);
    }
    let params = value.get("params").ok_or(ProtocolError::Malformed)?;
    if params.get("threadId").and_then(Value::as_str) != Some(thread) {
        return Err(ProtocolError::Unexpected);
    }
    match method {
        "item/agentMessage/delta" => {
            if params.get("turnId").and_then(Value::as_str) != Some(turn) {
                return Err(ProtocolError::Unexpected);
            }
            let delta = params
                .get("delta")
                .and_then(Value::as_str)
                .ok_or(ProtocolError::Malformed)?;
            Ok(TurnEvent::Delta(delta.to_owned()))
        }
        "turn/completed" => {
            let result = params.get("turn").ok_or(ProtocolError::Malformed)?;
            if result.get("id").and_then(Value::as_str) != Some(turn) {
                return Err(ProtocolError::Unexpected);
            }
            match result.get("status").and_then(Value::as_str) {
                Some("completed") => Ok(TurnEvent::Completed),
                Some("interrupted") => Ok(TurnEvent::Interrupted),
                Some("failed") => Ok(TurnEvent::Failed),
                _ => Err(ProtocolError::Malformed),
            }
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn requests_keep_approval_and_sandbox_policy_fixed() {
        let value: Value = serde_json::from_str(&start_thread(1, "model-a").unwrap()).unwrap();
        assert_eq!(value["params"]["sandbox"], "read-only");
        assert_eq!(value["params"]["approvalPolicy"], "on-request");
        assert_eq!(value["params"]["approvalsReviewer"], "user");
        assert!(start_turn(2, "thread", &"x".repeat(MAX_PROMPT + 1)).is_err());
        assert_eq!(
            start_turn(2, "thread", &"\u{1}".repeat(MAX_PROMPT)),
            Err(ProtocolError::TooLarge)
        );
        assert!(interrupt_turn(2, "thread\n", "turn").is_err());
        let safe = br#"{"id":1,"result":{"thread":{"id":"abc"},"sandbox":{"type":"readOnly","networkAccess":false},"approvalPolicy":"on-request","approvalsReviewer":"user"}}"#;
        assert_eq!(thread_response(safe, 1).unwrap(), "abc");
        let unsafe_policy = br#"{"id":1,"result":{"thread":{"id":"abc"},"sandbox":{"type":"dangerFullAccess"},"approvalPolicy":"never","approvalsReviewer":"user"}}"#;
        assert_eq!(
            thread_response(unsafe_policy, 1),
            Err(ProtocolError::Unexpected)
        );
        for (field, value) in [
            ("network", json!(true)),
            ("reviewer", json!("auto_review")),
            ("approval", json!("never")),
        ] {
            let mut changed: Value = serde_json::from_slice(safe).unwrap();
            match field {
                "network" => changed["result"]["sandbox"]["networkAccess"] = value,
                "reviewer" => changed["result"]["approvalsReviewer"] = value,
                _ => changed["result"]["approvalPolicy"] = value,
            }
            assert_eq!(
                thread_response(changed.to_string().as_bytes(), 1),
                Err(ProtocolError::Unexpected)
            );
        }
    }
    #[test]
    fn events_are_scoped_to_the_active_turn() {
        let line=br#"{"method":"item/agentMessage/delta","params":{"threadId":"t","turnId":"u","delta":"Hi"}}"#;
        assert_eq!(event(line, "t", "u"), Ok(TurnEvent::Delta("Hi".into())));
        assert!(!format!("{:?}", TurnEvent::Delta("private text".into())).contains("private text"));
        assert_eq!(event(line, "t", "other"), Err(ProtocolError::Unexpected));
        let done=br#"{"method":"turn/completed","params":{"threadId":"t","turn":{"id":"u","status":"interrupted"}}}"#;
        assert_eq!(event(done, "t", "u"), Ok(TurnEvent::Interrupted));
        assert_eq!(
            event(br#"{"id":3,"method":"item/agentMessage/delta"}"#, "t", "u"),
            Err(ProtocolError::Unexpected)
        );
    }
}
