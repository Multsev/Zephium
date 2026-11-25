//! Bounded approval previews for one active Codex turn.
use super::codex_protocol::{ProtocolError, MAX_MESSAGE_BYTES};
use serde_json::{json, Value};
use std::fmt;

const MAX_COMMAND: usize = 8192;
const MAX_DETAIL: usize = 2048;

#[derive(Clone, PartialEq, Eq)]
pub struct Approval {
    /// Unique within the active turn, assigned by Zephium.
    pub nonce: u64,
    pub command: String,
    pub cwd: String,
    pub reason: Option<String>,
    server_id: Value,
}
impl fmt::Debug for Approval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Approval")
            .field("nonce", &self.nonce)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    AcceptOnce,
    Decline,
    Cancel,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Answer {
    pub nonce: u64,
    pub decision: Decision,
}

fn optional_text(value: Option<&Value>, max: usize) -> Result<Option<String>, ProtocolError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value))
            if value.len() <= max
                && !value.chars().any(|c| {
                    (c.is_control() && !matches!(c, '\n' | '\r' | '\t'))
                        || matches!(c, '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
                }) => Ok(Some(value.clone())),
        _ => Err(ProtocolError::Malformed),
    }
}

/// Refuse requests whose full command or requested scope cannot be shown.
pub fn parse(line: &[u8], thread: &str, turn: &str, nonce: u64) -> Result<Approval, ProtocolError> {
    if line.len() > MAX_MESSAGE_BYTES {
        return Err(ProtocolError::TooLarge);
    }
    let value: Value = serde_json::from_slice(line).map_err(|_| ProtocolError::Malformed)?;
    if value.get("method").and_then(Value::as_str) != Some("item/commandExecution/requestApproval")
    {
        return Err(ProtocolError::Unexpected);
    }
    let id = value.get("id").ok_or(ProtocolError::Malformed)?;
    let valid_id = id.as_i64().is_some()
        || id
            .as_str()
            .is_some_and(|s| !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control));
    if !valid_id {
        return Err(ProtocolError::Malformed);
    }
    let params = value.get("params").ok_or(ProtocolError::Malformed)?;
    let params_object = params.as_object().ok_or(ProtocolError::Malformed)?;
    if params_object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "approvalId"
                | "command"
                | "commandActions"
                | "cwd"
                | "environmentId"
                | "itemId"
                | "networkApprovalContext"
                | "proposedExecpolicyAmendment"
                | "proposedNetworkPolicyAmendments"
                | "reason"
                | "startedAtMs"
                | "threadId"
                | "turnId"
        )
    }) {
        return Err(ProtocolError::Unexpected);
    }
    if params.get("threadId").and_then(Value::as_str) != Some(thread)
        || params.get("turnId").and_then(Value::as_str) != Some(turn)
    {
        return Err(ProtocolError::Unexpected);
    }
    for key in [
        "environmentId",
        "networkApprovalContext",
        "proposedExecpolicyAmendment",
        "proposedNetworkPolicyAmendments",
    ] {
        if params.get(key).is_some_and(|value| !value.is_null()) {
            return Err(ProtocolError::Unexpected);
        }
    }
    let command = optional_text(params.get("command"), MAX_COMMAND)?
        .filter(|s| !s.trim().is_empty())
        .ok_or(ProtocolError::Malformed)?;
    let cwd = optional_text(params.get("cwd"), MAX_DETAIL)?
        .filter(|s| !s.trim().is_empty() && !s.chars().any(char::is_control))
        .ok_or(ProtocolError::Malformed)?;
    Ok(Approval {
        nonce,
        command,
        cwd,
        reason: optional_text(params.get("reason"), MAX_DETAIL)?,
        server_id: id.clone(),
    })
}

pub fn response(request: &Approval, answer: Answer) -> Result<String, ProtocolError> {
    if answer.nonce != request.nonce {
        return Err(ProtocolError::Unexpected);
    }
    let decision = match answer.decision {
        Decision::AcceptOnce => "accept",
        Decision::Decline => "decline",
        Decision::Cancel => "cancel",
    };
    let mut line = json!({"id":request.server_id,"result":{"decision":decision}}).to_string();
    line.push('\n');
    Ok(line)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn approvals_are_scoped_and_only_single_use() {
        let request = br#"{"id":"a","method":"item/commandExecution/requestApproval","params":{"threadId":"t","turnId":"u","command":"ls -la","cwd":"/tmp"}}"#;
        let approval = parse(request, "t", "u", 7).unwrap();
        assert_eq!(approval.command, "ls -la");
        assert_eq!(
            parse(request, "t", "other", 7),
            Err(ProtocolError::Unexpected)
        );
        assert_eq!(
            response(
                &approval,
                Answer {
                    nonce: 8,
                    decision: Decision::AcceptOnce
                }
            ),
            Err(ProtocolError::Unexpected)
        );
        let response: Value = serde_json::from_str(
            &response(
                &approval,
                Answer {
                    nonce: 7,
                    decision: Decision::AcceptOnce,
                },
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(response["result"]["decision"], "accept");
        for (decision, expected) in [(Decision::Decline, "decline"), (Decision::Cancel, "cancel")] {
            let response: Value = serde_json::from_str(
                &super::response(&approval, Answer { nonce: 7, decision }).unwrap(),
            )
            .unwrap();
            assert_eq!(response["result"]["decision"], expected);
        }
        assert!(!format!("{approval:?}").contains("ls -la"));
        let network = br#"{"id":1,"method":"item/commandExecution/requestApproval","params":{"threadId":"t","turnId":"u","command":"curl example.com","networkApprovalContext":{}}}"#;
        assert_eq!(parse(network, "t", "u", 7), Err(ProtocolError::Unexpected));
        let unknown = br#"{"id":1,"method":"item/commandExecution/requestApproval","params":{"threadId":"t","turnId":"u","command":"ls","futureScope":"anything"}}"#;
        assert_eq!(parse(unknown, "t", "u", 7), Err(ProtocolError::Unexpected));
        let missing_cwd = br#"{"id":1,"method":"item/commandExecution/requestApproval","params":{"threadId":"t","turnId":"u","command":"ls"}}"#;
        assert_eq!(
            parse(missing_cwd, "t", "u", 7),
            Err(ProtocolError::Malformed)
        );
    }
}
