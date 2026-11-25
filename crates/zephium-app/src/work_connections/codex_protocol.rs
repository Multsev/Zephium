//! Bounded, credential-free decoding for a local Codex app-server connection.
//! This module never starts a process, reads credentials, or grants access.
use serde_json::{json, Value};

pub const MAX_MESSAGE_BYTES: usize = 256 * 1024;
const MAX_TEXT_BYTES: usize = 320;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    TooLarge,
    Malformed,
    Unexpected,
    Remote,
}

#[derive(Clone, PartialEq, Eq)]
pub enum Account {
    SignedOut,
    ChatGpt { email: Option<String>, plan: String },
    ApiKey,
    Other,
}

impl std::fmt::Debug for Account {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SignedOut => f.write_str("SignedOut"),
            Self::ChatGpt { .. } => f.write_str("ChatGpt(<redacted>)"),
            Self::ApiKey => f.write_str("ApiKey"),
            Self::Other => f.write_str("Other"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Window {
    pub used_percent: i32,
    pub duration_minutes: Option<u64>,
    pub resets_at: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Limit {
    pub id: Option<String>,
    pub name: Option<String>,
    pub plan: Option<String>,
    pub primary: Option<Window>,
    pub secondary: Option<Window>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Model {
    pub id: String,
    pub name: String,
    pub is_default: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelsPage {
    pub models: Vec<Model>,
    pub next_cursor: Option<String>,
}

/// The only account-related requests admitted by this read-only codec.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Request {
    Initialize,
    Initialized,
    Account,
    Limits,
    Models,
}

impl Request {
    pub fn encode(self, id: u64) -> String {
        let message = match self {
            Self::Initialize => {
                json!({"id":id,"method":"initialize","params":{"clientInfo":{"name":"zephium","title":"Zephium","version":"0.1.0"}}})
            }
            Self::Initialized => json!({"method":"initialized","params":{}}),
            Self::Account => {
                json!({"id":id,"method":"account/read","params":{"refreshToken":false}})
            }
            Self::Limits => json!({"id":id,"method":"account/rateLimits/read","params":{}}),
            Self::Models => json!({"id":id,"method":"model/list","params":{}}),
        };
        let mut encoded = message.to_string();
        encoded.push('\n');
        encoded
    }
}

/// Continue model discovery using only a cursor supplied by the server.
pub fn models_next_page(id: u64, cursor: &str) -> Result<String, ProtocolError> {
    if text(&Value::String(cursor.to_owned())).is_none() {
        return Err(ProtocolError::Malformed);
    }
    let mut encoded = json!({"id":id,"method":"model/list","params":{"cursor":cursor}}).to_string();
    encoded.push('\n');
    Ok(encoded)
}

fn message(line: &[u8]) -> Result<Value, ProtocolError> {
    if line.len() > MAX_MESSAGE_BYTES {
        return Err(ProtocolError::TooLarge);
    }
    serde_json::from_slice(line).map_err(|_| ProtocolError::Malformed)
}

pub(super) fn response(line: &[u8], id: u64) -> Result<Value, ProtocolError> {
    let value = message(line)?;
    if value.get("id").and_then(Value::as_u64) != Some(id) {
        return Err(ProtocolError::Unexpected);
    }
    if value.get("error").is_some_and(|v| !v.is_null()) {
        return Err(ProtocolError::Remote);
    }
    value
        .get("result")
        .filter(|v| v.is_object())
        .cloned()
        .ok_or(ProtocolError::Malformed)
}

pub fn initialize_response(line: &[u8], id: u64) -> Result<(), ProtocolError> {
    response(line, id).map(|_| ())
}

fn text(value: &Value) -> Option<String> {
    let s = value.as_str()?;
    (!s.is_empty() && s.len() <= MAX_TEXT_BYTES && !s.chars().any(char::is_control))
        .then(|| s.to_owned())
}

pub fn account_response(line: &[u8], id: u64) -> Result<Account, ProtocolError> {
    let result = response(line, id)?;
    let account = result.get("account").ok_or(ProtocolError::Malformed)?;
    if account.is_null() {
        return Ok(Account::SignedOut);
    }
    match account.get("type").and_then(Value::as_str) {
        Some("chatgpt") => Ok(Account::ChatGpt {
            email: account.get("email").and_then(text),
            plan: account
                .get("planType")
                .and_then(text)
                .unwrap_or_else(|| "unknown".to_owned()),
        }),
        Some("apiKey") => Ok(Account::ApiKey),
        Some(_) => Ok(Account::Other),
        None => Err(ProtocolError::Malformed),
    }
}

fn window(value: Option<&Value>) -> Result<Option<Window>, ProtocolError> {
    let Some(v) = value.filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    let used = v
        .get("usedPercent")
        .and_then(Value::as_i64)
        .and_then(|n| i32::try_from(n).ok())
        .ok_or(ProtocolError::Malformed)?;
    let duration = v
        .get("windowDurationMins")
        .and_then(Value::as_u64)
        .filter(|n| *n > 0);
    let resets = v
        .get("resetsAt")
        .and_then(Value::as_i64)
        .filter(|n| *n >= 0);
    Ok(Some(Window {
        used_percent: used,
        duration_minutes: duration,
        resets_at: resets,
    }))
}

fn limit(value: &Value) -> Result<Limit, ProtocolError> {
    if !value.is_object() {
        return Err(ProtocolError::Malformed);
    }
    Ok(Limit {
        id: value.get("limitId").and_then(text),
        name: value.get("limitName").and_then(text),
        plan: value.get("planType").and_then(text),
        primary: window(value.get("primary"))?,
        secondary: window(value.get("secondary"))?,
    })
}

/// Decode a full snapshot. Prefer the multi-bucket map when supplied.
pub fn limits_response(line: &[u8], id: u64) -> Result<Vec<Limit>, ProtocolError> {
    let result = response(line, id)?;
    let buckets = match result.get("rateLimitsByLimitId") {
        None | Some(Value::Null) => None,
        Some(value) => Some(value.as_object().ok_or(ProtocolError::Malformed)?),
    };
    if let Some(map) = buckets {
        if map.len() > 64 {
            return Err(ProtocolError::TooLarge);
        }
        if !map.is_empty() {
            return map
                .iter()
                .map(|(key, value)| {
                    let mut item = limit(value)?;
                    if item.id.is_none() {
                        item.id = text(&Value::String(key.clone()));
                    }
                    Ok(item)
                })
                .collect();
        }
    }
    Ok(vec![limit(
        result.get("rateLimits").ok_or(ProtocolError::Malformed)?,
    )?])
}

/// Decode a model page without accepting executable commands or configuration.
pub fn models_response(line: &[u8], id: u64) -> Result<ModelsPage, ProtocolError> {
    let result = response(line, id)?;
    let entries = result
        .get("data")
        .and_then(Value::as_array)
        .ok_or(ProtocolError::Malformed)?;
    if entries.len() > 256 {
        return Err(ProtocolError::TooLarge);
    }
    let mut models = Vec::new();
    for entry in entries {
        if entry
            .get("hidden")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            continue;
        }
        models.push(Model {
            id: entry
                .get("model")
                .and_then(text)
                .ok_or(ProtocolError::Malformed)?,
            name: entry
                .get("displayName")
                .and_then(text)
                .ok_or(ProtocolError::Malformed)?,
            is_default: entry
                .get("isDefault")
                .and_then(Value::as_bool)
                .ok_or(ProtocolError::Malformed)?,
        });
    }
    let next_cursor = match result.get("nextCursor") {
        None | Some(Value::Null) => None,
        Some(cursor) => Some(text(cursor).ok_or(ProtocolError::Malformed)?),
    };
    Ok(ModelsPage {
        models,
        next_cursor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn encodes_only_fixed_protocol_requests() {
        let request: Value = serde_json::from_str(&Request::Account.encode(7)).unwrap();
        assert_eq!(request["id"], 7);
        assert_eq!(request["method"], "account/read");
        assert_eq!(request["params"]["refreshToken"], false);
        let notification: Value = serde_json::from_str(&Request::Initialized.encode(9)).unwrap();
        assert!(notification.get("id").is_none());
    }
    #[test]
    fn account_variants_and_remote_errors() {
        assert!(!format!(
            "{:?}",
            Account::ChatGpt {
                email: Some("private@example.com".into()),
                plan: "pro".into()
            }
        )
        .contains("private@example.com"));
        assert_eq!(
            account_response(br#"{"id":1,"result":{"account":null}}"#, 1),
            Ok(Account::SignedOut)
        );
        assert_eq!(account_response(br#"{"id":1,"result":{"account":{"type":"chatgpt","planType":"prolite","email":null}}}"#, 1), Ok(Account::ChatGpt { email: None, plan: "prolite".into() }));
        assert_eq!(
            account_response(br#"{"id":1,"error":{"message":"secret"}}"#, 1),
            Err(ProtocolError::Remote)
        );
        assert_eq!(
            account_response(br#"{"id":2,"result":{}}"#, 1),
            Err(ProtocolError::Unexpected)
        );
        assert_eq!(
            account_response(&vec![b' '; MAX_MESSAGE_BYTES + 1], 1),
            Err(ProtocolError::TooLarge)
        );
    }
    #[test]
    fn multiple_limit_buckets_do_not_invent_secondary_windows() {
        let line=br#"{"id":2,"result":{"rateLimits":{},"rateLimitsByLimitId":{"codex":{"primary":{"usedPercent":14,"windowDurationMins":10080,"resetsAt":1795760117},"secondary":null,"planType":"prolite"}}}}"#;
        let limits = limits_response(line, 2).unwrap();
        assert_eq!(limits.len(), 1);
        assert_eq!(limits[0].id.as_deref(), Some("codex"));
        assert_eq!(limits[0].primary.as_ref().unwrap().used_percent, 14);
        assert_eq!(limits[0].secondary, None);
        let overflow = limits_response(
            br#"{"id":2,"result":{"rateLimits":{"primary":{"usedPercent":101}}}}"#,
            2,
        )
        .unwrap();
        assert_eq!(overflow[0].primary.as_ref().unwrap().used_percent, 101);
        assert_eq!(
            limits_response(
                br#"{"id":2,"result":{"rateLimits":{},"rateLimitsByLimitId":"bad"}}"#,
                2
            ),
            Err(ProtocolError::Malformed)
        );
    }
    #[test]
    fn model_pages_hide_internal_models_and_validate_metadata() {
        let line = br#"{"id":3,"result":{"data":[{"model":"gpt-a","displayName":"GPT A","isDefault":true},{"hidden":true}],"nextCursor":"next"}}"#;
        let page = models_response(line, 3).unwrap();
        assert_eq!(page.models.len(), 1);
        assert_eq!(page.models[0].id, "gpt-a");
        assert_eq!(page.next_cursor.as_deref(), Some("next"));
        let next: Value = serde_json::from_str(
            &models_next_page(4, page.next_cursor.as_deref().unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(next["params"]["cursor"], "next");
        assert_eq!(models_next_page(4, "\n"), Err(ProtocolError::Malformed));
        assert_eq!(
            models_response(br#"{"id":3,"result":{"data":[{"model":"x"}]}}"#, 3),
            Err(ProtocolError::Malformed)
        );
    }
}
