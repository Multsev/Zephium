//! Read-only handshake and discovery over caller-owned streams.
//! This layer does not create a process or choose credentials.
use super::codex_protocol::{self as protocol, Account, Limit, Model, ProtocolError, Request};
use super::codex_transport::{self as transport, TransportError};
use std::collections::HashSet;
use tokio::io::{AsyncBufRead, AsyncWrite};

const MAX_MODEL_PAGES: usize = 8;

#[derive(Debug, PartialEq, Eq)]
pub enum SessionError {
    Transport(TransportError),
    Protocol(ProtocolError),
    TooManyPages,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub account: Account,
    pub limits: Option<Vec<Limit>>,
    pub models: Vec<Model>,
}

/// Run one discovery exchange. On any error the caller must close the streams,
/// because an incomplete request may leave responses in flight.
pub async fn read_snapshot<R, W>(reader: &mut R, writer: &mut W) -> Result<Snapshot, SessionError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let init = transport::request(reader, writer, Request::Initialize, 1)
        .await
        .map_err(SessionError::Transport)?;
    protocol::initialize_response(&init, 1).map_err(SessionError::Protocol)?;
    transport::initialized(writer)
        .await
        .map_err(SessionError::Transport)?;
    let account = transport::request(reader, writer, Request::Account, 2)
        .await
        .map_err(SessionError::Transport)?;
    let account = protocol::account_response(&account, 2).map_err(SessionError::Protocol)?;
    if account == Account::SignedOut {
        return Ok(Snapshot {
            account,
            limits: None,
            models: Vec::new(),
        });
    }
    let limits = transport::request(reader, writer, Request::Limits, 3)
        .await
        .map_err(SessionError::Transport)?;
    let limits = match protocol::limits_response(&limits, 3) {
        Ok(limits) => Some(limits),
        Err(ProtocolError::Remote) => None,
        Err(error) => return Err(SessionError::Protocol(error)),
    };
    let line = transport::request(reader, writer, Request::Models, 4)
        .await
        .map_err(SessionError::Transport)?;
    let mut page = protocol::models_response(&line, 4).map_err(SessionError::Protocol)?;
    let mut models = std::mem::take(&mut page.models);
    let mut seen = HashSet::new();
    for page_number in 1..MAX_MODEL_PAGES {
        let Some(cursor) = page.next_cursor.take() else {
            break;
        };
        if !seen.insert(cursor.clone()) {
            return Err(SessionError::Protocol(ProtocolError::Unexpected));
        }
        let id = 4 + page_number as u64;
        let line = transport::models_next_page(reader, writer, id, &cursor)
            .await
            .map_err(SessionError::Transport)?;
        page = protocol::models_response(&line, id).map_err(SessionError::Protocol)?;
        models.append(&mut page.models);
    }
    if page.next_cursor.is_some() {
        return Err(SessionError::TooManyPages);
    }
    Ok(Snapshot {
        account,
        limits,
        models,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use tokio::io::{duplex, AsyncBufReadExt, AsyncWriteExt, BufReader};

    #[tokio::test]
    async fn unavailable_limits_do_not_hide_account_or_models() {
        let (client, server) = duplex(4096);
        let (client_read, mut client_write) = tokio::io::split(client);
        let (server_read, mut server_write) = tokio::io::split(server);
        let task = tokio::spawn(async move {
            let mut reader = BufReader::new(server_read);
            for response in [
                Some(json!({"id":1,"result":{}})),
                None,
                Some(json!({"id":2,"result":{"account":{"type":"chatgpt","planType":"pro"}}})),
                Some(json!({"id":3,"error":{"code":-32000,"message":"unavailable"}})),
                Some(
                    json!({"id":4,"result":{"data":[{"model":"a","displayName":"A","isDefault":true}]}}),
                ),
            ] {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                if let Some(value) = response {
                    server_write
                        .write_all(format!("{value}\n").as_bytes())
                        .await
                        .unwrap();
                }
            }
        });
        let snapshot = read_snapshot(&mut BufReader::new(client_read), &mut client_write)
            .await
            .unwrap();
        assert!(matches!(snapshot.account, Account::ChatGpt { .. }));
        assert_eq!(snapshot.limits, None);
        assert_eq!(snapshot.models[0].id, "a");
        task.await.unwrap();
    }

    #[tokio::test]
    async fn handshake_discovers_multiple_model_pages() {
        let (client, server) = duplex(4096);
        let (client_read, mut client_write) = tokio::io::split(client);
        let (server_read, mut server_write) = tokio::io::split(server);
        let task = tokio::spawn(async move {
            let mut reader = BufReader::new(server_read);
            for (expected, result) in [
                ("initialize", Some(json!({}))),
                ("initialized", None),
                (
                    "account/read",
                    Some(json!({"account":{"type":"chatgpt","planType":"pro"}})),
                ),
                (
                    "account/rateLimits/read",
                    Some(json!({"rateLimits":{"primary":{"usedPercent":42}}})),
                ),
                (
                    "model/list",
                    Some(
                        json!({"data":[{"model":"a","displayName":"A","isDefault":true}],"nextCursor":"next"}),
                    ),
                ),
                (
                    "model/list",
                    Some(
                        json!({"data":[{"model":"b","displayName":"B","isDefault":false}],"nextCursor":null}),
                    ),
                ),
            ] {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let request: Value = serde_json::from_str(&line).unwrap();
                assert_eq!(request["method"], expected);
                if let Some(result) = result {
                    if request["id"] == 5 {
                        assert_eq!(request["params"]["cursor"], "next");
                    }
                    let response = json!({"id":request["id"],"result":result});
                    server_write
                        .write_all(format!("{response}\n").as_bytes())
                        .await
                        .unwrap();
                }
            }
        });
        let snapshot = read_snapshot(&mut BufReader::new(client_read), &mut client_write)
            .await
            .unwrap();
        assert!(matches!(snapshot.account, Account::ChatGpt { .. }));
        assert_eq!(
            snapshot.limits.unwrap()[0]
                .primary
                .as_ref()
                .unwrap()
                .used_percent,
            42
        );
        assert_eq!(
            snapshot
                .models
                .iter()
                .map(|model| model.id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        task.await.unwrap();
    }

    #[tokio::test]
    async fn signed_out_does_not_request_private_account_data() {
        let (client, server) = duplex(2048);
        let (client_read, mut client_write) = tokio::io::split(client);
        let (server_read, mut server_write) = tokio::io::split(server);
        let task = tokio::spawn(async move {
            let mut reader = BufReader::new(server_read);
            for (method, response) in [
                ("initialize", Some(json!({"id":1,"result":{}}))),
                ("initialized", None),
                (
                    "account/read",
                    Some(json!({"id":2,"result":{"account":null}})),
                ),
            ] {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let request: Value = serde_json::from_str(&line).unwrap();
                assert_eq!(request["method"], method);
                if let Some(value) = response {
                    server_write
                        .write_all(format!("{value}\n").as_bytes())
                        .await
                        .unwrap();
                }
            }
            let mut line = String::new();
            assert_eq!(reader.read_line(&mut line).await.unwrap(), 0);
        });
        let snapshot = read_snapshot(&mut BufReader::new(client_read), &mut client_write)
            .await
            .unwrap();
        assert_eq!(snapshot.account, Account::SignedOut);
        assert_eq!(snapshot.limits, None);
        assert!(snapshot.models.is_empty());
        drop(client_write);
        task.await.unwrap();
    }
}
