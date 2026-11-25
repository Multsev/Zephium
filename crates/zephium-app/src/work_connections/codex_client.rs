//! Stateful Codex session over streams supplied by an admitted process owner.
//! This does not start the process or provide isolation on its own.
use super::codex_chat_protocol::{self as chat, TurnEvent};
use super::codex_protocol::Account;
use super::codex_session::{self, SessionError, Snapshot};
use super::codex_transport::{self as transport, ChatRequest, TransportError};
use super::codex_turn::{self, TurnError};
use tokio::io::{AsyncBufRead, AsyncWrite};
use tokio::sync::{mpsc, watch};

#[derive(Debug, PartialEq, Eq)]
pub enum ClientError {
    Discovery(SessionError),
    Transport(TransportError),
    Protocol(super::codex_protocol::ProtocolError),
    Turn(TurnError),
    SignedOut,
    UnsupportedAccount,
    UnknownModel,
    NoThread,
    Closed,
}

pub struct Client<R, W> {
    reader: R,
    writer: W,
    snapshot: Snapshot,
    thread: Option<String>,
    usable: bool,
}

impl<R: AsyncBufRead + Unpin, W: AsyncWrite + Unpin> Client<R, W> {
    pub async fn connect(mut reader: R, mut writer: W) -> Result<Self, ClientError> {
        let snapshot = codex_session::read_snapshot(&mut reader, &mut writer)
            .await
            .map_err(ClientError::Discovery)?;
        Ok(Self {
            reader,
            writer,
            snapshot,
            thread: None,
            usable: true,
        })
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }
    pub fn current_thread(&self) -> Option<&str> {
        self.thread.as_deref()
    }
    pub fn is_usable(&self) -> bool {
        self.usable
    }

    /// Refresh limits without restarting a conversation. An unavailable
    /// endpoint is represented as unknown, never as zero usage.
    pub async fn refresh_limits(
        &mut self,
    ) -> Result<Option<&[super::codex_protocol::Limit]>, ClientError> {
        self.ready()?;
        self.usable = false;
        let line = transport::request(
            &mut self.reader,
            &mut self.writer,
            super::codex_protocol::Request::Limits,
            30,
        )
        .await
        .map_err(ClientError::Transport)?;
        self.snapshot.limits = match super::codex_protocol::limits_response(&line, 30) {
            Ok(limits) => Some(limits),
            Err(super::codex_protocol::ProtocolError::Remote) => None,
            Err(error) => return Err(ClientError::Protocol(error)),
        };
        self.usable = true;
        Ok(self.snapshot.limits.as_deref())
    }

    fn ready(&self) -> Result<(), ClientError> {
        if !self.usable {
            return Err(ClientError::Closed);
        }
        match self.snapshot.account {
            Account::ChatGpt { .. } => {}
            Account::SignedOut => return Err(ClientError::SignedOut),
            _ => return Err(ClientError::UnsupportedAccount),
        }
        Ok(())
    }

    /// Only catalog models announced by the same authenticated server can be selected.
    pub async fn start_thread(&mut self, model: &str) -> Result<&str, ClientError> {
        self.ready()?;
        if !self.snapshot.models.iter().any(|item| item.id == model) {
            return Err(ClientError::UnknownModel);
        }
        self.usable = false;
        let exchange = transport::chat_request(
            &mut self.reader,
            &mut self.writer,
            ChatRequest::StartThread { model },
            20,
        )
        .await
        .map_err(ClientError::Transport)?;
        let thread =
            chat::thread_response(&exchange.response, 20).map_err(ClientError::Protocol)?;
        let response = super::codex_protocol::response(&exchange.response, 20)
            .map_err(ClientError::Protocol)?;
        if response.get("model").and_then(serde_json::Value::as_str) != Some(model) {
            return Err(ClientError::Protocol(
                super::codex_protocol::ProtocolError::Unexpected,
            ));
        }
        self.thread = Some(thread);
        self.usable = true;
        Ok(self.thread.as_deref().expect("thread just assigned"))
    }

    pub async fn resume_thread(&mut self, thread: &str) -> Result<&str, ClientError> {
        self.ready()?;
        self.usable = false;
        let exchange = transport::chat_request(
            &mut self.reader,
            &mut self.writer,
            ChatRequest::ResumeThread { thread },
            21,
        )
        .await
        .map_err(ClientError::Transport)?;
        let resumed =
            chat::thread_response(&exchange.response, 21).map_err(ClientError::Protocol)?;
        if resumed != thread {
            return Err(ClientError::Protocol(
                super::codex_protocol::ProtocolError::Unexpected,
            ));
        }
        self.thread = Some(resumed);
        self.usable = true;
        Ok(self.thread.as_deref().expect("thread just assigned"))
    }

    pub async fn turn(
        &mut self,
        prompt: &str,
        events: &mpsc::Sender<TurnEvent>,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<TurnEvent, ClientError> {
        self.ready()?;
        let thread = self.thread.as_deref().ok_or(ClientError::NoThread)?;
        self.usable = false;
        let result = codex_turn::stream_turn(
            &mut self.reader,
            &mut self.writer,
            thread,
            prompt,
            events,
            cancel,
        )
        .await
        .map_err(ClientError::Turn)?;
        self.usable = result.reusable;
        Ok(result.status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use tokio::io::{duplex, AsyncBufReadExt, AsyncWriteExt, BufReader};

    #[tokio::test]
    async fn cancelled_request_cannot_reuse_an_ambiguous_connection() {
        let (client_stream, _server) = duplex(2048);
        let (reader, writer) = tokio::io::split(client_stream);
        let mut client = Client {
            reader: BufReader::new(reader),
            writer,
            snapshot: Snapshot {
                account: Account::ChatGpt {
                    email: None,
                    plan: "pro".into(),
                },
                limits: None,
                models: vec![super::super::codex_protocol::Model {
                    id: "a".into(),
                    name: "A".into(),
                    is_default: true,
                }],
            },
            thread: None,
            usable: true,
        };
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(20),
            client.start_thread("a")
        )
        .await
        .is_err());
        assert!(!client.is_usable());
        assert_eq!(client.start_thread("a").await, Err(ClientError::Closed));
    }

    #[tokio::test]
    async fn connects_selects_server_model_and_streams_a_turn() {
        let (client, server) = duplex(8192);
        let (reader, writer) = tokio::io::split(client);
        let (server_read, mut server_write) = tokio::io::split(server);
        let server_task = tokio::spawn(async move {
            let mut reader = BufReader::new(server_read);
            for (method, result) in [
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
                        json!({"data":[{"model":"model-a","displayName":"Model A","isDefault":true}]}),
                    ),
                ),
                (
                    "thread/start",
                    Some(
                        json!({"thread":{"id":"t"},"model":"model-a","sandbox":{"type":"readOnly"},"approvalPolicy":"on-request","approvalsReviewer":"user"}),
                    ),
                ),
                ("turn/start", Some(json!({"turn":{"id":"u"}}))),
            ] {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let request: Value = serde_json::from_str(&line).unwrap();
                assert_eq!(request["method"], method);
                if method == "thread/start" {
                    assert_eq!(request["params"]["model"], "model-a");
                }
                if let Some(result) = result {
                    server_write
                        .write_all(
                            format!("{}\n", json!({"id":request["id"],"result":result})).as_bytes(),
                        )
                        .await
                        .unwrap();
                }
            }
            server_write.write_all(b"{\"method\":\"item/agentMessage/delta\",\"params\":{\"threadId\":\"t\",\"turnId\":\"u\",\"delta\":\"Hi\"}}\n{\"method\":\"turn/completed\",\"params\":{\"threadId\":\"t\",\"turn\":{\"id\":\"u\",\"status\":\"completed\"}}}\n").await.unwrap();
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(request["method"], "account/rateLimits/read");
            server_write
                .write_all(
                    b"{\"id\":30,\"result\":{\"rateLimits\":{\"primary\":{\"usedPercent\":43}}}}\n",
                )
                .await
                .unwrap();
        });
        let mut client = Client::connect(BufReader::new(reader), writer)
            .await
            .unwrap();
        assert!(matches!(client.snapshot().account, Account::ChatGpt { .. }));
        assert_eq!(
            client.start_thread("unannounced").await,
            Err(ClientError::UnknownModel)
        );
        assert_eq!(client.start_thread("model-a").await.unwrap(), "t");
        let (events, mut receiver) = mpsc::channel(8);
        let (_cancel, mut cancellation) = watch::channel(false);
        assert_eq!(
            client.turn("hello", &events, &mut cancellation).await,
            Ok(TurnEvent::Completed)
        );
        assert_eq!(receiver.recv().await, Some(TurnEvent::Delta("Hi".into())));
        assert_eq!(receiver.recv().await, Some(TurnEvent::Completed));
        assert!(client.is_usable());
        assert_eq!(
            client.refresh_limits().await.unwrap().unwrap()[0]
                .primary
                .as_ref()
                .unwrap()
                .used_percent,
            43
        );
        server_task.await.unwrap();
    }
}
