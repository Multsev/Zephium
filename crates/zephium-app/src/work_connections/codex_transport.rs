//! Bounded request/response framing. Process creation and access policy live elsewhere.
use super::codex_protocol::{ProtocolError, Request, MAX_MESSAGE_BYTES};
use serde_json::Value;
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

const MAX_INTERLEAVED: usize = 64;
const MAX_PENDING_BYTES: usize = 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const EVENT_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, PartialEq, Eq)]
pub enum TransportError {
    Io,
    Timeout,
    Closed,
    Protocol(ProtocolError),
}

pub enum ChatRequest<'a> {
    StartThread { model: &'a str },
    ResumeThread { thread: &'a str },
    StartTurn { thread: &'a str, prompt: &'a str },
    InterruptTurn { thread: &'a str, turn: &'a str },
}

pub struct ChatExchange {
    pub response: Vec<u8>,
    pub notifications: Vec<Vec<u8>>,
}

/// Read the next event for one active turn. Dropping this future cancels the
/// read, but the owner must then close the stream rather than reuse it.
pub async fn next_chat_event<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    thread: &str,
    turn: &str,
) -> Result<super::codex_chat_protocol::TurnEvent, TransportError> {
    use super::codex_chat_protocol::{event, TurnEvent};
    tokio::time::timeout(EVENT_TIMEOUT, async {
        for _ in 0..MAX_INTERLEAVED {
            let line = read_line(reader).await?;
            match event(&line, thread, turn).map_err(TransportError::Protocol)? {
                TurnEvent::Other => continue,
                item => return Ok(item),
            }
        }
        Err(TransportError::Protocol(ProtocolError::TooLarge))
    })
    .await
    .map_err(|_| TransportError::Timeout)?
}

/// Preserve bounded notifications which arrive before a chat response. Any
/// server-initiated request fails closed until an approval handler owns it.
pub async fn chat_request<R, W>(
    reader: &mut R,
    writer: &mut W,
    operation: ChatRequest<'_>,
    id: u64,
) -> Result<ChatExchange, TransportError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    use super::codex_chat_protocol as chat;
    let line = match operation {
        ChatRequest::StartThread { model } => chat::start_thread(id, model),
        ChatRequest::ResumeThread { thread } => chat::resume_thread(id, thread),
        ChatRequest::StartTurn { thread, prompt } => chat::start_turn(id, thread, prompt),
        ChatRequest::InterruptTurn { thread, turn } => chat::interrupt_turn(id, thread, turn),
    }
    .map_err(TransportError::Protocol)?;
    exchange(reader, writer, line, id, true).await
}

pub async fn initialized<W: AsyncWrite + Unpin>(writer: &mut W) -> Result<(), TransportError> {
    tokio::time::timeout(REQUEST_TIMEOUT, async {
        writer
            .write_all(Request::Initialized.encode(0).as_bytes())
            .await
            .map_err(|_| TransportError::Io)?;
        writer.flush().await.map_err(|_| TransportError::Io)
    })
    .await
    .map_err(|_| TransportError::Timeout)?
}

/// Read exactly one bounded JSON line without ever allocating past the bound.
async fn read_line<R: AsyncBufRead + Unpin>(reader: &mut R) -> Result<Vec<u8>, TransportError> {
    let mut line = Vec::new();
    loop {
        let bytes = reader.fill_buf().await.map_err(|_| TransportError::Io)?;
        if bytes.is_empty() {
            return Err(TransportError::Closed);
        }
        let length = bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(bytes.len(), |n| n + 1);
        if line.len() + length > MAX_MESSAGE_BYTES {
            return Err(TransportError::Protocol(ProtocolError::TooLarge));
        }
        let finished = bytes[length - 1] == b'\n';
        line.extend_from_slice(&bytes[..length]);
        reader.consume(length);
        if finished {
            return Ok(line);
        }
    }
}

/// Issue a fixed read-only request. Unknown notifications are discarded, and
/// server-initiated requests fail closed instead of granting an operation.
pub async fn request<R, W>(
    reader: &mut R,
    writer: &mut W,
    operation: Request,
    id: u64,
) -> Result<Vec<u8>, TransportError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    if operation == Request::Initialized {
        return Err(TransportError::Protocol(ProtocolError::Unexpected));
    }
    request_line(reader, writer, operation.encode(id), id).await
}

pub async fn models_next_page<R, W>(
    reader: &mut R,
    writer: &mut W,
    id: u64,
    cursor: &str,
) -> Result<Vec<u8>, TransportError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let line =
        super::codex_protocol::models_next_page(id, cursor).map_err(TransportError::Protocol)?;
    request_line(reader, writer, line, id).await
}

async fn request_line<R, W>(
    reader: &mut R,
    writer: &mut W,
    line: String,
    id: u64,
) -> Result<Vec<u8>, TransportError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    Ok(exchange(reader, writer, line, id, false).await?.response)
}

async fn exchange<R, W>(
    reader: &mut R,
    writer: &mut W,
    line: String,
    id: u64,
    retain: bool,
) -> Result<ChatExchange, TransportError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    tokio::time::timeout(REQUEST_TIMEOUT, async {
        writer
            .write_all(line.as_bytes())
            .await
            .map_err(|_| TransportError::Io)?;
        writer.flush().await.map_err(|_| TransportError::Io)?;
        let mut notifications = Vec::new();
        let mut total = 0usize;
        for _ in 0..MAX_INTERLEAVED {
            let line = read_line(reader).await?;
            let value: Value = serde_json::from_slice(&line)
                .map_err(|_| TransportError::Protocol(ProtocolError::Malformed))?;
            if !value.is_object() {
                return Err(TransportError::Protocol(ProtocolError::Malformed));
            }
            if value.get("method").is_some() {
                if value.get("id").is_some() {
                    return Err(TransportError::Protocol(ProtocolError::Unexpected));
                }
                if retain {
                    total = total
                        .checked_add(line.len())
                        .ok_or(TransportError::Protocol(ProtocolError::TooLarge))?;
                    if total > MAX_PENDING_BYTES {
                        return Err(TransportError::Protocol(ProtocolError::TooLarge));
                    }
                    notifications.push(line);
                }
                continue;
            }
            if value.get("id").and_then(Value::as_u64) != Some(id) {
                return Err(TransportError::Protocol(ProtocolError::Unexpected));
            }
            return Ok(ChatExchange {
                response: line,
                notifications,
            });
        }
        Err(TransportError::Protocol(ProtocolError::TooLarge))
    })
    .await
    .map_err(|_| TransportError::Timeout)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncReadExt, BufReader};

    #[tokio::test]
    async fn ignores_notifications_and_returns_matching_reply() {
        let (client, mut server) = duplex(2048);
        let (read, mut write) = tokio::io::split(client);
        let task = tokio::spawn(async move {
            let mut input = [0u8; 512];
            let count = server.read(&mut input).await.unwrap();
            assert!(std::str::from_utf8(&input[..count])
                .unwrap()
                .contains("account/read"));
            server.write_all(b"{\"method\":\"account/updated\",\"params\":{}}\n{\"id\":2,\"result\":{\"account\":null}}\n").await.unwrap();
        });
        let result = request(&mut BufReader::new(read), &mut write, Request::Account, 2)
            .await
            .unwrap();
        assert!(matches!(
            super::super::codex_protocol::account_response(&result, 2),
            Ok(super::super::codex_protocol::Account::SignedOut)
        ));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_server_commands_and_oversized_messages() {
        let (client, mut server) = duplex(1024);
        let (read, mut write) = tokio::io::split(client);
        let task = tokio::spawn(async move {
            let mut input = [0u8; 512];
            let _ = server.read(&mut input).await.unwrap();
            server
                .write_all(b"{\"method\":\"run\",\"id\":7}\n")
                .await
                .unwrap();
        });
        assert_eq!(
            request(&mut BufReader::new(read), &mut write, Request::Account, 2).await,
            Err(TransportError::Protocol(ProtocolError::Unexpected))
        );
        task.await.unwrap();
        let oversized_bytes = vec![b'a'; MAX_MESSAGE_BYTES + 1];
        let mut oversized = BufReader::new(&oversized_bytes[..]);
        assert_eq!(
            read_line(&mut oversized).await,
            Err(TransportError::Protocol(ProtocolError::TooLarge))
        );
    }

    #[tokio::test]
    async fn chat_preserves_notifications_arriving_before_response() {
        let (client, mut server) = duplex(2048);
        let (read, mut write) = tokio::io::split(client);
        let task = tokio::spawn(async move {
            let mut input = [0u8; 1024];
            let _ = server.read(&mut input).await.unwrap();
            server.write_all(b"{\"method\":\"item/agentMessage/delta\",\"params\":{\"threadId\":\"t\",\"turnId\":\"u\",\"delta\":\"Hi\"}}\n{\"id\":2,\"result\":{\"turn\":{\"id\":\"u\"}}}\n{\"method\":\"turn/completed\",\"params\":{\"threadId\":\"t\",\"turn\":{\"id\":\"u\",\"status\":\"completed\"}}}\n").await.unwrap();
        });
        let mut reader = BufReader::new(read);
        let result = chat_request(
            &mut reader,
            &mut write,
            ChatRequest::StartTurn {
                thread: "t",
                prompt: "Hello",
            },
            2,
        )
        .await
        .unwrap();
        let turn = super::super::codex_chat_protocol::turn_response(&result.response, 2).unwrap();
        assert_eq!(
            super::super::codex_chat_protocol::event(&result.notifications[0], "t", &turn),
            Ok(super::super::codex_chat_protocol::TurnEvent::Delta(
                "Hi".into()
            ))
        );
        assert_eq!(
            next_chat_event(&mut reader, "t", &turn).await,
            Ok(super::super::codex_chat_protocol::TurnEvent::Completed)
        );
        task.await.unwrap();
    }
}
