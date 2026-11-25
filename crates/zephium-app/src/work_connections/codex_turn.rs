//! One bounded Codex turn over already admitted streams.
//! The owner must close the connection on error or when reuse is false.
use super::codex_approval::{self as approval, Answer, Decision};
use super::codex_chat_protocol::{self as chat, TurnEvent};
use super::codex_protocol::{self, ProtocolError};
use super::codex_transport::{self as transport, ChatRequest, TransportError};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, watch};

const START_ID: u64 = 101;
const INTERRUPT_ID: u64 = 102;
const MAX_EVENTS: usize = 4096;
const MAX_OUTPUT_BYTES: usize = 8 * 1024 * 1024;
const TURN_DEADLINE: Duration = Duration::from_secs(600);
static APPROVAL_NONCE: AtomicU64 = AtomicU64::new(1);

fn next_approval_nonce() -> Result<u64, TurnError> {
    APPROVAL_NONCE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| TurnError::TooManyEvents)
}

#[derive(Debug, PartialEq, Eq)]
pub enum TurnError {
    Transport(TransportError),
    Protocol(ProtocolError),
    ConsumerGone,
    Deadline,
    TooManyEvents,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Outcome {
    pub status: TurnEvent,
    /// False if an interrupt response may still be in flight.
    pub reusable: bool,
}

fn deliver(
    event: TurnEvent,
    sender: &mpsc::Sender<TurnEvent>,
    output_bytes: &mut usize,
) -> Result<Option<TurnEvent>, TurnError> {
    if event == TurnEvent::Other {
        return Ok(None);
    }
    if let TurnEvent::Delta(text) = &event {
        *output_bytes = output_bytes
            .checked_add(text.len())
            .ok_or(TurnError::TooManyEvents)?;
        if *output_bytes > MAX_OUTPUT_BYTES {
            return Err(TurnError::TooManyEvents);
        }
    }
    let terminal = matches!(
        event,
        TurnEvent::Completed | TurnEvent::Interrupted | TurnEvent::Failed
    );
    sender
        .try_send(event.clone())
        .map_err(|_| TurnError::ConsumerGone)?;
    Ok(terminal.then_some(event))
}

/// Send a user message and deliver bounded events. `cancel` requests a
/// protocol interrupt; it does not claim the server has stopped until a
/// terminal event is received. Server-initiated approvals fail closed.
pub async fn stream_turn<R, W>(
    reader: &mut R,
    writer: &mut W,
    thread: &str,
    prompt: &str,
    sender: &mpsc::Sender<TurnEvent>,
    cancel: &mut watch::Receiver<bool>,
) -> Result<Outcome, TurnError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    tokio::time::timeout(
        TURN_DEADLINE,
        stream_inner(reader, writer, thread, prompt, sender, cancel, None),
    )
    .await
    .map_err(|_| TurnError::Deadline)?
}

/// Like `stream_turn`, but routes supported command requests to a user-facing
/// approval owner. Answers apply to one request only.
pub async fn stream_turn_with_approvals<R, W>(
    reader: &mut R,
    writer: &mut W,
    thread: &str,
    prompt: &str,
    sender: &mpsc::Sender<TurnEvent>,
    cancel: &mut watch::Receiver<bool>,
    answers: &mut mpsc::Receiver<Answer>,
) -> Result<Outcome, TurnError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    tokio::time::timeout(
        TURN_DEADLINE,
        stream_inner(
            reader,
            writer,
            thread,
            prompt,
            sender,
            cancel,
            Some(answers),
        ),
    )
    .await
    .map_err(|_| TurnError::Deadline)?
}

async fn stream_inner<R, W>(
    reader: &mut R,
    writer: &mut W,
    thread: &str,
    prompt: &str,
    sender: &mpsc::Sender<TurnEvent>,
    cancel: &mut watch::Receiver<bool>,
    mut answers: Option<&mut mpsc::Receiver<Answer>>,
) -> Result<Outcome, TurnError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let exchange = transport::chat_request(
        reader,
        writer,
        ChatRequest::StartTurn { thread, prompt },
        START_ID,
    )
    .await
    .map_err(TurnError::Transport)?;
    let turn = chat::turn_response(&exchange.response, START_ID).map_err(TurnError::Protocol)?;
    let mut count = 0;
    let mut output_bytes = 0;
    for line in exchange.notifications {
        let event = chat::event(&line, thread, &turn).map_err(TurnError::Protocol)?;
        count += 1;
        if let Some(status) = deliver(event, sender, &mut output_bytes)? {
            return Ok(Outcome {
                status,
                reusable: true,
            });
        }
    }
    let mut interrupted = false;
    let mut interrupt_pending = false;
    loop {
        let cancel_requested = *cancel.borrow_and_update();
        if !interrupted && cancel_requested {
            let request =
                chat::interrupt_turn(INTERRUPT_ID, thread, &turn).map_err(TurnError::Protocol)?;
            writer
                .write_all(request.as_bytes())
                .await
                .map_err(|_| TurnError::Transport(TransportError::Io))?;
            writer
                .flush()
                .await
                .map_err(|_| TurnError::Transport(TransportError::Io))?;
            interrupted = true;
            interrupt_pending = true;
        }
        // Keep the same read future alive when cancellation arrives; dropping a
        // partially read line would desynchronise the connection.
        let line = transport::read_line(reader);
        tokio::pin!(line);
        let bytes = loop {
            tokio::select! {
                result = &mut line => break result.map_err(TurnError::Transport)?,
                changed = cancel.changed(), if !interrupted => {
                    if changed.is_err() { return Err(TurnError::ConsumerGone); }
                    let cancel_requested = *cancel.borrow_and_update();
                    if cancel_requested {
                        let request = chat::interrupt_turn(INTERRUPT_ID, thread, &turn).map_err(TurnError::Protocol)?;
                        writer.write_all(request.as_bytes()).await.map_err(|_| TurnError::Transport(TransportError::Io))?;
                        writer.flush().await.map_err(|_| TurnError::Transport(TransportError::Io))?;
                        interrupted = true;
                        interrupt_pending = true;
                    }
                }
            }
        };
        count += 1;
        if count > MAX_EVENTS {
            return Err(TurnError::TooManyEvents);
        }
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|_| TurnError::Protocol(ProtocolError::Malformed))?;
        if value.get("method").is_none() {
            if !interrupt_pending {
                return Err(TurnError::Protocol(ProtocolError::Unexpected));
            }
            codex_protocol::response(&bytes, INTERRUPT_ID).map_err(TurnError::Protocol)?;
            interrupt_pending = false;
            continue;
        }
        if value.get("id").is_some() {
            if interrupted {
                return Err(TurnError::Protocol(ProtocolError::Unexpected));
            }
            let answers = answers
                .as_deref_mut()
                .ok_or(TurnError::Protocol(ProtocolError::Unexpected))?;
            let approval = approval::parse(&bytes, thread, &turn, next_approval_nonce()?)
                .map_err(TurnError::Protocol)?;
            deliver(
                TurnEvent::ApprovalRequested(approval.clone()),
                sender,
                &mut output_bytes,
            )?;
            let mut answer = if *cancel.borrow_and_update() {
                Answer {
                    nonce: approval.nonce,
                    decision: Decision::Cancel,
                }
            } else {
                tokio::time::timeout(Duration::from_secs(120), async {
                    tokio::select! {
                        answer = answers.recv() => answer.ok_or(TurnError::ConsumerGone),
                        changed = cancel.changed() => {
                            if changed.is_err() || !*cancel.borrow_and_update() { return Err(TurnError::ConsumerGone); }
                            Ok(Answer { nonce: approval.nonce, decision: Decision::Cancel })
                        }
                    }
                }).await.map_err(|_| TurnError::Deadline)??
            };
            if *cancel.borrow_and_update() {
                answer = Answer {
                    nonce: approval.nonce,
                    decision: Decision::Cancel,
                };
            }
            let response = approval::response(&approval, answer).map_err(TurnError::Protocol)?;
            writer
                .write_all(response.as_bytes())
                .await
                .map_err(|_| TurnError::Transport(TransportError::Io))?;
            writer
                .flush()
                .await
                .map_err(|_| TurnError::Transport(TransportError::Io))?;
            if answer.decision == Decision::Cancel {
                interrupted = true;
            }
            continue;
        }
        let event = chat::event(&bytes, thread, &turn).map_err(TurnError::Protocol)?;
        if let Some(status) = deliver(event, sender, &mut output_bytes)? {
            return Ok(Outcome {
                status,
                reusable: !interrupt_pending,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncBufReadExt, BufReader};
    use tokio::sync::oneshot;

    #[test]
    fn output_limit_is_enforced_before_delivery() {
        let (events, mut received) = mpsc::channel(1);
        let mut size = MAX_OUTPUT_BYTES;
        assert_eq!(
            deliver(TurnEvent::Delta("x".into()), &events, &mut size),
            Err(TurnError::TooManyEvents)
        );
        assert!(received.try_recv().is_err());
    }

    #[tokio::test]
    async fn supported_approval_waits_for_a_matching_one_time_answer() {
        let (client, server) = duplex(4096);
        let (client_read, mut client_write) = tokio::io::split(client);
        let (server_read, mut server_write) = tokio::io::split(server);
        let server_task = tokio::spawn(async move {
            let mut reader = BufReader::new(server_read);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            server_write.write_all(b"{\"id\":101,\"result\":{\"turn\":{\"id\":\"u\"}}}\n{\"id\":7,\"method\":\"item/commandExecution/requestApproval\",\"params\":{\"threadId\":\"t\",\"turnId\":\"u\",\"command\":\"ls -la\",\"cwd\":\"/tmp\"}}\n").await.unwrap();
            line.clear();
            reader.read_line(&mut line).await.unwrap();
            let response: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(response["id"], 7);
            assert_eq!(response["result"]["decision"], "accept");
            server_write.write_all(b"{\"method\":\"turn/completed\",\"params\":{\"threadId\":\"t\",\"turn\":{\"id\":\"u\",\"status\":\"completed\"}}}\n").await.unwrap();
        });
        let (events, mut received) = mpsc::channel(8);
        let (answer_tx, mut answers) = mpsc::channel(1);
        let (_cancel, mut cancellation) = watch::channel(false);
        let turn = tokio::spawn(async move {
            stream_turn_with_approvals(
                &mut BufReader::new(client_read),
                &mut client_write,
                "t",
                "hello",
                &events,
                &mut cancellation,
                &mut answers,
            )
            .await
        });
        let Some(TurnEvent::ApprovalRequested(preview)) = received.recv().await else {
            panic!("missing approval preview")
        };
        assert_eq!(preview.command, "ls -la");
        answer_tx
            .send(Answer {
                nonce: preview.nonce,
                decision: Decision::AcceptOnce,
            })
            .await
            .unwrap();
        assert_eq!(received.recv().await, Some(TurnEvent::Completed));
        assert_eq!(
            turn.await.unwrap(),
            Ok(Outcome {
                status: TurnEvent::Completed,
                reusable: true
            })
        );
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_during_approval_never_accepts_the_command() {
        let (client, server) = duplex(4096);
        let (client_read, mut client_write) = tokio::io::split(client);
        let (server_read, mut server_write) = tokio::io::split(server);
        let server_task = tokio::spawn(async move {
            let mut reader = BufReader::new(server_read);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            server_write.write_all(b"{\"id\":101,\"result\":{\"turn\":{\"id\":\"u\"}}}\n{\"id\":7,\"method\":\"item/commandExecution/requestApproval\",\"params\":{\"threadId\":\"t\",\"turnId\":\"u\",\"command\":\"ls\"}}\n").await.unwrap();
            line.clear();
            reader.read_line(&mut line).await.unwrap();
            let response: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(response["result"]["decision"], "cancel");
            server_write.write_all(b"{\"method\":\"turn/completed\",\"params\":{\"threadId\":\"t\",\"turn\":{\"id\":\"u\",\"status\":\"interrupted\"}}}\n").await.unwrap();
        });
        let (events, mut received) = mpsc::channel(8);
        let (_answer_tx, mut answers) = mpsc::channel(1);
        let (cancel, mut cancellation) = watch::channel(false);
        let turn = tokio::spawn(async move {
            stream_turn_with_approvals(
                &mut BufReader::new(client_read),
                &mut client_write,
                "t",
                "hello",
                &events,
                &mut cancellation,
                &mut answers,
            )
            .await
        });
        assert!(matches!(
            received.recv().await,
            Some(TurnEvent::ApprovalRequested(_))
        ));
        cancel.send(true).unwrap();
        assert_eq!(received.recv().await, Some(TurnEvent::Interrupted));
        assert_eq!(
            turn.await.unwrap(),
            Ok(Outcome {
                status: TurnEvent::Interrupted,
                reusable: true
            })
        );
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn interrupt_keeps_a_partially_read_notification() {
        let (client, server) = duplex(4096);
        let (client_read, mut client_write) = tokio::io::split(client);
        let (server_read, mut server_write) = tokio::io::split(server);
        let (partial_sent, partial_seen) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            let mut reader = BufReader::new(server_read);
            let mut request = String::new();
            reader.read_line(&mut request).await.unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&request).unwrap()["method"],
                "turn/start"
            );
            server_write.write_all(b"{\"id\":101,\"result\":{\"turn\":{\"id\":\"u\"}}}\n{\"method\":\"item/agentMessage/delta\",\"params\":{").await.unwrap();
            partial_sent.send(()).unwrap();
            request.clear();
            reader.read_line(&mut request).await.unwrap();
            let interrupt: Value = serde_json::from_str(&request).unwrap();
            assert_eq!(interrupt["method"], "turn/interrupt");
            assert_eq!(interrupt["params"]["turnId"], "u");
            server_write.write_all(b"\"threadId\":\"t\",\"turnId\":\"u\",\"delta\":\"Hi\"}}\n{\"id\":102,\"result\":{}}\n{\"method\":\"turn/completed\",\"params\":{\"threadId\":\"t\",\"turn\":{\"id\":\"u\",\"status\":\"interrupted\"}}}\n").await.unwrap();
        });
        let (events, mut received) = mpsc::channel(8);
        let (cancel, mut cancellation) = watch::channel(false);
        let turn = tokio::spawn(async move {
            stream_turn(
                &mut BufReader::new(client_read),
                &mut client_write,
                "t",
                "hello",
                &events,
                &mut cancellation,
            )
            .await
        });
        partial_seen.await.unwrap();
        cancel.send(true).unwrap();
        assert_eq!(received.recv().await, Some(TurnEvent::Delta("Hi".into())));
        assert_eq!(received.recv().await, Some(TurnEvent::Interrupted));
        assert_eq!(
            turn.await.unwrap(),
            Ok(Outcome {
                status: TurnEvent::Interrupted,
                reusable: true
            })
        );
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn completion_before_interrupt_ack_requires_connection_close() {
        let (client, server) = duplex(4096);
        let (client_read, mut client_write) = tokio::io::split(client);
        let (server_read, mut server_write) = tokio::io::split(server);
        let server_task = tokio::spawn(async move {
            let mut reader = BufReader::new(server_read);
            let mut request = String::new();
            reader.read_line(&mut request).await.unwrap();
            server_write
                .write_all(b"{\"id\":101,\"result\":{\"turn\":{\"id\":\"u\"}}}\n")
                .await
                .unwrap();
            request.clear();
            reader.read_line(&mut request).await.unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&request).unwrap()["method"],
                "turn/interrupt"
            );
            server_write.write_all(b"{\"method\":\"turn/completed\",\"params\":{\"threadId\":\"t\",\"turn\":{\"id\":\"u\",\"status\":\"completed\"}}}\n").await.unwrap();
        });
        let (events, _received) = mpsc::channel(8);
        let (_cancel, mut cancellation) = watch::channel(true);
        let result = stream_turn(
            &mut BufReader::new(client_read),
            &mut client_write,
            "t",
            "hello",
            &events,
            &mut cancellation,
        )
        .await;
        assert_eq!(
            result,
            Ok(Outcome {
                status: TurnEvent::Completed,
                reusable: false
            })
        );
        server_task.await.unwrap();
    }
}
