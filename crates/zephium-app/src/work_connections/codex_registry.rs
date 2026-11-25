//! Profile-scoped ownership of admitted Codex sessions.
//! Attaching streams is a native-host capability, never a page or IPC input.
use super::codex_approval::Answer;
use super::codex_chat_protocol::TurnEvent;
use super::codex_client::{Client, ClientError};
use super::codex_session::Snapshot;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex};
use zephium_core::ids::ProfileId;

const MAX_PROFILES: usize = 16;
const MAX_THREAD_OWNERS: usize = 64;
const MAX_THREADS: usize = 512;
type Input = BufReader<Box<dyn AsyncRead + Send + Unpin>>;
type Output = Box<dyn AsyncWrite + Send + Unpin>;
type Session = Client<Input, Output>;

#[derive(Debug, PartialEq, Eq)]
pub enum RegistryError {
    Busy,
    Missing,
    Revoked,
    Limit,
    ConsumerGone,
    UnknownThread,
    Client(ClientError),
    Internal,
}

struct Entry {
    client: AsyncMutex<Session>,
    snapshot: Mutex<Snapshot>,
    account: AccountScope,
    revoked: watch::Sender<bool>,
    // A qualified runtime owner keeps its child and sandbox alive here.
    _lease: Box<dyn Send + Sync>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum AccountScope {
    Email(String),
    /// An account without a usable identity can resume only in this session.
    Session(u64),
}

fn account_scope(account: &super::codex_protocol::Account, session: u64) -> AccountScope {
    match account {
        super::codex_protocol::Account::ChatGpt {
            email: Some(email), ..
        } => AccountScope::Email(email.clone()),
        _ => AccountScope::Session(session),
    }
}
enum Slot {
    Connecting {
        id: u64,
        revoked: watch::Sender<bool>,
    },
    Ready(Arc<Entry>),
}
struct Reservation<'a> {
    registry: &'a CodexRegistry,
    profile: ProfileId,
    id: u64,
    active: bool,
}

struct ActiveOperation<'a> {
    registry: &'a CodexRegistry,
    profile: ProfileId,
    entry: Arc<Entry>,
    armed: bool,
}
impl Drop for ActiveOperation<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Ok(mut slots) = self.registry.entries.lock() {
            if matches!(slots.get(&self.profile), Some(Slot::Ready(current)) if Arc::ptr_eq(current, &self.entry))
            {
                slots.remove(&self.profile);
                self.entry.revoked.send_replace(true);
            }
        }
    }
}
impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut slots) = self.registry.entries.lock() {
            if matches!(slots.get(&self.profile), Some(Slot::Connecting { id, .. }) if *id == self.id)
            {
                slots.remove(&self.profile);
            }
        }
    }
}
#[derive(Default)]
pub struct CodexRegistry {
    entries: Mutex<HashMap<ProfileId, Slot>>,
    threads: Mutex<HashMap<(ProfileId, AccountScope), HashSet<String>>>,
    next: AtomicU64,
}

impl CodexRegistry {
    /// Only a trusted native launcher may provide streams and a runtime lease.
    /// No filesystem path, process command or credential is accepted from IPC.
    pub async fn attach<R, W, L>(
        &self,
        profile: ProfileId,
        reader: R,
        writer: W,
        lease: L,
    ) -> Result<Snapshot, RegistryError>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
        L: Send + Sync + 'static,
    {
        let id = self
            .next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| RegistryError::Limit)?;
        let (connecting_revoked, mut connecting_cancel) = watch::channel(false);
        {
            let mut slots = self.entries.lock().map_err(|_| RegistryError::Internal)?;
            if slots.contains_key(&profile) {
                return Err(RegistryError::Busy);
            }
            if slots.len() >= MAX_PROFILES {
                return Err(RegistryError::Limit);
            }
            slots.insert(
                profile,
                Slot::Connecting {
                    id,
                    revoked: connecting_revoked,
                },
            );
        }
        let mut reservation = Reservation {
            registry: self,
            profile,
            id,
            active: true,
        };
        let input: Input = BufReader::new(Box::new(reader));
        let output: Output = Box::new(writer);
        let connected = tokio::select! {
            connected = Client::connect(input, output) => connected,
            _ = connecting_cancel.changed() => return Err(RegistryError::Revoked),
        };
        let mut slots = self.entries.lock().map_err(|_| RegistryError::Internal)?;
        if !matches!(slots.get(&profile), Some(Slot::Connecting { id: current, .. }) if *current == id)
        {
            return Err(RegistryError::Revoked);
        }
        match connected {
            Err(error) => {
                slots.remove(&profile);
                Err(RegistryError::Client(error))
            }
            Ok(client) => {
                let snapshot = client.snapshot().clone();
                let account = account_scope(&snapshot.account, id);
                let (revoked, _) = watch::channel(false);
                slots.insert(
                    profile,
                    Slot::Ready(Arc::new(Entry {
                        client: AsyncMutex::new(client),
                        snapshot: Mutex::new(snapshot.clone()),
                        account,
                        revoked,
                        _lease: Box::new(lease),
                    })),
                );
                reservation.active = false;
                Ok(snapshot)
            }
        }
    }

    pub fn disconnect(&self, profile: ProfileId) -> Result<(), RegistryError> {
        let removed = self
            .entries
            .lock()
            .map_err(|_| RegistryError::Internal)?
            .remove(&profile);
        match removed {
            Some(Slot::Ready(entry)) => {
                entry.revoked.send_replace(true);
                Ok(())
            }
            Some(Slot::Connecting { revoked, .. }) => {
                revoked.send_replace(true);
                Ok(())
            }
            None => Err(RegistryError::Missing),
        }
    }

    /// Remove both the active session and in-memory thread ownership when the
    /// browser profile is deleted. Ownership is not yet persisted on disk.
    pub fn forget_profile(&self, profile: ProfileId) -> Result<(), RegistryError> {
        let mut slots = self.entries.lock().map_err(|_| RegistryError::Internal)?;
        let mut threads = self.threads.lock().map_err(|_| RegistryError::Internal)?;
        let removed = slots.remove(&profile);
        threads.retain(|(owner, _), _| *owner != profile);
        drop(threads);
        drop(slots);
        match removed {
            Some(Slot::Ready(entry)) => {
                entry.revoked.send_replace(true);
            }
            Some(Slot::Connecting { revoked, .. }) => {
                revoked.send_replace(true);
            }
            None => {}
        }
        Ok(())
    }

    fn entry(&self, profile: ProfileId) -> Result<Arc<Entry>, RegistryError> {
        match self
            .entries
            .lock()
            .map_err(|_| RegistryError::Internal)?
            .get(&profile)
        {
            Some(Slot::Ready(entry)) => Ok(entry.clone()),
            Some(Slot::Connecting { .. }) => Err(RegistryError::Busy),
            None => Err(RegistryError::Missing),
        }
    }
    fn current(&self, profile: ProfileId, entry: &Arc<Entry>) -> Result<bool, RegistryError> {
        Ok(
            matches!(self.entries.lock().map_err(|_| RegistryError::Internal)?.get(&profile), Some(Slot::Ready(current)) if Arc::ptr_eq(current, entry)),
        )
    }
    async fn publish(
        &self,
        profile: ProfileId,
        entry: &Arc<Entry>,
        sender: &mpsc::Sender<TurnEvent>,
        event: TurnEvent,
    ) -> Result<(), RegistryError> {
        let permit = tokio::time::timeout(std::time::Duration::from_secs(5), sender.reserve())
            .await
            .map_err(|_| RegistryError::ConsumerGone)?
            .map_err(|_| RegistryError::ConsumerGone)?;
        let slots = self.entries.lock().map_err(|_| RegistryError::Internal)?;
        if !matches!(slots.get(&profile), Some(Slot::Ready(current)) if Arc::ptr_eq(current, entry))
        {
            return Err(RegistryError::Revoked);
        }
        permit.send(event);
        Ok(())
    }

    pub async fn snapshot(&self, profile: ProfileId) -> Result<Snapshot, RegistryError> {
        let entry = self.entry(profile)?;
        if !self.current(profile, &entry)? {
            return Err(RegistryError::Revoked);
        }
        let snapshot = entry
            .snapshot
            .lock()
            .map_err(|_| RegistryError::Internal)?
            .clone();
        if !self.current(profile, &entry)? {
            return Err(RegistryError::Revoked);
        }
        Ok(snapshot)
    }

    pub async fn refresh_limits(&self, profile: ProfileId) -> Result<Snapshot, RegistryError> {
        let entry = self.entry(profile)?;
        let mut revoked = entry.revoked.subscribe();
        if *revoked.borrow_and_update() {
            return Err(RegistryError::Revoked);
        }
        let mut client = tokio::select! { lock = entry.client.lock() => lock, _ = revoked.changed() => return Err(RegistryError::Revoked) };
        if !self.current(profile, &entry)? {
            return Err(RegistryError::Revoked);
        }
        let mut operation = ActiveOperation {
            registry: self,
            profile,
            entry: entry.clone(),
            armed: true,
        };
        tokio::select! {
            result = client.refresh_limits() => { result.map_err(RegistryError::Client)?; },
            _ = revoked.changed() => return Err(RegistryError::Revoked),
        }
        if !self.current(profile, &entry)? {
            return Err(RegistryError::Revoked);
        }
        let snapshot = client.snapshot().clone();
        *entry.snapshot.lock().map_err(|_| RegistryError::Internal)? = snapshot.clone();
        if !self.current(profile, &entry)? {
            return Err(RegistryError::Revoked);
        }
        if client.is_usable() {
            operation.armed = false;
        }
        Ok(snapshot)
    }

    pub async fn start_thread(
        &self,
        profile: ProfileId,
        model: &str,
    ) -> Result<String, RegistryError> {
        let entry = self.entry(profile)?;
        let mut revoked = entry.revoked.subscribe();
        if *revoked.borrow_and_update() {
            return Err(RegistryError::Revoked);
        }
        let mut client = tokio::select! { lock = entry.client.lock() => lock, _ = revoked.changed() => return Err(RegistryError::Revoked) };
        if !self.current(profile, &entry)? {
            return Err(RegistryError::Revoked);
        }
        {
            let threads = self.threads.lock().map_err(|_| RegistryError::Internal)?;
            let owner = (profile, entry.account.clone());
            if threads
                .get(&owner)
                .is_some_and(|items| items.len() >= MAX_THREADS)
                || (!threads.contains_key(&owner) && threads.len() >= MAX_THREAD_OWNERS)
            {
                return Err(RegistryError::Limit);
            }
        }
        let mut operation = ActiveOperation {
            registry: self,
            profile,
            entry: entry.clone(),
            armed: true,
        };
        let result = tokio::select! {
            result = client.start_thread(model) => result.map(str::to_owned).map_err(RegistryError::Client),
            _ = revoked.changed() => return Err(RegistryError::Revoked),
        };
        if !self.current(profile, &entry)? {
            return Err(RegistryError::Revoked);
        }
        if let Ok(thread) = &result {
            let slots = self.entries.lock().map_err(|_| RegistryError::Internal)?;
            if !matches!(slots.get(&profile), Some(Slot::Ready(current)) if Arc::ptr_eq(current, &entry))
            {
                return Err(RegistryError::Revoked);
            }
            let mut threads = self.threads.lock().map_err(|_| RegistryError::Internal)?;
            let owner = (profile, entry.account.clone());
            if !threads.contains_key(&owner) && threads.len() >= MAX_THREAD_OWNERS {
                return Err(RegistryError::Limit);
            }
            threads.entry(owner).or_default().insert(thread.clone());
        }
        if client.is_usable() {
            operation.armed = false;
        }
        result
    }

    pub async fn resume_thread(
        &self,
        profile: ProfileId,
        thread: &str,
    ) -> Result<String, RegistryError> {
        let entry = self.entry(profile)?;
        let mut revoked = entry.revoked.subscribe();
        if *revoked.borrow_and_update() {
            return Err(RegistryError::Revoked);
        }
        let mut client = tokio::select! { lock = entry.client.lock() => lock, _ = revoked.changed() => return Err(RegistryError::Revoked) };
        if !self.current(profile, &entry)? {
            return Err(RegistryError::Revoked);
        }
        if !self
            .threads
            .lock()
            .map_err(|_| RegistryError::Internal)?
            .get(&(profile, entry.account.clone()))
            .is_some_and(|threads| threads.contains(thread))
        {
            return Err(RegistryError::UnknownThread);
        }
        let mut operation = ActiveOperation {
            registry: self,
            profile,
            entry: entry.clone(),
            armed: true,
        };
        let result = tokio::select! {
            result = client.resume_thread(thread) => result.map(str::to_owned).map_err(RegistryError::Client),
            _ = revoked.changed() => return Err(RegistryError::Revoked),
        };
        if !self.current(profile, &entry)? {
            return Err(RegistryError::Revoked);
        }
        if client.is_usable() {
            operation.armed = false;
        }
        result
    }

    pub async fn turn(
        &self,
        profile: ProfileId,
        prompt: &str,
        sender: &mpsc::Sender<TurnEvent>,
        cancel: &mut watch::Receiver<bool>,
        answers: &mut mpsc::Receiver<Answer>,
    ) -> Result<TurnEvent, RegistryError> {
        let entry = self.entry(profile)?;
        let mut revoked = entry.revoked.subscribe();
        if *revoked.borrow_and_update() {
            return Err(RegistryError::Revoked);
        }
        let mut client = tokio::select! { lock = entry.client.lock() => lock, _ = revoked.changed() => return Err(RegistryError::Revoked) };
        if !self.current(profile, &entry)? {
            return Err(RegistryError::Revoked);
        }
        let mut operation = ActiveOperation {
            registry: self,
            profile,
            entry: entry.clone(),
            armed: true,
        };
        let (events, mut receive) = mpsc::channel(32);
        let result = {
            let run = client.turn_with_approvals(prompt, &events, cancel, answers);
            tokio::pin!(run);
            loop {
                tokio::select! {
                    result = &mut run => break result.map_err(RegistryError::Client),
                    event = receive.recv() => match event {
                        Some(event) => self.publish(profile, &entry, sender, event).await?,
                        None => return Err(RegistryError::ConsumerGone),
                    },
                    _ = revoked.changed() => return Err(RegistryError::Revoked),
                }
            }
        };
        while let Ok(event) = receive.try_recv() {
            self.publish(profile, &entry, sender, event).await?;
        }
        if !self.current(profile, &entry)? {
            return Err(RegistryError::Revoked);
        }
        if client.is_usable() {
            operation.armed = false;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use tokio::io::{duplex, AsyncBufReadExt, AsyncWriteExt};

    struct Lease(Arc<AtomicBool>);
    impl Drop for Lease {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn known_threads_are_bound_to_account_or_single_session() {
        use super::super::codex_protocol::Account;
        let first = Account::ChatGpt {
            email: Some("one@example.com".into()),
            plan: "pro".into(),
        };
        let second = Account::ChatGpt {
            email: Some("two@example.com".into()),
            plan: "pro".into(),
        };
        let anonymous = Account::ChatGpt {
            email: None,
            plan: "pro".into(),
        };
        assert!(account_scope(&first, 1) == account_scope(&first, 2));
        assert!(account_scope(&first, 1) != account_scope(&second, 1));
        assert!(account_scope(&anonymous, 1) != account_scope(&anonymous, 2));
    }

    #[tokio::test]
    async fn profile_scope_and_disconnect_release_runtime_lease() {
        let registry = CodexRegistry::default();
        let owner = ProfileId::generate();
        let other = ProfileId::generate();
        let (client, server) = duplex(2048);
        let (read, write) = tokio::io::split(client);
        let (server_read, mut server_write) = tokio::io::split(server);
        let fake = tokio::spawn(async move {
            let mut read = BufReader::new(server_read);
            for response in [
                Some("{\"id\":1,\"result\":{}}\n"),
                None,
                Some("{\"id\":2,\"result\":{\"account\":null}}\n"),
            ] {
                let mut line = String::new();
                read.read_line(&mut line).await.unwrap();
                if let Some(response) = response {
                    server_write.write_all(response.as_bytes()).await.unwrap();
                }
            }
        });
        let dropped = Arc::new(AtomicBool::new(false));
        let snapshot = registry
            .attach(owner, read, write, Lease(dropped.clone()))
            .await
            .unwrap();
        assert_eq!(
            snapshot.account,
            super::super::codex_protocol::Account::SignedOut
        );
        assert_eq!(registry.snapshot(other).await, Err(RegistryError::Missing));
        assert_eq!(registry.disconnect(other), Err(RegistryError::Missing));
        registry.disconnect(owner).unwrap();
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(registry.snapshot(owner).await, Err(RegistryError::Missing));
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_handshake_does_not_reserve_profile_forever() {
        let registry = CodexRegistry::default();
        let profile = ProfileId::generate();
        let (client, _server) = duplex(2048);
        let (read, write) = tokio::io::split(client);
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(20),
            registry.attach(profile, read, write, ())
        )
        .await
        .is_err());
        assert_eq!(
            registry.snapshot(profile).await,
            Err(RegistryError::Missing)
        );
    }

    #[tokio::test]
    async fn disconnect_cancels_pending_handshake() {
        let registry = Arc::new(CodexRegistry::default());
        let profile = ProfileId::generate();
        let (client, server) = duplex(2048);
        let (read, write) = tokio::io::split(client);
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = registry.clone();
        let lease = Lease(dropped.clone());
        let task = tokio::spawn(async move { owner.attach(profile, read, write, lease).await });
        let mut server = BufReader::new(server);
        let mut line = String::new();
        server.read_line(&mut line).await.unwrap();
        registry.disconnect(profile).unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap(),
            Err(RegistryError::Revoked)
        );
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn old_cancelled_connect_does_not_remove_replacement() {
        let registry = Arc::new(CodexRegistry::default());
        let profile = ProfileId::generate();
        let (old, server) = duplex(2048);
        let (read, write) = tokio::io::split(old);
        let owner = registry.clone();
        let old_task = tokio::spawn(async move { owner.attach(profile, read, write, ()).await });
        let mut server = BufReader::new(server);
        let mut line = String::new();
        server.read_line(&mut line).await.unwrap();
        registry.disconnect(profile).unwrap();
        let (new, server) = duplex(2048);
        let (read, write) = tokio::io::split(new);
        let (server_read, mut server_write) = tokio::io::split(server);
        let fake = tokio::spawn(async move {
            let mut reader = BufReader::new(server_read);
            for response in [
                Some("{\"id\":1,\"result\":{}}\n"),
                None,
                Some("{\"id\":2,\"result\":{\"account\":null}}\n"),
            ] {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                if let Some(response) = response {
                    server_write.write_all(response.as_bytes()).await.unwrap();
                }
            }
        });
        registry.attach(profile, read, write, ()).await.unwrap();
        assert_eq!(old_task.await.unwrap(), Err(RegistryError::Revoked));
        assert!(registry.snapshot(profile).await.is_ok());
        registry.disconnect(profile).unwrap();
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn abandoning_thread_start_removes_ambiguous_session() {
        use serde_json::{json, Value};
        let registry = Arc::new(CodexRegistry::default());
        let profile = ProfileId::generate();
        let (client, server) = duplex(4096);
        let (read, write) = tokio::io::split(client);
        let (server_read, mut server_write) = tokio::io::split(server);
        let (started, start_seen) = tokio::sync::oneshot::channel();
        let fake = tokio::spawn(async move {
            let mut reader = BufReader::new(server_read);
            for result in [
                Some(json!({})),
                None,
                Some(json!({"account":{"type":"chatgpt","planType":"pro"}})),
                Some(json!({"rateLimits":{}})),
                Some(json!({"data":[{"model":"a","displayName":"A","isDefault":true}]})),
            ] {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let request: Value = serde_json::from_str(&line).unwrap();
                if let Some(result) = result {
                    server_write
                        .write_all(
                            format!("{}\n", json!({"id":request["id"],"result":result})).as_bytes(),
                        )
                        .await
                        .unwrap();
                }
            }
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&line).unwrap()["method"],
                "thread/start"
            );
            started.send(()).unwrap();
            line.clear();
            assert_eq!(reader.read_line(&mut line).await.unwrap(), 0);
        });
        let dropped = Arc::new(AtomicBool::new(false));
        registry
            .attach(profile, read, write, Lease(dropped.clone()))
            .await
            .unwrap();
        let owner = registry.clone();
        let operation = tokio::spawn(async move { owner.start_thread(profile, "a").await });
        start_seen.await.unwrap();
        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());
        assert_eq!(
            registry.snapshot(profile).await,
            Err(RegistryError::Missing)
        );
        assert!(dropped.load(Ordering::SeqCst));
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn disconnect_revokes_in_flight_turn_and_releases_lease() {
        use serde_json::{json, Value};
        use tokio::sync::oneshot;
        let registry = Arc::new(CodexRegistry::default());
        let profile = ProfileId::generate();
        let (client, server) = duplex(8192);
        let (read, write) = tokio::io::split(client);
        let (server_read, mut server_write) = tokio::io::split(server);
        let (release, released) = oneshot::channel();
        let fake = tokio::spawn(async move {
            let mut read = BufReader::new(server_read);
            for (method, result) in [
                ("initialize", Some(json!({}))),
                ("initialized", None),
                (
                    "account/read",
                    Some(json!({"account":{"type":"chatgpt","planType":"pro"}})),
                ),
                ("account/rateLimits/read", Some(json!({"rateLimits":{}}))),
                (
                    "model/list",
                    Some(json!({"data":[{"model":"a","displayName":"A","isDefault":true}]})),
                ),
                (
                    "thread/start",
                    Some(
                        json!({"thread":{"id":"t"},"model":"a","sandbox":{"type":"readOnly"},"approvalPolicy":"on-request","approvalsReviewer":"user"}),
                    ),
                ),
                (
                    "thread/resume",
                    Some(
                        json!({"thread":{"id":"t"},"sandbox":{"type":"readOnly"},"approvalPolicy":"on-request","approvalsReviewer":"user"}),
                    ),
                ),
                (
                    "account/rateLimits/read",
                    Some(json!({"rateLimits":{"primary":{"usedPercent":43}}})),
                ),
                ("turn/start", Some(json!({"turn":{"id":"u"}}))),
            ] {
                let mut line = String::new();
                read.read_line(&mut line).await.unwrap();
                let request: Value = serde_json::from_str(&line).unwrap();
                assert_eq!(request["method"], method);
                if let Some(result) = result {
                    server_write
                        .write_all(
                            format!("{}\n", json!({"id":request["id"],"result":result})).as_bytes(),
                        )
                        .await
                        .unwrap();
                }
            }
            server_write.write_all(b"{\"method\":\"item/agentMessage/delta\",\"params\":{\"threadId\":\"t\",\"turnId\":\"u\",\"delta\":\"first\"}}\n").await.unwrap();
            let _ = released.await;
            let _ = server_write.write_all(b"{\"method\":\"item/agentMessage/delta\",\"params\":{\"threadId\":\"t\",\"turnId\":\"u\",\"delta\":\"late\"}}\n").await;
        });
        let dropped = Arc::new(AtomicBool::new(false));
        registry
            .attach(profile, read, write, Lease(dropped.clone()))
            .await
            .unwrap();
        registry.start_thread(profile, "a").await.unwrap();
        assert_eq!(
            registry.resume_thread(profile, "other").await,
            Err(RegistryError::UnknownThread)
        );
        assert_eq!(registry.resume_thread(profile, "t").await.unwrap(), "t");
        assert_eq!(
            registry
                .refresh_limits(profile)
                .await
                .unwrap()
                .limits
                .unwrap()[0]
                .primary
                .as_ref()
                .unwrap()
                .used_percent,
            43
        );
        let (events, mut received) = mpsc::channel(8);
        let (_cancel, mut cancellation) = watch::channel(false);
        let (_answers, mut answers) = mpsc::channel(1);
        let owner = registry.clone();
        let task = tokio::spawn(async move {
            owner
                .turn(profile, "hello", &events, &mut cancellation, &mut answers)
                .await
        });
        assert_eq!(
            received.recv().await,
            Some(TurnEvent::Delta("first".into()))
        );
        let snapshot = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            registry.snapshot(profile),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            snapshot.account,
            super::super::codex_protocol::Account::ChatGpt { .. }
        ));
        registry.disconnect(profile).unwrap();
        let _ = release.send(());
        assert_eq!(task.await.unwrap(), Err(RegistryError::Revoked));
        assert!(received.recv().await.is_none());
        assert!(dropped.load(Ordering::SeqCst));
        assert!(registry
            .threads
            .lock()
            .unwrap()
            .keys()
            .any(|(owner, _)| *owner == profile));
        registry.forget_profile(profile).unwrap();
        assert!(!registry
            .threads
            .lock()
            .unwrap()
            .keys()
            .any(|(owner, _)| *owner == profile));
        fake.await.unwrap();
    }
}
