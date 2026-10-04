use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Notify;

use crate::api::schema::WorkspaceTransferImportParams;
use crate::events::AppEvent;
use crate::handoff_runtime::{HandoffRuntimeState, ImportedHandoffRuntime};
use crate::layout::PaneId;
use crate::persist::SessionSnapshot;
use crate::server::handoff;
use crate::terminal::{TerminalId, TerminalRuntime, TerminalState};
use crate::workspace::Workspace;

#[path = "startup.rs"]
mod startup;

pub(super) const MAX_PANES: usize = 256;
const TIMEOUT: Duration = Duration::from_secs(15);
const MAX_PAYLOAD: usize = 4 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
pub(super) struct TransferManifest {
    pub version: u32,
    pub protocol: u32,
    pub source_socket: PathBuf,
    pub source_workspace_id: String,
    pub snapshot: SessionSnapshot,
    pub panes: Vec<HandoffRuntimeState>,
    pub agent_states: HashMap<u32, Option<crate::terminal::state::HandoffAgentState>>,
    pub workspace_tokens: crate::metadata_tokens::TransferTokens,
    pub workspace_sequences: HashMap<String, u64>,
    pub terminal_tokens: HashMap<u32, crate::metadata_tokens::TransferTokens>,
    pub terminal_metadata: HashMap<u32, crate::terminal::state::TransferTerminalMetadata>,
}

/// A transport failure must never terminate a source PTY. Normal rollback
/// drains this back into the app; a disappearing event loop preserves children.
#[derive(Default)]
pub(super) struct ExportRuntimes(pub Vec<(PaneId, TerminalId, TerminalRuntime)>);

impl Drop for ExportRuntimes {
    fn drop(&mut self) {
        for (_, _, runtime) in self.0.drain(..) {
            runtime.preserve_for_handoff();
        }
    }
}

pub(super) struct ImportSettings {
    pub workspace_id: String,
    pub scrollback_limit_bytes: usize,
    pub default_shell: String,
    pub shell_mode: crate::config::ShellModeConfig,
    pub events: tokio::sync::mpsc::Sender<AppEvent>,
    pub render_notify: Arc<Notify>,
    pub render_dirty: Arc<crate::render_signal::RenderSignal>,
}

pub(super) struct StagedWorkspace {
    pub workspace: Workspace,
    pub terminals: HashMap<TerminalId, TerminalState>,
    pub runtimes: HashMap<TerminalId, TerminalRuntime>,
}

pub(super) enum Resolution {
    Committed,
    Cancelled,
    Unresolved(String),
}

pub(super) enum TransferEvent {
    TargetValidateNames {
        names: Vec<String>,
        validated: mpsc::Sender<Result<(), String>>,
    },
    SourcePrepared {
        token: String,
        workspace_id: String,
        decide: mpsc::Sender<bool>,
        decision: Arc<AtomicU8>,
    },
    SourceFinished {
        token: String,
        runtimes: ExportRuntimes,
        result: io::Result<String>,
    },
    TargetCommitted {
        staged: Box<StagedWorkspace>,
        installed: mpsc::Sender<Result<(), String>>,
    },
    TargetFinished {
        error: Option<String>,
    },
    TargetActivated {
        installed: mpsc::Sender<Result<(), String>>,
        result: Result<(), String>,
    },
    TargetUncertain {
        staged: Box<StagedWorkspace>,
        source_socket: PathBuf,
        token: String,
        error: String,
    },
    TargetResolution {
        resolution: Resolution,
    },
}

fn cancellation_resolution(value: &Value) -> Resolution {
    match (
        value["result"]["committed"].as_bool(),
        value["result"]["cancelled"].as_bool(),
    ) {
        (Some(true), Some(false)) => Resolution::Committed,
        (Some(false), Some(true)) => Resolution::Cancelled,
        _ => Resolution::Unresolved(
            "source did not confirm a terminal commit/cancellation decision".into(),
        ),
    }
}

fn cancel_at(socket: &Path, token: &str) -> Resolution {
    match rpc(
        socket,
        &json!({"id":token,"method":"workspace.transfer.cancel","params":{"token":token}}),
    ) {
        Ok(value) => cancellation_resolution(&value),
        Err(error) => Resolution::Unresolved(error.to_string()),
    }
}

pub(super) fn retry_resolution(
    socket: PathBuf,
    token: String,
    tx: mpsc::Sender<TransferEvent>,
    notify: Arc<Notify>,
) {
    tokio::task::spawn_blocking(move || {
        let resolution = cancel_at(&socket, &token);
        let _ = emit(&tx, &notify, TransferEvent::TargetResolution { resolution });
    });
}

pub(super) fn wait_activation(
    resumes: Vec<io::Result<mpsc::Receiver<io::Result<()>>>>,
    installed: mpsc::Sender<Result<(), String>>,
    tx: mpsc::Sender<TransferEvent>,
    notify: Arc<Notify>,
) {
    tokio::task::spawn_blocking(move || {
        let deadline = Instant::now() + Duration::from_secs(2);
        let result = activation_result(resumes, deadline);
        let _ = emit(
            &tx,
            &notify,
            TransferEvent::TargetActivated { installed, result },
        );
    });
}

fn activation_result(
    resumes: Vec<io::Result<mpsc::Receiver<io::Result<()>>>>,
    deadline: Instant,
) -> Result<(), String> {
    for resume in resumes {
        let ack = resume.map_err(|error| error.to_string())?;
        let timeout = remaining(deadline).map_err(|error| error.to_string())?;
        ack.recv_timeout(timeout)
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn emit(tx: &mpsc::Sender<TransferEvent>, notify: &Notify, event: TransferEvent) -> io::Result<()> {
    tx.send(event)
        .map_err(|_| io::Error::other("workspace transfer coordinator stopped"))?;
    notify.notify_one();
    Ok(())
}

pub(super) fn new_token() -> io::Result<String> {
    // Cryptographic randomness without introducing another dependency.
    let mut bytes = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

struct SocketGuard(PathBuf);
impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Both ends verify same-user owner-only socket files; the independent random
/// capability authenticates the invitation-to-transport association.
pub(super) fn validate_socket(path: &Path) -> io::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_socket()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "workspace transfer requires a same-user owner-only Unix socket",
        ));
    }
    Ok(())
}

fn configure(stream: &UnixStream) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(TIMEOUT))?;
    stream.set_write_timeout(Some(TIMEOUT))
}

fn write_json(stream: &mut UnixStream, value: &impl Serialize) -> io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    if bytes.len() > MAX_PAYLOAD {
        return Err(io::Error::other("workspace transfer payload exceeds 4 MiB"));
    }
    let deadline = Instant::now() + TIMEOUT;
    write_deadline(stream, &(bytes.len() as u32).to_be_bytes(), deadline)?;
    write_deadline(stream, &bytes, deadline)
}

fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "workspace transfer transport deadline exceeded",
            )
        })
}

fn write_deadline(stream: &mut UnixStream, mut bytes: &[u8], deadline: Instant) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.set_write_timeout(Some(remaining(deadline)?))?;
        match stream.write(bytes) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "workspace transfer stream closed",
                ))
            }
            Ok(count) => bytes = &bytes[count..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn read_deadline(
    stream: &mut UnixStream,
    mut bytes: &mut [u8],
    deadline: Instant,
) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.set_read_timeout(Some(remaining(deadline)?))?;
        match stream.read(bytes) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "workspace transfer stream closed",
                ))
            }
            Ok(count) => bytes = &mut bytes[count..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn read_json<T: serde::de::DeserializeOwned>(stream: &mut UnixStream) -> io::Result<T> {
    let mut length = [0u8; 4];
    let deadline = Instant::now() + TIMEOUT;
    read_deadline(stream, &mut length, deadline)?;
    let length = u32::from_be_bytes(length) as usize;
    if length == 0 || length > MAX_PAYLOAD {
        return Err(io::Error::other(
            "invalid workspace transfer payload length",
        ));
    }
    let mut bytes = vec![0; length];
    read_deadline(stream, &mut bytes, deadline)?;
    serde_json::from_slice(&bytes).map_err(io::Error::other)
}

#[derive(Debug)]
pub(super) struct ForwardError {
    pub submitted: bool,
    error: io::Error,
}

impl std::fmt::Display for ForwardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

fn forwarding_response_timeout(value: &Value) -> Option<Duration> {
    let method = value["method"].as_str().unwrap_or_default();
    let wait = match method {
        "agent.wait" | "pane.wait_for_output" | "events.wait" => Some(&value["params"]),
        "agent.prompt" if value["params"]["wait"].is_object() => Some(&value["params"]["wait"]),
        _ => None,
    };
    match wait {
        Some(wait) => wait["timeout_ms"]
            .as_u64()
            .map(|ms| Duration::from_millis(ms).saturating_add(TIMEOUT)),
        None => Some(TIMEOUT),
    }
}

pub(super) fn rpc_forward(socket: &Path, value: &Value) -> Result<Value, ForwardError> {
    rpc_inner(socket, value, TIMEOUT, forwarding_response_timeout(value))
}

pub(super) fn rpc(socket: &Path, value: &Value) -> io::Result<Value> {
    rpc_inner(socket, value, TIMEOUT, Some(TIMEOUT)).map_err(|error| error.error)
}

fn rpc_inner(
    socket: &Path,
    value: &Value,
    send_timeout: Duration,
    response_timeout: Option<Duration>,
) -> Result<Value, ForwardError> {
    let before = |error| ForwardError {
        submitted: false,
        error,
    };
    let after = |error| ForwardError {
        submitted: true,
        error,
    };
    validate_socket(socket).map_err(before)?;
    let mut stream = UnixStream::connect(socket).map_err(before)?;
    configure(&stream).map_err(before)?;
    let bytes = serde_json::to_vec(value).map_err(|error| before(io::Error::other(error)))?;
    if bytes.len() > MAX_PAYLOAD {
        return Err(before(io::Error::other(
            "forwarded API request exceeds 4 MiB",
        )));
    }
    let deadline = Instant::now() + send_timeout;
    write_deadline(&mut stream, &bytes, deadline).map_err(after)?;
    write_deadline(&mut stream, b"\n", deadline).map_err(after)?;
    // A server-side wait is response latency, not handshake latency. Missing
    // timeout_ms preserves the API's unbounded-wait semantics.
    let response_deadline =
        response_timeout.and_then(|timeout| Instant::now().checked_add(timeout));
    // Bounded independently of the session API's general line reader.
    let mut response = Vec::new();
    let mut buffer = [0; 8192];
    while response.len() <= MAX_PAYLOAD {
        stream
            .set_read_timeout(
                response_deadline
                    .map(remaining)
                    .transpose()
                    .map_err(after)?,
            )
            .map_err(after)?;
        let count = match stream.read(&mut buffer) {
            Ok(0) => {
                return Err(after(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "forwarded API stream closed",
                )))
            }
            Ok(count) => count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(after(error)),
        };
        let end = buffer[..count].iter().position(|b| *b == b'\n');
        let length = end.unwrap_or(count);
        if response.len() + length > MAX_PAYLOAD {
            break;
        }
        response.extend_from_slice(&buffer[..length]);
        if end.is_some() {
            return serde_json::from_slice(&response)
                .map_err(|error| after(io::Error::other(error)));
        }
    }
    Err(after(io::Error::other(
        "forwarded API response exceeds 4 MiB",
    )))
}

pub(super) fn export(
    token: String,
    destination: PathBuf,
    mut manifest: TransferManifest,
    mut runtimes: ExportRuntimes,
    tx: mpsc::Sender<TransferEvent>,
    notify: Arc<Notify>,
    phase: Arc<AtomicU8>,
    session: String,
) {
    tokio::task::spawn_blocking(move || {
        let mut committed = false;
        let result = (|| -> io::Result<String> {
            startup::ensure_ready(&session, &destination)?;
            if phase.load(Ordering::Acquire) == 2 {
                return Err(io::Error::other(
                    "source cancelled workspace transfer during destination startup",
                ));
            }
            let pause_deadline = std::time::Instant::now() + Duration::from_secs(2);
            for (_, _, runtime) in &runtimes.0 {
                let remaining = pause_deadline
                    .checked_duration_since(std::time::Instant::now())
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "workspace PTY pause deadline exceeded",
                        )
                    })?;
                runtime.pause_handoff_reader(remaining)?;
            }
            let mut fds = Vec::new();
            for (pane, _, runtime) in &runtimes.0 {
                if let Some(cwd) = runtime.cwd_for_persistence() {
                    for tab in &mut manifest.snapshot.workspaces[0].tabs {
                        if let Some(saved) = tab.panes.get_mut(&pane.raw()) {
                            saved.cwd = cwd.clone();
                        }
                    }
                }
                let mut state = runtime.handoff_runtime_state(pane.raw());
                state.agent_state = manifest.agent_states.remove(&pane.raw()).flatten();
                state.initial_history_ansi = runtime.handoff_history_ansi();
                manifest.panes.push(state);
                fds.push(unsafe { OwnedFd::from_raw_fd(runtime.duplicate_handoff_fd()?) });
            }
            let path = crate::session::data_dir().join(format!("xfer-{}.sock", &token[..16]));
            // A fresh capability-derived pathname; never remove somebody else's socket.
            let listener = UnixListener::bind(&path)?;
            let _guard = SocketGuard(path.clone());
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
            listener.set_nonblocking(true)?;
            let invitation = rpc(
                &destination,
                &json!({"id":token,"method":"workspace.transfer.import","params":{"socket_path":path,"token":token}}),
            )?;
            if invitation.get("error").is_some() {
                return Err(io::Error::other(
                    invitation["error"]["message"]
                        .as_str()
                        .unwrap_or("destination rejected workspace transfer"),
                ));
            }
            let (mut stream, _) = handoff::accept_with_timeout(&listener, TIMEOUT)?;
            configure(&stream)?;
            let received_token: String = read_json(&mut stream)?;
            if received_token != token {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "workspace transfer token mismatch",
                ));
            }
            write_json(&mut stream, &manifest)?;
            let validated: Value = read_json(&mut stream)?;
            if validated != "validated" {
                return Err(io::Error::other(
                    validated["error"]
                        .as_str()
                        .unwrap_or("destination did not validate workspace"),
                ));
            }
            handoff::send_fds(
                &stream,
                &fds.iter().map(AsRawFd::as_raw_fd).collect::<Vec<_>>(),
            )?;
            let ready: Value = read_json(&mut stream)?;
            let workspace_id = ready["workspace_id"]
                .as_str()
                .filter(|id| crate::workspace::public_workspace_number(id).is_some())
                .ok_or_else(|| io::Error::other("destination did not stage workspace"))?
                .to_string();
            let (decide, decision) = mpsc::channel();
            emit(
                &tx,
                &notify,
                TransferEvent::SourcePrepared {
                    token: token.clone(),
                    workspace_id: workspace_id.clone(),
                    decide,
                    decision: phase.clone(),
                },
            )?;
            let elected = decision.recv_timeout(TIMEOUT).unwrap_or(false);
            if !elected {
                let _ = phase.compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire);
            }
            if phase.load(Ordering::Acquire) != 1 {
                return Err(io::Error::other("source cancelled workspace transfer"));
            }
            committed = true;
            // Source relinquishes its reader before the destination can activate.
            // preserve_for_handoff deliberately leaves original child waiters alive.
            for (_, _, runtime) in runtimes.0.drain(..) {
                runtime.preserve_for_handoff();
            }
            write_json(&mut stream, &"committed")?;
            let owned: String = read_json(&mut stream)?;
            if owned != "owned" {
                return Err(io::Error::other("destination ownership not confirmed"));
            }
            Ok(workspace_id)
        })();
        if !committed {
            // Every pre-commit failure is terminal. A delayed prepared event or
            // destination cancellation query cannot subsequently elect commit.
            let _ = phase.compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire);
            for (_, _, runtime) in &runtimes.0 {
                runtime.set_handoff_reader_paused(false);
            }
        }
        let _ = emit(
            &tx,
            &notify,
            TransferEvent::SourceFinished {
                token,
                runtimes,
                result,
            },
        );
    });
}

pub(super) fn import(
    params: WorkspaceTransferImportParams,
    settings: ImportSettings,
    tx: mpsc::Sender<TransferEvent>,
) {
    tokio::task::spawn_blocking(move || {
        let notify = settings.render_notify.clone();
        let mut retained = false;
        let result = (|| -> io::Result<()> {
            validate_socket(&params.socket_path)?;
            let mut stream = UnixStream::connect(&params.socket_path)?;
            configure(&stream)?;
            write_json(&mut stream, &params.token)?;
            let mut manifest: TransferManifest = read_json(&mut stream)?;
            validate_manifest(&manifest)?;
            validate_socket(&manifest.source_socket)?;
            // The event loop validates current live destination names before
            // descriptors are received or the source can elect commit.
            let names = manifest
                .snapshot
                .workspaces
                .iter()
                .flat_map(|workspace| workspace.tabs.iter())
                .flat_map(|tab| tab.panes.values())
                .filter_map(|pane| pane.agent_name.clone())
                .collect();
            let (validated, validation) = mpsc::channel();
            emit(
                &tx,
                &notify,
                TransferEvent::TargetValidateNames { names, validated },
            )?;
            if let Err(error) = validation.recv_timeout(TIMEOUT).map_err(io::Error::other)? {
                write_json(&mut stream, &json!({"error":error}))?;
                return Err(io::Error::other(error));
            }
            write_json(&mut stream, &"validated")?;
            // OwnedFd guards cover any pre-restore failure. restore_handoff
            // consumes each descriptor into the nonowning runtime.
            let mut fds: Vec<_> = handoff::recv_fds(&stream, manifest.panes.len())?
                .into_iter()
                .map(|fd| unsafe { OwnedFd::from_raw_fd(fd) })
                .collect();
            manifest.snapshot.workspaces[0].id = Some(settings.workspace_id.clone());
            let mut imports = HashMap::new();
            use std::os::fd::IntoRawFd;
            for (state, fd) in manifest.panes.iter().cloned().zip(fds.drain(..)) {
                imports.insert(
                    state.pane_id,
                    ImportedHandoffRuntime {
                        master_fd: fd.into_raw_fd(),
                        state,
                    },
                );
            }
            let restored = crate::persist::restore_handoff(
                &manifest.snapshot,
                settings.scrollback_limit_bytes,
                &settings.default_shell,
                settings.shell_mode,
                &mut imports,
                settings.events.clone(),
                settings.render_notify.clone(),
                settings.render_dirty.clone(),
            );
            // Strict restore errors can leave unconsumed raw descriptors.
            for (_, import) in imports.drain() {
                drop(unsafe { OwnedFd::from_raw_fd(import.master_fd) });
            }
            let (mut workspaces, mut terminals, runtimes) = restored?;
            if workspaces.len() != 1 || runtimes.len() != manifest.panes.len() {
                return Err(io::Error::other(
                    "destination restored an incomplete workspace",
                ));
            }
            let mut workspace = workspaces.remove(0);
            let aliases = crate::persist::handoff_pane_aliases(
                &manifest.snapshot,
                std::slice::from_ref(&workspace),
            );
            let resolve = |raw| {
                aliases
                    .get(&raw)
                    .copied()
                    .unwrap_or_else(|| PaneId::from_raw(raw))
            };
            workspace
                .metadata_tokens
                .restore_transfer(manifest.workspace_tokens);
            workspace.metadata_token_sequences = manifest.workspace_sequences;
            for (raw, tokens) in manifest.terminal_tokens {
                let pane = resolve(raw);
                if let Some(id) = workspace.tabs.iter().find_map(|tab| tab.terminal_id(pane)) {
                    if let Some(terminal) = terminals.get_mut(id) {
                        terminal.metadata_tokens.restore_transfer(tokens);
                    }
                }
            }
            for (raw, metadata) in manifest.terminal_metadata {
                let pane = resolve(raw);
                if let Some(id) = workspace.tabs.iter().find_map(|tab| tab.terminal_id(pane)) {
                    if let Some(terminal) = terminals.get_mut(id) {
                        terminal.restore_transfer_metadata(metadata);
                    }
                }
            }
            for state in &manifest.panes {
                let pane = resolve(state.pane_id);
                if let Some(id) = workspace.tabs.iter().find_map(|tab| tab.terminal_id(pane)) {
                    if let Some(terminal) = terminals.get_mut(id) {
                        terminal.set_terminal_title(state.terminal_title.clone());
                    }
                }
            }
            let staged = Box::new(StagedWorkspace {
                workspace,
                terminals,
                runtimes,
            });
            write_json(&mut stream, &json!({"workspace_id":settings.workspace_id}))?;
            let commit: io::Result<String> = read_json(&mut stream);
            if !matches!(commit, Ok(ref value) if value == "committed") {
                // A lost commit byte is ambiguous. Query the source's event-loop
                // decision, never infer rollback from an ownership-ack timeout.
                match cancel_at(&manifest.source_socket, &params.token) {
                    Resolution::Committed => {}
                    Resolution::Cancelled => {
                        return Err(io::Error::other("source cancelled workspace transfer"))
                    }
                    Resolution::Unresolved(error) => {
                        emit(
                            &tx,
                            &notify,
                            TransferEvent::TargetUncertain {
                                staged,
                                source_socket: manifest.source_socket,
                                token: params.token,
                                error,
                            },
                        )?;
                        retained = true;
                        return Ok(());
                    }
                }
            }
            let (installed, installation) = mpsc::channel();
            emit(
                &tx,
                &notify,
                TransferEvent::TargetCommitted { staged, installed },
            )?;
            installation
                .recv_timeout(TIMEOUT)
                .map_err(io::Error::other)?
                .map_err(io::Error::other)?;
            // Installation is already committed: a lost ack is only a warning.
            if let Err(error) = write_json(&mut stream, &"owned") {
                tracing::warn!(%error, "workspace ownership acknowledgement lost after commit");
            }
            Ok(())
        })();
        if !retained {
            let _ = emit(
                &tx,
                &notify,
                TransferEvent::TargetFinished {
                    error: result.err().map(|e| e.to_string()),
                },
            );
        }
    });
}

fn validate_manifest(manifest: &TransferManifest) -> io::Result<()> {
    if manifest.version != 1 || manifest.protocol != crate::protocol::PROTOCOL_VERSION {
        return Err(io::Error::other(
            "workspace transfer requires matching server protocol versions",
        ));
    }
    if manifest.snapshot.workspaces.len() != 1
        || manifest.panes.is_empty()
        || manifest.panes.len() > MAX_PANES
    {
        return Err(io::Error::other(
            "invalid workspace transfer workspace/pane count",
        ));
    }
    let workspace = &manifest.snapshot.workspaces[0];
    if workspace.worktree_space.is_some()
        || workspace.id.as_deref() != Some(manifest.source_workspace_id.as_str())
    {
        return Err(io::Error::other(
            "invalid workspace transfer identity or worktree association",
        ));
    }
    let expected: HashSet<_> = workspace
        .tabs
        .iter()
        .flat_map(|t| t.panes.keys().copied())
        .collect();
    let received: HashSet<_> = manifest.panes.iter().map(|p| p.pane_id).collect();
    if expected != received
        || received.len() != manifest.panes.len()
        || expected.len()
            != workspace
                .tabs
                .iter()
                .map(|tab| tab.panes.len())
                .sum::<usize>()
        || workspace.tabs.is_empty()
    {
        return Err(io::Error::other(
            "workspace transfer runtime identities do not match snapshot",
        ));
    }
    for tab in &workspace.tabs {
        let mut layout = Vec::new();
        collect_layout(&tab.layout, &mut layout);
        if layout.len() != tab.panes.len()
            || layout.iter().collect::<HashSet<_>>().len() != layout.len()
            || layout.iter().any(|id| !tab.panes.contains_key(id))
        {
            return Err(io::Error::other(
                "workspace transfer layout does not match panes",
            ));
        }
    }
    Ok(())
}

fn collect_layout(node: &crate::persist::LayoutSnapshot, out: &mut Vec<u32>) {
    match node {
        crate::persist::LayoutSnapshot::Pane(id) => out.push(*id),
        crate::persist::LayoutSnapshot::Split { first, second, .. } => {
            collect_layout(first, out);
            collect_layout(second, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixtureChild(Box<dyn portable_pty::Child + Send + Sync>);

    impl Drop for FixtureChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn losing_commit_transport_and_source_api_retains_real_staged_descriptors() {
        lost_commit_fixture(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn uncertain_import_recovers_commit_and_activates_real_pty_exactly_once() {
        lost_commit_fixture(true).await;
    }

    async fn lost_commit_fixture(recover: bool) {
        use std::io::{BufRead, BufReader};
        use std::os::fd::{FromRawFd, OwnedFd};

        let suffix = new_token().expect("token")[..16].to_string();
        let transfer_path =
            std::env::temp_dir().join(format!("wxt-{}-{suffix}.sock", std::process::id()));
        let api_path =
            std::env::temp_dir().join(format!("wxa-{}-{suffix}.sock", std::process::id()));
        let transfer_listener = UnixListener::bind(&transfer_path).expect("transfer socket");
        let api_listener = UnixListener::bind(&api_path).expect("source API socket");
        let _transfer_guard = SocketGuard(transfer_path.clone());
        let _api_guard = SocketGuard(api_path.clone());
        for path in [&transfer_path, &api_path] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                .expect("socket permissions");
        }
        let workspace = Workspace::test_new("fd-retention");
        let mut snapshot = crate::persist::capture(
            std::slice::from_ref(&workspace),
            &HashMap::new(),
            &crate::terminal::TerminalRuntimeRegistry::new(),
            Some(0),
            0,
        );
        snapshot.workspaces[0].identity_cwd = std::env::temp_dir();
        for pane in snapshot.workspaces[0].tabs[0].panes.values_mut() {
            pane.cwd = std::env::temp_dir();
        }
        let raw = *snapshot.workspaces[0].tabs[0]
            .panes
            .keys()
            .next()
            .expect("one pane");
        let manifest = TransferManifest {
            version: 1,
            protocol: crate::protocol::PROTOCOL_VERSION,
            source_socket: api_path.clone(),
            source_workspace_id: workspace.id.clone(),
            snapshot,
            panes: vec![HandoffRuntimeState {
                pane_id: raw,
                child_pid: 0,
                rows: 24,
                cols: 80,
                cell_width_px: 0,
                cell_height_px: 0,
                keyboard_protocol_flags: 0,
                keyboard_protocol_ansi: None,
                input_state: None,
                terminal_title: None,
                initial_history_ansi: None,
                agent_state: None,
            }],
            agent_states: HashMap::new(),
            workspace_tokens: crate::metadata_tokens::MetadataTokens::default().capture_transfer(),
            workspace_sequences: HashMap::new(),
            terminal_tokens: HashMap::new(),
            terminal_metadata: HashMap::new(),
        };
        let token = new_token().expect("token");
        let expected_token = token.clone();
        let (master, mut child_endpoint, child) = if recover {
            let pair = portable_pty::native_pty_system()
                .openpty(portable_pty::PtySize {
                    rows: 24,
                    cols: 80,
                    pixel_width: 0,
                    pixel_height: 0,
                })
                .expect("real PTY");
            let mut command = portable_pty::CommandBuilder::new("/bin/sh");
            command.arg("-c");
            command.arg("printf 'RECOVERY_READY\\n'; while IFS= read -r line; do printf 'RECOVERY_OUTPUT:%s\\n' \"$line\"; done");
            let child = FixtureChild(pair.slave.spawn_command(command).expect("PTY child"));
            let fd = unsafe {
                libc::fcntl(
                    pair.master.as_raw_fd().expect("master fd"),
                    libc::F_DUPFD_CLOEXEC,
                    0,
                )
            };
            assert!(fd >= 0, "duplicate real PTY descriptor");
            let master = std::fs::File::from(unsafe { OwnedFd::from_raw_fd(fd) });
            (master, None, Some(child))
        } else {
            let (master, endpoint) = UnixStream::pair().expect("PTY stand-in");
            (
                std::fs::File::from(OwnedFd::from(master)),
                Some(endpoint),
                None,
            )
        };
        let source = std::thread::spawn(move || {
            let (mut stream, _) = transfer_listener.accept().expect("destination");
            assert_eq!(
                read_json::<String>(&mut stream).expect("capability"),
                expected_token
            );
            write_json(&mut stream, &manifest).expect("manifest");
            assert_eq!(
                read_json::<String>(&mut stream).expect("validated"),
                "validated"
            );
            handoff::send_fds(&stream, &[master.as_raw_fd()]).expect("descriptor");
            let ready: Value = read_json(&mut stream).expect("prepared");
            assert_eq!(ready["workspace_id"], "w2");
            // Lose both transports after staging, with no commit/cancel proof.
            drop(api_listener);
            drop(master);
        });
        let (events, _events_rx) = tokio::sync::mpsc::channel(64);
        let (tx, rx) = mpsc::channel();
        import(
            WorkspaceTransferImportParams {
                socket_path: transfer_path,
                token: token.clone(),
            },
            ImportSettings {
                workspace_id: "w2".into(),
                scrollback_limit_bytes: 1024 * 1024,
                default_shell: "/bin/sh".into(),
                shell_mode: crate::config::ShellModeConfig::Auto,
                events,
                render_notify: Arc::new(Notify::new()),
                render_dirty: Arc::new(crate::render_signal::RenderSignal::new()),
            },
            tx,
        );
        let uncertain = loop {
            match rx
                .recv_timeout(Duration::from_secs(5))
                .expect("transfer event")
            {
                TransferEvent::TargetValidateNames { validated, .. } => {
                    let _ = validated.send(Ok(()));
                }
                event @ TransferEvent::TargetUncertain { .. } => break event,
                TransferEvent::TargetFinished { error } => {
                    panic!("staging was discarded after an unresolved source: {error:?}")
                }
                _ => panic!("unexpected transfer event"),
            }
        };
        source.join().expect("source transport");
        if recover {
            let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
            let mut app = crate::app::App::new(
                &crate::config::Config::default(),
                crate::app::AppPolicy::TEST,
                None,
                api_rx,
                crate::api::EventHub::default(),
            );
            let mut coordinator = super::super::Coordinator {
                incoming: true,
                ..Default::default()
            };
            coordinator
                .reserve_server()
                .expect("reserve importing server");
            coordinator.tx.send(uncertain).expect("retained staging");
            coordinator.poll(&mut app);
            assert!(
                app.state.workspaces.is_empty(),
                "unresolved staging stays invisible"
            );
            assert!(coordinator.workspace_pending("w2"));
            assert_eq!(coordinator.stop_control.load(Ordering::Acquire), 1);
            let (respond_to, response) = mpsc::channel();
            let report = crate::api::ApiRequestMessage {
                request: serde_json::from_value(json!({"id":"retained-report","method":"workspace.report_metadata","params":{"workspace_id":"w2","source":"test","tokens":{"summary":"recovered"},"seq":1}})).expect("report"),
                respond_to, response_write_complete: None,
            };
            assert!(coordinator.intercept(&mut app, report).is_none());
            assert!(matches!(
                response.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));

            std::fs::remove_file(&api_path).expect("replace unavailable source socket");
            let listener = UnixListener::bind(&api_path).expect("recovered source API");
            std::fs::set_permissions(&api_path, std::fs::Permissions::from_mode(0o600))
                .expect("owner-only source API");
            let source_api = std::thread::spawn(move || {
                let (stream, _) = listener.accept().expect("resolution RPC");
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).expect("resolution request");
                let request: Value = serde_json::from_str(&line).expect("resolution JSON");
                assert_eq!(request["method"], "workspace.transfer.cancel");
                assert_eq!(request["params"]["token"], token);
                writeln!(reader.get_mut(), "{}", json!({"id":request["id"],"result":{"type":"workspace_transfer_status","committed":true,"cancelled":false}})).expect("committed decision");
            });
            coordinator.uncertain.as_mut().expect("staging").next_retry = Instant::now();
            let deadline = Instant::now() + Duration::from_secs(5);
            while coordinator.busy() {
                coordinator.poll(&mut app);
                assert!(
                    Instant::now() < deadline,
                    "uncertain import did not activate"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            source_api.join().expect("source API");
            assert!(coordinator.uncertain.is_none());
            assert_eq!(coordinator.stop_control.load(Ordering::Acquire), 0);
            assert_eq!(app.state.workspaces.len(), 1);
            assert_eq!(app.terminal_runtimes.len(), 1);
            let reports = coordinator.take_replayed_requests();
            assert_eq!(reports.len(), 1);
            for report in reports {
                report
                    .respond_to
                    .send(app.handle_api_request(report.request))
                    .expect("report reply");
            }
            let reply: Value = serde_json::from_str(
                &response
                    .recv_timeout(Duration::from_secs(2))
                    .expect("retained report replayed"),
            )
            .expect("report result");
            assert!(reply.get("error").is_none(), "{reply}");
            let workspace: Value = serde_json::from_str(
                &app.handle_api_request(
                    serde_json::from_value(
                        json!({"id":"get","method":"workspace.get","params":{"workspace_id":"w2"}}),
                    )
                    .expect("get workspace"),
                ),
            )
            .expect("workspace result");
            assert_eq!(
                workspace["result"]["workspace"]["tokens"]["summary"],
                "recovered"
            );
            coordinator
                .tx
                .send(TransferEvent::TargetResolution {
                    resolution: Resolution::Committed,
                })
                .expect("duplicate resolution");
            coordinator.poll(&mut app);
            assert_eq!(app.state.workspaces.len(), 1);
            assert!(coordinator.take_replayed_requests().is_empty());
            let runtime = app.terminal_runtimes.values().next().expect("imported PTY");
            let deadline = Instant::now() + Duration::from_secs(5);
            while !runtime.visible_text().contains("RECOVERY_READY") {
                assert!(Instant::now() < deadline, "reader failed to resume");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            runtime
                .try_send_paste("round-trip\n".into())
                .expect("real PTY input");
            while !runtime
                .visible_text()
                .contains("RECOVERY_OUTPUT:round-trip")
            {
                assert!(
                    Instant::now() < deadline,
                    "real PTY output missing after activation"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            drop(child);
            return;
        }
        let TransferEvent::TargetUncertain { staged, .. } = uncertain else {
            unreachable!()
        };
        assert_eq!(staged.runtimes.len(), 1);
        child_endpoint
            .as_mut()
            .expect("PTY stand-in")
            .write_all(b"retained")
            .expect("destination still holds the only transferred descriptor");
        drop(staged);
    }

    #[test]
    fn only_terminal_atomic_cancellation_allows_staging_to_be_discarded() {
        for value in [
            json!({"result":{"committed":false}}),
            json!({"result":{"committed":false,"cancelled":false}}),
            json!({"error":{"message":"unavailable"}}),
        ] {
            assert!(matches!(
                cancellation_resolution(&value),
                Resolution::Unresolved(_)
            ));
        }
        assert!(matches!(
            cancellation_resolution(&json!({"result":{"committed":false,"cancelled":true}})),
            Resolution::Cancelled
        ));
        assert!(matches!(
            cancellation_resolution(&json!({"result":{"committed":true,"cancelled":false}})),
            Resolution::Committed
        ));
    }

    #[test]
    fn forwarded_waits_have_response_budgets_separate_from_handshake_latency() {
        assert_eq!(
            forwarding_response_timeout(
                &json!({"method":"agent.wait","params":{"timeout_ms":60000}})
            ),
            Some(Duration::from_secs(75))
        );
        assert_eq!(
            forwarding_response_timeout(
                &json!({"method":"agent.prompt","params":{"wait":{"timeout_ms":120000}}})
            ),
            Some(Duration::from_secs(135))
        );
        assert_eq!(
            forwarding_response_timeout(&json!({"method":"agent.wait","params":{}})),
            None
        );
        assert_eq!(
            forwarding_response_timeout(&json!({"method":"agent.prompt","params":{"wait":{}}})),
            None
        );
        assert_eq!(
            forwarding_response_timeout(&json!({"method":"agent.prompt","params":{}})),
            Some(TIMEOUT)
        );
    }

    #[test]
    fn activation_uses_one_aggregate_deadline_even_for_256_panes() {
        let mut senders = Vec::new();
        let resumes = (0..256)
            .map(|_| {
                let (sender, ack) = mpsc::channel();
                senders.push(sender);
                Ok(ack)
            })
            .collect();
        let error = activation_result(resumes, Instant::now() - Duration::from_millis(1))
            .expect_err("expired aggregate deadline");
        assert!(error.contains("deadline exceeded"));
    }

    #[test]
    fn an_eof_after_prompt_submission_is_an_unknown_outcome_not_unavailability() {
        use std::io::{BufRead, BufReader};
        let path = std::env::temp_dir().join(format!(
            "wxf-{}-{}.sock",
            std::process::id(),
            &new_token().expect("token")[..16]
        ));
        let listener = UnixListener::bind(&path).expect("test socket");
        let _guard = SocketGuard(path.clone());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("owner-only socket");
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("request connection");
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).expect("submitted prompt");
            assert!(line.contains("agent.prompt"));
            // Simulate execution/submission followed by a lost response.
        });
        let error=rpc_forward(&path,&json!({"id":"prompt","method":"agent.prompt","params":{"target":"w1:p1","text":"execute","wait":{"timeout_ms":120000}}})).expect_err("response lost");
        server.join().expect("server");
        assert!(error.submitted);
    }

    #[test]
    fn a_wait_response_can_arrive_after_the_send_deadline() {
        use std::io::{BufRead, BufReader};
        let path = std::env::temp_dir().join(format!(
            "wxw-{}-{}.sock",
            std::process::id(),
            &new_token().expect("token")[..16]
        ));
        let listener = UnixListener::bind(&path).expect("test socket");
        let _guard = SocketGuard(path.clone());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("owner-only socket");
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("request connection");
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).expect("request");
            std::thread::sleep(Duration::from_millis(1200));
            writeln!(
                reader.get_mut(),
                "{{\"id\":\"wait\",\"result\":{{\"type\":\"ok\"}}}}"
            )
            .expect("wait response");
        });
        let response = rpc_inner(
            &path,
            &json!({"id":"wait","method":"agent.wait","params":{"timeout_ms":60000}}),
            Duration::from_secs(1),
            Some(Duration::from_secs(3)),
        )
        .expect("response after send deadline");
        server.join().expect("server");
        assert_eq!(response["result"]["type"], "ok");
    }

    #[test]
    fn transfer_frames_reject_oversized_length_before_allocating() {
        let (mut reader, mut writer) = UnixStream::pair().expect("socket pair");
        writer
            .write_all(&((MAX_PAYLOAD + 1) as u32).to_be_bytes())
            .expect("write length");
        assert!(read_json::<Value>(&mut reader).is_err());
    }

    #[test]
    fn transfer_tokens_are_unpredictable_unique_capabilities() {
        let a = new_token().expect("random token");
        let b = new_token().expect("random token");
        assert_eq!(a.len(), 64);
        assert_ne!(a, b);
    }

    #[test]
    fn transfer_frames_and_descriptor_batches_share_a_stream() {
        let (mut source, mut target) = UnixStream::pair().expect("socket pair");
        let file = std::fs::File::open("/dev/null").expect("test descriptor");
        write_json(&mut source, &"validated").expect("write frame");
        handoff::send_fds(&source, &[file.as_raw_fd(), file.as_raw_fd()])
            .expect("send descriptors");
        write_json(&mut source, &"committed").expect("write commit");
        assert_eq!(
            read_json::<String>(&mut target).expect("read frame"),
            "validated"
        );
        let descriptors = handoff::recv_fds(&target, 2).expect("receive descriptors");
        for fd in descriptors {
            drop(unsafe { OwnedFd::from_raw_fd(fd) });
        }
        assert_eq!(
            read_json::<String>(&mut target).expect("read commit"),
            "committed"
        );
    }
}
