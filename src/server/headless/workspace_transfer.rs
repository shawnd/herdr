//! Event-loop adapter for live workspace transfer. Blocking PTY/transport work
//! lives in spawn_blocking jobs; only state installation/removal runs here.
//!
//! Wire this module into HeadlessServer with one `Coordinator` field. Call
//! `intercept` before normal API dispatch, `poll` after each wake/drain, and
//! `filter_event` before app event dispatch. Use `workspace_pending` and
//! `pane_pending` to gate client input and organization mutations as well.

#![cfg(unix)]

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::api::schema::WorkspaceTransferImportParams;
use crate::api::ApiRequestMessage;
use crate::app::App;
use crate::events::AppEvent;
use crate::layout::PaneId;
use crate::terminal::TerminalId;

#[path = "workspace_transfer/transport.rs"]
mod transport;
use transport::{
    ExportRuntimes, ImportSettings, Resolution, StagedWorkspace, TransferEvent, TransferManifest,
};

struct Pending {
    message: ApiRequestMessage,
    workspace_id: String,
    session: String,
    destination: PathBuf,
    terminals: Vec<TerminalId>,
    panes: HashSet<PaneId>,
    deferred: Vec<AppEvent>,
    reports: Vec<ApiRequestMessage>,
    public_panes: HashMap<PaneId, String>,
    pane_aliases: HashMap<String, String>,
    committed: bool,
    decision: Arc<AtomicU8>,
}

struct UncertainImport {
    staged: Box<StagedWorkspace>,
    source_socket: PathBuf,
    token: String,
    next_retry: Instant,
    resolving: bool,
}

struct Forward {
    socket: PathBuf,
    workspace_id: String,
}

struct PaneForward {
    source_workspace_id: String,
    pane_id: String,
}

pub(crate) struct Coordinator {
    tx: mpsc::Sender<TransferEvent>,
    rx: mpsc::Receiver<TransferEvent>,
    pending: HashMap<String, Pending>,
    forwarding: HashMap<String, Forward>,
    pane_forwarding: HashMap<String, PaneForward>,
    imported: HashSet<String>,
    imported_panes: HashSet<PaneId>,
    retired_panes: HashSet<PaneId>,
    decisions: HashMap<String, Arc<AtomicU8>>,
    incoming: bool,
    incoming_panes: HashSet<PaneId>,
    incoming_terminals: HashSet<TerminalId>,
    incoming_workspace: Option<String>,
    incoming_reports: Vec<ApiRequestMessage>,
    uncertain: Option<UncertainImport>,
    replayed_events: Vec<AppEvent>,
    replayed_requests: Vec<ApiRequestMessage>,
    stop_control: Arc<AtomicU8>,
}

impl Default for Coordinator {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            tx,
            rx,
            pending: HashMap::new(),
            forwarding: HashMap::new(),
            pane_forwarding: HashMap::new(),
            imported: HashSet::new(),
            imported_panes: HashSet::new(),
            retired_panes: HashSet::new(),
            decisions: HashMap::new(),
            incoming: false,
            incoming_panes: HashSet::new(),
            incoming_terminals: HashSet::new(),
            incoming_workspace: None,
            incoming_reports: Vec::new(),
            uncertain: None,
            replayed_events: Vec::new(),
            replayed_requests: Vec::new(),
            stop_control: Arc::new(AtomicU8::new(0)),
        }
    }
}

impl Coordinator {
    /// Bind to the API handle before serving requests, including after socket restoration.
    pub(crate) fn set_stop_control(&mut self, control: Arc<AtomicU8>) {
        self.stop_control = control;
    }

    fn reserve_server(&self) -> io::Result<()> {
        self.stop_control
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| io::Error::other("server is shutting down or another transfer is pending"))
    }

    pub(crate) fn take_replayed_events(&mut self) -> Vec<AppEvent> {
        std::mem::take(&mut self.replayed_events)
    }

    pub(crate) fn take_replayed_requests(&mut self) -> Vec<ApiRequestMessage> {
        std::mem::take(&mut self.replayed_requests)
    }

    pub(crate) fn busy(&self) -> bool {
        !self.pending.is_empty() || self.incoming
    }

    pub(crate) fn workspace_pending(&self, workspace_id: &str) -> bool {
        self.incoming_workspace.as_deref() == Some(workspace_id)
            || self
                .pending
                .values()
                .any(|p| p.workspace_id == workspace_id)
    }

    pub(crate) fn pane_pending(&self, pane_id: PaneId) -> bool {
        self.incoming_panes.contains(&pane_id)
            || self.pending.values().any(|p| p.panes.contains(&pane_id))
    }

    pub(crate) fn terminal_pending(&self, terminal_id: &TerminalId) -> bool {
        self.incoming_terminals.contains(terminal_id)
            || self
                .pending
                .values()
                .any(|p| p.terminals.contains(terminal_id))
    }

    fn transfer_decision(&self, token: &str) -> (bool, bool) {
        match self
            .decisions
            .get(token)
            .map(|phase| phase.load(Ordering::Acquire))
        {
            Some(1) => (true, false),
            Some(2) => (false, true),
            _ => (false, false),
        }
    }

    fn cancel_transfer(&self, token: &str) -> (bool, bool) {
        if let Some(phase) = self.decisions.get(token) {
            let _ = phase.compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire);
        }
        self.transfer_decision(token)
    }

    fn resolve_request_workspace(&self, app: &App, id: &str) -> Option<String> {
        // Exact inherited pane aliases take precedence over their textual
        // workspace prefix, which may still name an unrelated local workspace.
        if let Some(route) = self.pane_forwarding.get(id) {
            return Some(route.source_workspace_id.clone());
        }
        if let Some(pending) = self
            .pending
            .values()
            .find(|p| p.pane_aliases.contains_key(id))
        {
            return Some(pending.workspace_id.clone());
        }
        if let Some((index, _)) = app.parse_pane_id(id) {
            return app
                .state
                .workspaces
                .get(index)
                .map(|workspace| workspace.id.clone());
        }
        // Named agent/terminal targets must also freeze their actual workspace,
        // rather than slipping through the implicit active-workspace check.
        app.state
            .workspaces
            .iter()
            .find(|workspace| {
                workspace.tabs.iter().any(|tab| {
                    tab.panes.values().any(|pane| {
                        app.state
                            .terminals
                            .get(&pane.attached_terminal_id)
                            .is_some_and(|terminal| {
                                terminal.agent_name.as_deref() == Some(id)
                                    || terminal.id.to_string() == id
                            })
                    })
                })
            })
            .map(|workspace| workspace.id.clone())
    }

    fn exact_forward_rewrites(
        &self,
        params: &Value,
        workspace_id: &str,
    ) -> HashMap<String, String> {
        let mut rewrites = HashMap::new();
        visit_ids(params, &mut |id| {
            if let Some(route) = self
                .pane_forwarding
                .get(id)
                .filter(|route| route.source_workspace_id == workspace_id)
            {
                rewrites.insert(id.to_string(), route.pane_id.clone());
            }
        });
        rewrites
    }

    /// Source detector/child-wait events must never apply to a retired identity.
    /// During prepare retain them for rollback (including child death).
    pub(crate) fn filter_event(&mut self, event: AppEvent) -> Option<AppEvent> {
        let pane = match &event {
            AppEvent::PaneDied { pane_id, .. }
            | AppEvent::AgentProcessDetected { pane_id, .. }
            | AppEvent::CodexPromptObserved { pane_id, .. }
            | AppEvent::StateChanged { pane_id, .. }
            | AppEvent::HookStateReported { pane_id, .. }
            | AppEvent::AgentSessionReported { pane_id, .. }
            | AppEvent::AgentResumeReported { pane_id, .. }
            | AppEvent::ReportedAgentShellReturned { pane_id, .. }
            | AppEvent::HookMetadataReported { pane_id, .. }
            | AppEvent::HookAuthorityCleared { pane_id, .. }
            | AppEvent::HookAgentReleased { pane_id, .. }
            | AppEvent::TerminalBell { pane_id, .. }
            | AppEvent::TerminalCwdReported { pane_id, .. }
            | AppEvent::WorktreeRuntimeRestoreFailed { pane_id, .. } => Some(*pane_id),
            _ => None,
        };
        if let Some(pane) = pane {
            if self.retired_panes.contains(&pane) {
                return None;
            }
            if let Some(pending) = self.pending.values_mut().find(|p| p.panes.contains(&pane)) {
                // Detection producers are bounded and transfers have a deadline.
                if pending.deferred.len() < 4096 {
                    pending.deferred.push(event);
                }
                return None;
            }
        }
        Some(event)
    }

    /// Returns an unhandled request for normal dispatch. Forwarding happens
    /// before local resolution so inherited caller/hook IDs cannot hit a target
    /// session's identically-numbered preexisting workspace.
    pub(crate) fn intercept(
        &mut self,
        app: &mut App,
        message: ApiRequestMessage,
    ) -> Option<ApiRequestMessage> {
        let value = match serde_json::to_value(&message.request) {
            Ok(value) => value,
            Err(_) => return Some(message),
        };
        let method = value["method"].as_str().unwrap_or_default();
        if method == "server.live_handoff"
            && (!self.forwarding.is_empty() || !self.imported.is_empty())
        {
            respond_error(&message, "live server replacement is unavailable while this server retains workspace-transfer routing; inherited process sockets require the original routing server");
            return None;
        }
        if method == "workspace.transfer" {
            if let Err(error) = self.begin(app, &value, message) {
                let (message, error) = *error;
                respond_error(&message, &error);
            }
            return None;
        }
        if method == "workspace.transfer.import" {
            let result =
                serde_json::from_value::<WorkspaceTransferImportParams>(value["params"].clone())
                    .map_err(io::Error::other)
                    .and_then(|params| self.import(app, params));
            match result {
                Ok(()) => respond(&message, json!({"type":"ok"})),
                Err(error) => respond_error(&message, &error.to_string()),
            }
            return None;
        }
        if matches!(
            method,
            "workspace.transfer.status" | "workspace.transfer.cancel"
        ) {
            let token = value["params"]["token"].as_str().unwrap_or_default();
            let (committed, cancelled) = if method == "workspace.transfer.cancel" {
                self.cancel_transfer(token)
            } else {
                self.transfer_decision(token)
            };
            respond(
                &message,
                json!({"type":"workspace_transfer_status", "committed":committed,"cancelled":cancelled}),
            );
            return None;
        }
        if self.incoming && matches!(method, "agent.rename" | "agent.start") {
            respond_error(
                &message,
                "workspace import pending; retry agent name changes after completion",
            );
            return None;
        }
        let ids = request_workspace_ids(&value["params"], |id| {
            self.resolve_request_workspace(app, id)
        });
        if matches!(
            method,
            "pane.report_agent"
                | "pane.report_agent_session"
                | "pane.report_metadata"
                | "pane.clear_agent_authority"
                | "pane.release_agent"
                | "workspace.report_metadata"
        ) {
            if let Some(pending) = self
                .pending
                .values_mut()
                .find(|p| ids.contains(&p.workspace_id))
            {
                if pending.reports.len() < 256 {
                    pending.reports.push(message);
                } else {
                    respond_error(&message, "workspace transfer report queue is full; retry");
                }
                return None;
            }
            if self
                .incoming_workspace
                .as_ref()
                .is_some_and(|workspace| ids.contains(workspace))
            {
                if self.incoming_reports.len() < 256 {
                    self.incoming_reports.push(message);
                } else {
                    respond_error(&message, "workspace activation report queue is full; retry");
                }
                return None;
            }
        }
        let implicit_pending = ids.is_empty()
            && crate::api::request_changes_ui(&message.request)
            && app
                .state
                .active
                .and_then(|i| app.state.workspaces.get(i))
                .is_some_and(|w| self.workspace_pending(&w.id));
        if implicit_pending || ids.iter().any(|id| self.workspace_pending(id)) {
            respond_error(
                &message,
                "workspace transfer pending; retry after completion",
            );
            return None;
        }
        let routes: Vec<_> = ids
            .iter()
            .filter_map(|id| self.forwarding.get(id).map(|f| (id, f)))
            .collect();
        if let Some((old, route)) = routes.first() {
            // Cross-session operations cannot be resolved atomically. Never
            // leave another source-session target to resolve on the destination.
            if ids.iter().any(|id| id != *old) {
                respond_error(&message, "request spans transferred and local workspaces");
            } else {
                let forwarded_agent_probe = method == "agent.get";
                let mut forwarded = value;
                let exact = self.exact_forward_rewrites(&forwarded["params"], old);
                rewrite_request_ids(&mut forwarded["params"], old, &route.workspace_id, &exact);
                let socket = route.socket.clone();
                tokio::task::spawn_blocking(move || {
                    match transport::rpc_forward(&socket, &forwarded) {
                        Ok(mut response) => {
                            if forwarded_agent_probe && response.is_object() {
                                // Waits must not consume identically-numbered local
                                // pane events for an agent resolved on another server.
                                response["workspace_transfer_forwarded"] = Value::Bool(true);
                            }
                            let _ = message.respond_to.send(response.to_string());
                        }
                        Err(error) => respond_forward_error(&message, &error),
                    }
                });
            }
            return None;
        }
        // Session-global mutations can indirectly destroy or move pending panes.
        if self.busy()
            && matches!(
                method,
                "server.stop"
                    | "server.live_handoff"
                    | "layout.apply"
                    | "worktree.remove"
                    | "plugin.unlink"
                    | "plugin.disable"
            )
        {
            respond_error(
                &message,
                "workspace transfer pending; retry after completion",
            );
            return None;
        }
        Some(message)
    }

    fn begin(
        &mut self,
        app: &mut App,
        value: &Value,
        message: ApiRequestMessage,
    ) -> Result<(), Box<(ApiRequestMessage, String)>> {
        let preflight = (|| -> io::Result<_> {
            if self.busy() {
                return Err(io::Error::other("another workspace transfer is pending"));
            }
            if self.forwarding.len() >= 128 {
                return Err(io::Error::other("workspace transfer routing limit reached"));
            }
            if self.decisions.len() >= 1024 {
                return Err(io::Error::other(
                    "workspace transfer decision-journal limit reached",
                ));
            }
            let workspace_id = value["params"]["workspace_id"]
                .as_str()
                .ok_or_else(|| io::Error::other("workspace_id required"))?
                .to_string();
            let session = value["params"]["session"]
                .as_str()
                .ok_or_else(|| io::Error::other("session required"))?
                .to_string();
            crate::session::validate_name(&session).map_err(io::Error::other)?;
            let name =
                (session != crate::session::DEFAULT_SESSION_NAME).then_some(session.as_str());
            let destination = crate::session::api_socket_path_for(name);
            if destination == crate::api::socket_path()
                || crate::session::data_dir_for(name) == crate::session::data_dir()
            {
                return Err(io::Error::other("workspace is already in that session"));
            }
            if self.imported.contains(&workspace_id) || self.forwarding.contains_key(&workspace_id)
            {
                return Err(io::Error::other(
                    "a transferred workspace cannot be transferred again yet",
                ));
            }
            let ws = app
                .state
                .workspaces
                .iter()
                .find(|w| w.id == workspace_id)
                .ok_or_else(|| io::Error::other("workspace not found"))?;
            if ws.worktree_space.is_some() {
                return Err(io::Error::other(
                    "worktree-group workspaces cannot be transferred independently",
                ));
            }
            let panes: HashSet<_> = ws
                .tabs
                .iter()
                .flat_map(|t| t.panes.keys().copied())
                .collect();
            if panes.iter().any(|pane| self.imported_panes.contains(pane)) {
                return Err(io::Error::other(
                    "a previously imported pane cannot be transferred to another session again yet",
                ));
            }
            if app
                .state
                .public_pane_id_aliases
                .values()
                .filter(|pane| panes.contains(*pane))
                .count()
                > 4096
            {
                return Err(io::Error::other(
                    "workspace transfer pane-alias routing limit reached",
                ));
            }
            if panes.iter().any(|p| app.state.plugin_panes.contains_key(p)) {
                return Err(io::Error::other(
                    "workspace has plugin-owned panes; transfer is not supported",
                ));
            }
            let terminals: Vec<_> = ws
                .tabs
                .iter()
                .flat_map(|t| t.panes.values().map(|p| p.attached_terminal_id.clone()))
                .collect();
            if terminals.is_empty()
                || terminals.len() > transport::MAX_PANES
                || terminals.iter().collect::<HashSet<_>>().len() != terminals.len()
            {
                return Err(io::Error::other(
                    "workspace must contain 1..256 distinct live terminals",
                ));
            }
            if terminals
                .iter()
                .any(|id| app.terminal_runtimes.get(id).is_none())
            {
                return Err(io::Error::other(
                    "all workspace panes must have running terminal runtimes",
                ));
            }
            if terminals.iter().any(|id| {
                app.state
                    .terminals
                    .get(id)
                    .is_some_and(|terminal| terminal.managed_agent_launch_pending())
            }) {
                return Err(io::Error::other(
                    "workspace has an agent launch pending; retry transfer after launch settles",
                ));
            }
            let snapshot = crate::persist::capture(
                std::slice::from_ref(ws),
                &app.state.terminals,
                &app.terminal_runtimes,
                Some(0),
                0,
            );
            let pane_entries: Vec<_> = ws
                .tabs
                .iter()
                .flat_map(|t| {
                    t.panes
                        .iter()
                        .map(|(p, state)| (*p, state.attached_terminal_id.clone()))
                })
                .collect();
            let agent_states = pane_entries
                .iter()
                .map(|(p, id)| {
                    (
                        p.raw(),
                        app.state
                            .terminals
                            .get(id)
                            .and_then(|t| t.handoff_agent_state()),
                    )
                })
                .collect();
            let token = transport::new_token()?;
            let manifest = TransferManifest {
                version: 1,
                protocol: crate::protocol::PROTOCOL_VERSION,
                source_socket: crate::api::socket_path(),
                source_workspace_id: workspace_id.clone(),
                snapshot,
                panes: Vec::new(),
                agent_states,
                workspace_tokens: ws.metadata_tokens.capture_transfer(),
                workspace_sequences: ws.metadata_token_sequences.clone(),
                terminal_tokens: pane_entries
                    .iter()
                    .filter_map(|(p, id)| {
                        app.state
                            .terminals
                            .get(id)
                            .map(|t| (p.raw(), t.metadata_tokens.capture_transfer()))
                    })
                    .collect(),
                terminal_metadata: pane_entries
                    .iter()
                    .filter_map(|(p, id)| {
                        app.state
                            .terminals
                            .get(id)
                            .map(|t| (p.raw(), t.capture_transfer_metadata()))
                    })
                    .collect(),
            };
            Ok((
                workspace_id,
                session,
                destination,
                panes,
                terminals,
                pane_entries,
                token,
                manifest,
            ))
        })();
        let (workspace_id, session, destination, panes, terminals, pane_entries, token, manifest) =
            match preflight {
                Ok(result) => result,
                Err(error) => return Err(Box::new((message, error.to_string()))),
            };
        if let Err(error) = self.reserve_server() {
            return Err(Box::new((message, error.to_string())));
        }
        let mut runtimes = ExportRuntimes::default();
        let decision = Arc::new(AtomicU8::new(0));
        self.decisions.insert(token.clone(), decision.clone());
        let public_panes: HashMap<PaneId, String> = app
            .state
            .workspaces
            .iter()
            .find(|w| w.id == workspace_id)
            .map(|workspace| {
                workspace
                    .public_pane_numbers
                    .iter()
                    .map(|(pane, number)| {
                        (
                            *pane,
                            crate::workspace::public_pane_id_for_number(&workspace_id, *number),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        let pane_aliases = app
            .state
            .public_pane_id_aliases
            .iter()
            .filter(|(_, pane)| panes.contains(*pane))
            .filter_map(|(alias, pane)| {
                public_panes
                    .get(pane)
                    .map(|canonical| (alias.clone(), canonical.clone()))
            })
            .collect();
        for (pane, id) in pane_entries {
            if let Some(runtime) = app.terminal_runtimes.remove(&id) {
                runtimes.0.push((pane, id, runtime));
            }
        }
        self.pending.insert(
            token.clone(),
            Pending {
                message,
                workspace_id,
                session: session.clone(),
                destination: destination.clone(),
                terminals,
                panes,
                deferred: Vec::new(),
                reports: Vec::new(),
                public_panes,
                pane_aliases,
                committed: false,
                decision: decision.clone(),
            },
        );
        transport::export(
            token,
            destination,
            manifest,
            runtimes,
            self.tx.clone(),
            app.render_notify.clone(),
            decision,
            session,
        );
        Ok(())
    }

    fn import(&mut self, app: &App, params: WorkspaceTransferImportParams) -> io::Result<()> {
        if self.busy() {
            return Err(io::Error::other("another workspace transfer is pending"));
        }
        transport::validate_socket(&params.socket_path)?;
        if params.token.len() != 64 || !params.token.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(io::Error::other("invalid workspace transfer token"));
        }
        let workspace_id = loop {
            let id = crate::workspace::generate_workspace_id();
            if !app.state.workspaces.iter().any(|w| w.id == id)
                && !self.forwarding.contains_key(&id)
            {
                break id;
            }
        };
        self.reserve_server()?;
        self.incoming_workspace = Some(workspace_id.clone());
        let settings = ImportSettings {
            workspace_id,
            scrollback_limit_bytes: app.state.pane_scrollback_limit_bytes,
            default_shell: app.state.default_shell.clone(),
            shell_mode: app.state.shell_mode,
            events: app.event_tx.clone(),
            render_notify: app.render_notify.clone(),
            render_dirty: app.render_dirty.clone(),
        };
        self.incoming = true;
        transport::import(params, settings, self.tx.clone());
        Ok(())
    }

    /// Return true when workspace topology changed so the parent invalidates
    /// client snapshots and requests a full render. Poll on every server wake.
    pub(crate) fn poll(&mut self, app: &mut App) -> bool {
        if let Some(uncertain) = self.uncertain.as_mut() {
            if !uncertain.resolving && Instant::now() >= uncertain.next_retry {
                uncertain.resolving = true;
                transport::retry_resolution(
                    uncertain.source_socket.clone(),
                    uncertain.token.clone(),
                    self.tx.clone(),
                    app.render_notify.clone(),
                );
            }
        }
        let mut changed = false;
        while let Ok(event) = self.rx.try_recv() {
            match event {
                TransferEvent::TargetValidateNames { names, validated } => {
                    let _ = validated.send(validate_import_agent_names(app, &names));
                }
                TransferEvent::SourcePrepared {
                    token,
                    workspace_id,
                    decide,
                    decision,
                } => {
                    if let Some(pending) = self.pending.get_mut(&token) {
                        if let Some(index) = app
                            .state
                            .workspaces
                            .iter()
                            .position(|w| w.id == pending.workspace_id)
                        {
                            // A delayed queued event cannot commit after transport
                            // already timed out and elected rollback.
                            if decision
                                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                                .is_err()
                            {
                                continue;
                            }
                            // The event-loop owns the commit point. Record it before
                            // notifying transport; EOF after this point is never rollback.
                            for (alias, canonical) in &pending.pane_aliases {
                                self.pane_forwarding.insert(
                                    alias.clone(),
                                    PaneForward {
                                        source_workspace_id: pending.workspace_id.clone(),
                                        pane_id: canonical.replacen(
                                            &pending.workspace_id,
                                            &workspace_id,
                                            1,
                                        ),
                                    },
                                );
                            }
                            self.forwarding.insert(
                                pending.workspace_id.clone(),
                                Forward {
                                    socket: pending.destination.clone(),
                                    workspace_id,
                                },
                            );
                            self.retired_panes.extend(pending.panes.iter().copied());
                            app.state
                                .public_pane_id_aliases
                                .retain(|_, pane| !pending.panes.contains(pane));
                            app.state
                                .pane_id_aliases
                                .retain(|_, pane| !pending.panes.contains(pane));
                            let removed = app.state.workspaces.remove(index);
                            app.emit_workspace_transfer_out(&removed);
                            for id in &pending.terminals {
                                app.state.terminals.remove(id);
                                app.state.direct_attach_resize_locks.remove(id);
                            }
                            adjust_selection_after_remove(app, index);
                            pending.committed = true;
                            app.state.session_dirty = true;
                            app.checkpoint_session_after_transfer();
                            let _ = decide.send(true);
                            changed = true;
                        } else {
                            let _ = decide.send(false);
                        }
                    } else {
                        let _ = decide.send(false);
                    }
                }
                TransferEvent::SourceFinished {
                    token,
                    mut runtimes,
                    result,
                } => {
                    if let Some(pending) = self.pending.remove(&token) {
                        if !pending.committed {
                            let _ = pending.decision.compare_exchange(
                                0,
                                2,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            );
                            for (_, id, runtime) in runtimes.0.drain(..) {
                                app.terminal_runtimes.insert(id, runtime);
                            }
                            self.replayed_events.extend(pending.deferred);
                            self.replayed_requests.extend(pending.reports);
                        } else if let Some(route) = self.forwarding.get(&pending.workspace_id) {
                            let socket = route.socket.clone();
                            let old = pending.workspace_id.clone();
                            let new = route.workspace_id.clone();
                            let exact: HashMap<_, _> = pending
                                .pane_aliases
                                .into_iter()
                                .map(|(alias, canonical)| {
                                    (alias, canonical.replacen(&old, &new, 1))
                                })
                                .collect();
                            let hooks: Vec<_> = pending
                                .deferred
                                .into_iter()
                                .filter_map(|event| {
                                    deferred_hook_request(event, &pending.public_panes)
                                })
                                .collect();
                            let reports = pending.reports;
                            // Keep report order across the prepare window; these
                            // producers must not lose the final idle/session hook.
                            tokio::task::spawn_blocking(move || {
                                for mut hook in hooks {
                                    rewrite_request_ids(&mut hook["params"], &old, &new, &exact);
                                    if let Err(error) = transport::rpc(&socket, &hook) {
                                        tracing::warn!(%error, "deferred transfer hook forwarding failed");
                                    }
                                }
                                for report in reports {
                                    let mut value = match serde_json::to_value(&report.request) {
                                        Ok(value) => value,
                                        Err(error) => {
                                            respond_error(&report, &error.to_string());
                                            continue;
                                        }
                                    };
                                    rewrite_request_ids(&mut value["params"], &old, &new, &exact);
                                    match transport::rpc(&socket, &value) {
                                        Ok(response) => {
                                            let _ = report.respond_to.send(response.to_string());
                                        }
                                        Err(error) => respond_error(&report, &error.to_string()),
                                    }
                                }
                            });
                        }
                        match result {
                            Ok(workspace_id) => respond(&pending.message, json!({"type":"workspace_transferred", "workspace_id":workspace_id, "session":pending.session})),
                            Err(error) if pending.committed => respond_error(&pending.message, &format!("workspace transfer committed; destination confirmation unavailable: {error}")),
                            Err(error) => respond_error(&pending.message, &error.to_string()),
                        }
                        changed = true;
                    }
                }
                TransferEvent::TargetCommitted { staged, installed } => {
                    let mut staged = *staged;
                    let workspace_id = staged.workspace.id.clone();
                    self.incoming_workspace = Some(workspace_id.clone());
                    // Fresh raw PaneId/TerminalId allocation is performed by
                    // restore_handoff; reject a retransfer via the imported set.
                    self.imported.insert(workspace_id);
                    self.incoming_panes.extend(
                        staged
                            .workspace
                            .tabs
                            .iter()
                            .flat_map(|tab| tab.panes.keys().copied()),
                    );
                    self.incoming_terminals
                        .extend(staged.terminals.keys().cloned());
                    self.imported_panes.extend(
                        staged
                            .workspace
                            .tabs
                            .iter()
                            .flat_map(|tab| tab.panes.keys().copied()),
                    );
                    let mut resumes = Vec::new();
                    for (id, mut runtime) in staged.runtimes.drain() {
                        if let Some(terminal) = staged.terminals.get(&id) {
                            runtime.set_full_lifecycle_authority_active(
                                terminal.full_lifecycle_hook_authority_active(),
                            );
                            runtime.set_self_reported_agent_active(
                                terminal.self_reported_agent_active(),
                            );
                        }
                        runtime.apply_host_terminal_theme(app.state.host_terminal_theme);
                        runtime.apply_host_terminal_appearance(app.state.host_terminal_appearance);
                        runtime.assume_handoff_ownership();
                        resumes.push(runtime.resume_handoff_reader_after_commit());
                        runtime.nudge_child_redraw_after_handoff();
                        app.terminal_runtimes.insert(id, runtime);
                    }
                    app.state.terminals.extend(staged.terminals.drain());
                    app.state.workspaces.push(staged.workspace);
                    let workspace_index = app.state.workspaces.len() - 1;
                    if app.state.active.is_none() {
                        app.state.active = Some(workspace_index);
                    }
                    app.emit_workspace_transfer_in(workspace_index);
                    app.state.session_dirty = true;
                    app.checkpoint_session_after_transfer();
                    transport::wait_activation(
                        resumes,
                        installed,
                        self.tx.clone(),
                        app.render_notify.clone(),
                    );
                    changed = true;
                }
                TransferEvent::TargetFinished { error } => {
                    self.incoming = false;
                    if let Some(error) = error {
                        self.clear_incoming_reports(&error);
                        tracing::warn!(%error, "workspace import failed before installation");
                    }
                }
                TransferEvent::TargetActivated { installed, result } => {
                    self.incoming_panes.clear();
                    self.incoming_terminals.clear();
                    self.incoming_workspace = None;
                    self.replayed_requests.append(&mut self.incoming_reports);
                    if let Err(error) = &result {
                        tracing::warn!(%error,"workspace committed but PTY activation acknowledgement unavailable");
                    }
                    if installed.send(result).is_err() {
                        self.incoming = false;
                    }
                    changed = true;
                }
                TransferEvent::TargetUncertain {
                    staged,
                    source_socket,
                    token,
                    error,
                } => {
                    tracing::warn!(%error,"retaining quiesced workspace staging until source decision is resolved");
                    self.incoming_workspace = Some(staged.workspace.id.clone());
                    self.uncertain = Some(UncertainImport {
                        staged,
                        source_socket,
                        token,
                        next_retry: Instant::now() + Duration::from_secs(1),
                        resolving: false,
                    });
                }
                TransferEvent::TargetResolution { resolution } => {
                    match resolution {
                        Resolution::Unresolved(_) => {
                            if let Some(uncertain) = self.uncertain.as_mut() {
                                uncertain.resolving = false;
                                uncertain.next_retry = Instant::now() + Duration::from_secs(1);
                            }
                        }
                        Resolution::Cancelled => {
                            // Only a source CAS to its terminal cancelled state
                            // permits discarding non-owning staged descriptors.
                            self.uncertain.take();
                            self.incoming = false;
                            self.clear_incoming_reports("workspace import cancelled by source");
                        }
                        Resolution::Committed => {
                            if let Some(uncertain) = self.uncertain.take() {
                                let (installed, _ack) = mpsc::channel();
                                let _ = self.tx.send(TransferEvent::TargetCommitted {
                                    staged: uncertain.staged,
                                    installed,
                                });
                            }
                        }
                    }
                }
            }
        }
        if !self.busy() {
            let _ = self
                .stop_control
                .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire);
        }
        changed
    }

    fn clear_incoming_reports(&mut self, error: &str) {
        self.incoming_workspace = None;
        self.incoming_panes.clear();
        self.incoming_terminals.clear();
        for report in self.incoming_reports.drain(..) {
            respond_error(&report, error);
        }
    }
}

fn validate_import_agent_names(app: &App, names: &[String]) -> Result<(), String> {
    let existing: HashSet<_> = app
        .state
        .terminals
        .values()
        .filter_map(|terminal| terminal.agent_name.as_deref())
        .collect();
    let mut incoming = HashSet::new();
    for name in names {
        if existing.contains(name.as_str()) {
            return Err(format!("workspace transfer rejected: agent name '{name}' already exists in the destination session"));
        }
        if !incoming.insert(name.as_str()) {
            return Err(format!("workspace transfer rejected: source workspace contains duplicate agent name '{name}'"));
        }
    }
    Ok(())
}

fn adjust_selection_after_remove(app: &mut App, index: usize) {
    let len = app.state.workspaces.len();
    app.state.active = app.state.active.and_then(|active| {
        if len == 0 {
            None
        } else {
            Some(if active > index {
                active - 1
            } else {
                active.min(len - 1)
            })
        }
    });
    app.state.selected = if app.state.selected > index {
        app.state.selected - 1
    } else {
        app.state.selected.min(len.saturating_sub(1))
    };
    app.state.previous_pane_focus = None;
}

fn respond(message: &ApiRequestMessage, result: Value) {
    let _ = message
        .respond_to
        .send(json!({"id":message.request.id, "result":result}).to_string());
}

fn respond_error(message: &ApiRequestMessage, error: &str) {
    let _ = message.respond_to.send(json!({"id":message.request.id,"error":{"code":"workspace_transfer_failed","message":error}}).to_string());
}

fn respond_forward_error(message: &ApiRequestMessage, error: &transport::ForwardError) {
    let (code, detail) = if error.submitted {
        ("forwarded_request_outcome_unknown",format!("request was sent to the transferred workspace but its response was not received; remote outcome is unknown: {error}"))
    } else {
        (
            "destination_unavailable",
            format!(
                "transferred workspace destination unavailable before request submission: {error}"
            ),
        )
    };
    let _ = message
        .respond_to
        .send(json!({"id":message.request.id,"error":{"code":code,"message":detail}}).to_string());
}

fn deferred_hook_request(event: AppEvent, panes: &HashMap<PaneId, String>) -> Option<Value> {
    let (pane, method, mut params, session) = match event {
        AppEvent::HookStateReported {
            pane_id,
            source,
            agent_label,
            state,
            message,
            seq,
            session_ref,
        } => (
            pane_id,
            "pane.report_agent",
            json!({"source":source,"agent":agent_label,"state":state,"message":message,"seq":seq}),
            session_ref,
        ),
        AppEvent::AgentSessionReported {
            pane_id,
            source,
            agent_label,
            seq,
            session_ref,
            session_start_source,
        } => (
            pane_id,
            "pane.report_agent_session",
            json!({"source":source,"agent":agent_label,"seq":seq,"session_start_source":session_start_source}),
            session_ref,
        ),
        AppEvent::AgentResumeReported {
            pane_id,
            source,
            agent_label,
            seq,
            argv,
        } => (
            pane_id,
            "pane.report_agent_session",
            json!({"source":source,"agent":agent_label,"seq":seq,"resume_argv":argv}),
            None,
        ),
        AppEvent::HookMetadataReported {
            pane_id,
            source,
            agent_label,
            applies_to_source,
            title,
            display_agent,
            state_labels,
            clear_title,
            clear_display_agent,
            clear_state_labels,
            seq,
            ttl,
        } => (
            pane_id,
            "pane.report_metadata",
            json!({"source":source,"agent":agent_label,"applies_to_source":applies_to_source,"title":title,"display_agent":display_agent,"state_labels":state_labels,"clear_title":clear_title,"clear_display_agent":clear_display_agent,"clear_state_labels":clear_state_labels,"seq":seq,"ttl_ms":ttl.map(|ttl|ttl.as_millis() as u64)}),
            None,
        ),
        AppEvent::HookAuthorityCleared {
            pane_id,
            source,
            seq,
        } => (
            pane_id,
            "pane.clear_agent_authority",
            json!({"source":source,"seq":seq}),
            None,
        ),
        AppEvent::HookAgentReleased {
            pane_id,
            source,
            agent_label,
            seq,
            ..
        } => (
            pane_id,
            "pane.release_agent",
            json!({"source":source,"agent":agent_label,"seq":seq}),
            None,
        ),
        _ => return None,
    };
    params["pane_id"] = json!(panes.get(&pane)?);
    if let Some(session) = session {
        let key = match session.kind {
            crate::agent_resume::AgentSessionRefKind::Id => "agent_session_id",
            crate::agent_resume::AgentSessionRefKind::Path => "agent_session_path",
        };
        params[key] = json!(session.value);
    }
    Some(json!({"id":"transfer:deferred-hook","method":method,"params":params}))
}

fn identity_key(key: &str) -> bool {
    matches!(
        key,
        "workspace_id"
            | "source_workspace_id"
            | "before_workspace_id"
            | "workspace_ids"
            | "pane_id"
            | "pane_ids"
            | "tab_id"
            | "target"
            | "caller_pane_id"
            | "source_pane_id"
            | "target_pane_id"
            | "focused_pane_id"
            | "neighbor_pane_id"
    )
}

fn opaque_payload(key: &str) -> bool {
    matches!(
        key,
        "tokens"
            | "state_labels"
            | "env"
            | "args"
            | "resume_argv"
            | "payload"
            | "data"
            | "metadata"
            | "arguments"
            | "params"
    )
}

fn request_workspace_ids(
    value: &Value,
    resolve: impl Fn(&str) -> Option<String>,
) -> HashSet<String> {
    let mut ids = HashSet::new();
    visit_ids(value, &mut |id| {
        if let Some(workspace) = resolve(id) {
            ids.insert(workspace);
            return;
        }
        let ws = id.split(':').next().unwrap_or_default();
        if crate::workspace::public_workspace_number(ws).is_some() {
            ids.insert(ws.to_string());
        }
    });
    ids
}

fn visit_ids(value: &Value, visitor: &mut impl FnMut(&str)) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if opaque_payload(key) {
                    continue;
                }
                if identity_key(key) {
                    if let Some(id) = value.as_str() {
                        visitor(id);
                    }
                    if let Some(values) = value.as_array() {
                        for value in values {
                            if let Some(id) = value.as_str() {
                                visitor(id);
                            }
                        }
                    }
                } else {
                    visit_ids(value, visitor);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                visit_ids(value, visitor);
            }
        }
        _ => {}
    }
}

fn rewrite_request_ids(value: &mut Value, old: &str, new: &str, exact: &HashMap<String, String>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if opaque_payload(key) {
                    continue;
                }
                if identity_key(key) {
                    rewrite_identity(value, old, new, exact);
                } else {
                    rewrite_request_ids(value, old, new, exact);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                rewrite_request_ids(value, old, new, exact);
            }
        }
        _ => {}
    }
}

fn rewrite_identity(value: &mut Value, old: &str, new: &str, exact: &HashMap<String, String>) {
    if let Some(id) = value.as_str() {
        if let Some(canonical) = exact.get(id) {
            *value = Value::String(canonical.clone());
        } else if id == old
            || id
                .strip_prefix(old)
                .is_some_and(|suffix| suffix.starts_with(':'))
        {
            *value = Value::String(format!("{new}{}", &id[old.len()..]));
        }
    } else if let Some(values) = value.as_array_mut() {
        for value in values {
            rewrite_identity(value, old, new, exact);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_app() -> App {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        App::new(
            &crate::config::Config::default(),
            crate::app::AppPolicy::TEST,
            None,
            api_rx,
            crate::api::EventHub::default(),
        )
    }

    #[test]
    fn transfer_and_shutdown_cannot_both_reserve_the_server() {
        let phase = Arc::new(AtomicU8::new(0));
        let mut coordinator = Coordinator::default();
        coordinator.set_stop_control(phase.clone());
        coordinator
            .reserve_server()
            .expect("transfer elected first");
        assert_eq!(
            phase.compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire),
            Err(1)
        );
        coordinator.poll(&mut test_app());
        assert_eq!(
            phase.compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire),
            Ok(0)
        );
        assert!(
            coordinator.reserve_server().is_err(),
            "accepted shutdown cannot race a new transfer"
        );
        coordinator.poll(&mut test_app());
        assert_eq!(
            phase.load(Ordering::Acquire),
            2,
            "idle polling cannot undo shutdown"
        );
    }

    #[test]
    fn payload_maps_are_opaque_to_identity_extraction_and_rewriting() {
        let mut params = json!({
            "pane_id":"w1:p1",
            "destination":{"workspace_id":"w1"},
            "tokens":{"pane_id":"w1:p1","workspace_id":"w99","caller_pane_id":"w2:p1"},
            "state_labels":{"pane_id":"w1:p1","workspace_id":"w98"},
            "env":{"workspace_id":"w97"},
            "args":{"target":{"pane_id":"w96:p1"}}
        });
        let original = params.clone();
        assert_eq!(
            request_workspace_ids(&params, |_| None),
            HashSet::from(["w1".into()])
        );
        rewrite_request_ids(
            &mut params,
            "w1",
            "w3",
            &HashMap::from([("w1:p1".into(), "w3:p9".into())]),
        );
        assert_eq!(params["pane_id"], "w3:p9");
        assert_eq!(params["destination"]["workspace_id"], "w3");
        for key in ["tokens", "state_labels", "env", "args"] {
            assert_eq!(params[key], original[key]);
        }
    }

    #[tokio::test]
    async fn cancellation_prevents_a_queued_prepared_event_from_committing_later() {
        let mut app = test_app();
        let mut workspace = crate::workspace::Workspace::test_new("source");
        workspace.id = "w1".into();
        app.state.workspaces = vec![workspace];
        let phase = Arc::new(AtomicU8::new(0));
        let mut coordinator = Coordinator::default();
        coordinator.decisions.insert("token".into(), phase.clone());
        let (transfer, _response) = message(
            json!({"id":"transfer","method":"workspace.transfer","params":{"workspace_id":"w1","session":"target"}}),
        );
        coordinator.pending.insert(
            "token".into(),
            Pending {
                message: transfer,
                workspace_id: "w1".into(),
                session: "target".into(),
                destination: "/unused".into(),
                terminals: Vec::new(),
                panes: HashSet::new(),
                deferred: Vec::new(),
                reports: Vec::new(),
                public_panes: HashMap::new(),
                pane_aliases: HashMap::new(),
                committed: false,
                decision: phase.clone(),
            },
        );
        let (decide, decision) = mpsc::channel();
        assert!(coordinator
            .tx
            .send(TransferEvent::SourcePrepared {
                token: "token".into(),
                workspace_id: "w2".into(),
                decide,
                decision: phase.clone()
            })
            .is_ok());
        assert_eq!(
            coordinator.transfer_decision("token"),
            (false, false),
            "pending status is not cancellation confirmation"
        );
        assert_eq!(coordinator.cancel_transfer("token"), (false, true));
        assert!(!coordinator.poll(&mut app));
        assert_eq!(app.state.workspaces.len(), 1);
        assert_eq!(app.state.workspaces[0].id, "w1");
        assert!(!matches!(decision.try_recv(), Ok(true)));
        assert!(phase
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err());
        assert_eq!(coordinator.cancel_transfer("unknown"), (false, false));
        let committed = Arc::new(AtomicU8::new(1));
        coordinator.decisions.insert("committed".into(), committed);
        assert_eq!(
            coordinator.cancel_transfer("committed"),
            (true, false),
            "cancellation cannot reverse an elected commit"
        );
    }

    #[tokio::test]
    async fn unresolved_source_keeps_staged_runtime_alive_until_cancellation_is_confirmed() {
        let mut app = test_app();
        let mut coordinator = Coordinator {
            incoming: true,
            ..Coordinator::default()
        };
        let (runtime, mut reader) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
        let id = TerminalId::alloc();
        let staged = StagedWorkspace {
            workspace: crate::workspace::Workspace::test_new("staged"),
            terminals: HashMap::new(),
            runtimes: HashMap::from([(id, runtime)]),
        };
        assert!(coordinator
            .tx
            .send(TransferEvent::TargetUncertain {
                staged: Box::new(staged),
                source_socket: "/unused".into(),
                token: "token".into(),
                error: "source unavailable".into()
            })
            .is_ok());
        coordinator.poll(&mut app);
        let incoming_workspace = coordinator
            .incoming_workspace
            .clone()
            .expect("staged identity");
        let (report, report_response) = message(
            json!({"id":"retained","method":"workspace.report_metadata","params":{"workspace_id":incoming_workspace,"source":"test","tokens":{"summary":"queued"}}}),
        );
        assert!(coordinator.intercept(&mut app, report).is_none());
        assert!(matches!(
            report_response.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        coordinator
            .uncertain
            .as_mut()
            .expect("retained staging")
            .next_retry = Instant::now() + Duration::from_secs(3600);
        assert!(!reader.is_closed());
        assert!(coordinator
            .tx
            .send(TransferEvent::TargetResolution {
                resolution: Resolution::Unresolved("pending/unknown".into())
            })
            .is_ok());
        coordinator.poll(&mut app);
        assert!(coordinator.uncertain.is_some());
        assert!(
            !reader.is_closed(),
            "unknown status must not close staging I/O"
        );
        assert!(coordinator
            .tx
            .send(TransferEvent::TargetResolution {
                resolution: Resolution::Cancelled
            })
            .is_ok());
        // Prevent the retry timer from making this deterministic state test do I/O.
        coordinator
            .uncertain
            .as_mut()
            .expect("retained staging")
            .next_retry = Instant::now() + Duration::from_secs(3600);
        coordinator.poll(&mut app);
        assert!(coordinator.uncertain.is_none());
        assert!(tokio::time::timeout(Duration::from_secs(2), reader.recv())
            .await
            .expect("staging closes after confirmed cancellation")
            .is_none());
        assert!(!coordinator.busy());
        assert!(coordinator.incoming_workspace.is_none());
        assert!(coordinator.incoming_reports.is_empty());
        let reply: Value = serde_json::from_str(
            &report_response
                .recv_timeout(Duration::from_secs(1))
                .expect("cancelled report response"),
        )
        .expect("JSON reply");
        assert!(reply["error"]["message"]
            .as_str()
            .expect("error")
            .contains("cancelled"));
    }

    fn message(value: Value) -> (ApiRequestMessage, mpsc::Receiver<String>) {
        let (respond_to, response) = mpsc::channel();
        (
            ApiRequestMessage {
                request: serde_json::from_value(value).expect("unit request"),
                respond_to,
                response_write_complete: None,
            },
            response,
        )
    }

    #[tokio::test]
    async fn destination_validates_current_names_and_blocks_name_changes_until_import_finishes() {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &crate::config::Config::default(),
            crate::app::AppPolicy::TEST,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        let mut coordinator = Coordinator {
            incoming: true,
            ..Coordinator::default()
        };
        let mut terminal = crate::terminal::TerminalState::new(TerminalId::alloc(), "/tmp".into());
        terminal.set_agent_name("reviewer".into());
        app.state.terminals.insert(terminal.id.clone(), terminal);
        let (validated, validation) = mpsc::channel();
        assert!(coordinator
            .tx
            .send(TransferEvent::TargetValidateNames {
                names: vec!["reviewer".into()],
                validated
            })
            .is_ok());
        assert!(!coordinator.poll(&mut app));
        assert!(validation
            .recv()
            .expect("validation reply")
            .expect_err("duplicate must reject")
            .contains("reviewer"));
        for value in [
            json!({"id":"rename","method":"agent.rename","params":{"target":"reviewer","name":"another-name"}}),
            json!({"id":"start","method":"agent.start","params":{"pane_id":"w1:p1","name":"another-name","kind":"pi"}}),
        ] {
            let (request, response) = message(value);
            assert!(coordinator.intercept(&mut app, request).is_none());
            assert!(response
                .recv()
                .expect("name mutation rejection")
                .contains("import pending"));
        }
        app.state.terminals.clear();
        assert!(
            validate_import_agent_names(&app, &["reviewer".into(), "reviewer".into()])
                .expect_err("duplicate source names")
                .contains("source workspace")
        );
        assert!(
            validate_import_agent_names(&app, &["reviewer".into(), "implementer".into()]).is_ok()
        );
    }

    #[tokio::test]
    async fn inherited_tab_alias_is_frozen_and_reports_replay_through_headless_after_rollback() {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &crate::config::Config::default(),
            crate::app::AppPolicy::TEST,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        let mut local = crate::workspace::Workspace::test_new("local");
        local.id = "w1".into();
        let mut exporting = crate::workspace::Workspace::test_new("exporting");
        exporting.id = "w2".into();
        app.state.workspaces = vec![local, exporting];
        app.state.active = Some(0);
        let pane = PaneId::from_raw(999);
        let (transfer, _) = message(
            json!({"id":"transfer","method":"workspace.transfer","params":{"workspace_id":"w2","session":"target"}}),
        );
        let mut coordinator = Coordinator::default();
        coordinator.pending.insert(
            "token".into(),
            Pending {
                message: transfer,
                workspace_id: "w2".into(),
                session: "target".into(),
                destination: "/unused-test-socket".into(),
                terminals: Vec::new(),
                panes: HashSet::from([pane]),
                deferred: Vec::new(),
                reports: Vec::new(),
                public_panes: HashMap::from([(pane, "w2:p2".into())]),
                pane_aliases: HashMap::from([("w1:p1".into(), "w2:p2".into())]),
                committed: false,
                decision: Arc::new(AtomicU8::new(0)),
            },
        );
        let (input, response) = message(
            json!({"id":"input","method":"pane.send_input","params":{"pane_id":"w1:p1","text":"blocked"}}),
        );
        assert!(coordinator.intercept(&mut app, input).is_none());
        assert!(
            serde_json::from_str::<Value>(&response.recv().expect("pending rejection"))
                .expect("response")
                .get("error")
                .is_some()
        );
        let (report, response) = message(
            json!({"id":"hook","method":"pane.report_metadata","params":{"pane_id":"w1:p1","source":"test","tokens":{"route":"deferred"}}}),
        );
        assert!(coordinator.intercept(&mut app, report).is_none());
        assert!(
            matches!(response.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "pending alias hook must remain queued, not rejected or dispatched locally"
        );
        let (rename, _) = message(
            json!({"id":"local","method":"workspace.rename","params":{"workspace_id":"w1","label":"local"}}),
        );
        assert!(
            coordinator.intercept(&mut app, rename).is_some(),
            "the original workspace must remain local and unfrozen"
        );
        assert!(coordinator
            .filter_event(AppEvent::TerminalBell {
                pane_id: pane,
                count: 1
            })
            .is_none());
        assert!(coordinator
            .tx
            .send(TransferEvent::SourceFinished {
                token: "token".into(),
                runtimes: ExportRuntimes::default(),
                result: Err(io::Error::other("unit rollback"))
            })
            .is_ok());
        assert!(coordinator.poll(&mut app));
        assert!(!coordinator.workspace_pending("w2"));
        let requests = coordinator.take_replayed_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            serde_json::to_value(&requests[0].request).expect("request")["params"]["pane_id"],
            "w1:p1"
        );
        assert_eq!(coordinator.take_replayed_events().len(), 1);
        assert!(coordinator.take_replayed_requests().is_empty());
    }

    #[test]
    fn forwarding_rewrites_only_identity_fields_and_respects_workspace_boundaries() {
        let mut params = json!({"pane_id":"w1:p2","caller_pane_id":"w1:p1", "target_pane_id":"w11:p1", "text":"w1:p2", "workspace_id":"w1"});
        rewrite_request_ids(&mut params, "w1", "w3", &HashMap::new());
        assert_eq!(params["pane_id"], "w3:p2");
        assert_eq!(params["caller_pane_id"], "w3:p1");
        assert_eq!(params["target_pane_id"], "w11:p1");
        assert_eq!(params["text"], "w1:p2");
        assert_eq!(params["workspace_id"], "w3");
    }

    #[test]
    fn multi_workspace_requests_are_identified_before_forwarding() {
        let ids = request_workspace_ids(
            &json!({"pane_id":"w1:p2","caller_pane_id":"w2:p1", "text":"w3:p1"}),
            |_| None,
        );
        assert_eq!(ids, HashSet::from(["w1".into(), "w2".into()]));
    }

    #[test]
    fn tab_transfer_alias_routes_only_the_exported_pane_not_its_original_workspace() {
        let aliases = HashMap::from([("w1:p1".to_string(), "w3:p2".to_string())]);
        let mut params = json!({"pane_id":"w1:p1","caller_pane_id":"w1:p1","text":"w1:p1"});
        let resolve = |id: &str| aliases.contains_key(id).then(|| "w2".to_string());
        assert_eq!(
            request_workspace_ids(&params, resolve),
            HashSet::from(["w2".into()])
        );
        rewrite_request_ids(&mut params, "w2", "w3", &aliases);
        assert_eq!(params["pane_id"], "w3:p2");
        assert_eq!(params["caller_pane_id"], "w3:p2");
        assert_eq!(params["text"], "w1:p1");
        // The old workspace and its other panes remain local. Mixing them with
        // an exported alias must be rejected before transport rewrites anything.
        let mixed = json!({"pane_id":"w1:p1","workspace_id":"w1","target_pane_id":"w1:p2"});
        assert_eq!(
            request_workspace_ids(&mixed, resolve),
            HashSet::from(["w1".into(), "w2".into()])
        );
    }
}
