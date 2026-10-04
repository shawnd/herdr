use super::*;
use crate::api::schema::{
    EmptyParams, Method, ResponseResult, TabTransferParams, WorkspaceTransferParams,
};

impl ClientTransferOverlay {
    fn method(&self, destination: String) -> Method {
        match &self.source {
            ClientTransferSource::Tab { tab_id, .. } => Method::TabTransfer(TabTransferParams {
                tab_id: tab_id.clone(),
                workspace_id: destination,
                focus: true,
            }),
            ClientTransferSource::Workspace { workspace_id } => {
                Method::WorkspaceTransfer(WorkspaceTransferParams {
                    workspace_id: workspace_id.clone(),
                    session: destination,
                })
            }
        }
    }
}

impl ClientShellState {
    pub(super) fn begin_transfer(
        &mut self,
        source: ClientTransferSource,
        outcome: &mut ClientShellInput,
    ) {
        let Some(snapshot) = self.snapshot.as_deref() else {
            return;
        };
        let entries = match &source {
            ClientTransferSource::Tab { workspace_id, .. } => snapshot
                .workspaces
                .iter()
                .filter(|workspace| &workspace.workspace_id != workspace_id)
                .map(|workspace| ClientTransferDestination {
                    id: workspace.workspace_id.clone(),
                    label: workspace.label.clone(),
                    detail: workspace.new_workspace_cwd.clone(),
                })
                .collect(),
            ClientTransferSource::Workspace { .. } => Vec::new(),
        };
        let loading = matches!(source, ClientTransferSource::Workspace { .. });
        let picker_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        let mut picker = ClientTransferOverlay {
            picker_id,
            endpoint_id: self.active_endpoint_id.clone(),
            boot_id: snapshot.boot_id.clone(),
            source,
            entries,
            query: TextEditor::default(),
            search_focused: false,
            selected: 0,
            loading,
            submitting: false,
            error: None,
        };
        let method = picker.method(String::new());
        if !self.supports_endpoint_method(&method) {
            let name = crate::api::api_method_name(&method);
            let message = format!(
                "This server does not support {name}. Update and restart it to enable this action."
            );
            self.push_endpoint_notice(
                ClientEndpointNoticeKind::Unsupported,
                name,
                "Action unavailable",
                message.clone(),
            );
            picker.error = Some(message);
            picker.loading = false;
        }
        let list_sessions = picker.loading;
        self.overlay = Some(ClientShellOverlay::Transfer(picker));
        if list_sessions
            && !self.push_endpoint_method_with_kind(
                Method::SessionList(EmptyParams {}),
                PendingEndpointKind::TransferSessions {
                    picker_id,
                    endpoint_id: self.active_endpoint_id.clone(),
                },
                outcome,
            )
        {
            if let Some(ClientShellOverlay::Transfer(picker)) = self.overlay.as_mut() {
                picker.loading = false;
                picker.error = Some("Session destinations are unavailable on this server.".into());
            }
        }
        outcome.repaint = true;
    }

    pub(super) fn transfer_pending_is_current(&self, kind: &PendingEndpointKind) -> bool {
        let (picker_id, endpoint_id) = match kind {
            PendingEndpointKind::TransferSessions {
                picker_id,
                endpoint_id,
            }
            | PendingEndpointKind::Transfer {
                picker_id,
                endpoint_id,
            } => (picker_id, endpoint_id),
            _ => return true,
        };
        matches!(self.overlay.as_ref(), Some(ClientShellOverlay::Transfer(picker))
            if picker.picker_id == *picker_id && &picker.endpoint_id == endpoint_id
                && &self.active_endpoint_id == endpoint_id
                && self.snapshot.as_deref().is_some_and(|snapshot| snapshot.boot_id == picker.boot_id))
    }

    pub(super) fn complete_transfer(
        &mut self,
        kind: PendingEndpointKind,
        result: Result<ResponseResult, ClientShellEndpointError>,
    ) -> bool {
        let Some(ClientShellOverlay::Transfer(picker)) = self.overlay.as_mut() else {
            return false;
        };
        match (kind, result) {
            (
                PendingEndpointKind::TransferSessions { .. },
                Ok(ResponseResult::SessionList { sessions }),
            ) => {
                picker.loading = false;
                picker.entries = sessions
                    .into_iter()
                    .filter(|session| !session.current && !session.name.is_empty())
                    .map(|session| ClientTransferDestination {
                        id: session.name.clone(),
                        label: session.name,
                        detail: if session.running {
                            "running local session on this server".into()
                        } else {
                            "stopped local session · starts when moved".into()
                        },
                    })
                    .collect();
                picker.entries.sort_by(|a, b| a.label.cmp(&b.label));
                picker.entries.dedup_by(|a, b| a.id == b.id);
                Self::reset_transfer_selection(picker);
                picker.error = None;
            }
            (PendingEndpointKind::Transfer { .. }, Ok(result)) => {
                let expected = matches!(
                    (&picker.source, &result),
                    (
                        ClientTransferSource::Tab { .. },
                        ResponseResult::TabInfo { .. }
                    ) | (
                        ClientTransferSource::Workspace { .. },
                        ResponseResult::WorkspaceTransferred { .. }
                    )
                );
                if expected {
                    self.overlay = None;
                } else {
                    picker.submitting = false;
                    picker.error = Some("Server returned an unexpected transfer result.".into());
                }
            }
            (_, Err(error)) => {
                picker.loading = false;
                picker.submitting = false;
                picker.error = Some(error.message);
            }
            _ => {
                picker.loading = false;
                picker.submitting = false;
                picker.error = Some("Server returned an unexpected session list.".into());
            }
        }
        true
    }

    pub(super) fn submit_transfer(&mut self, outcome: &mut ClientShellInput) {
        let Some(ClientShellOverlay::Transfer(picker)) = self.overlay.as_ref() else {
            return;
        };
        if picker.loading || picker.submitting {
            return;
        }
        let valid = self.active_endpoint_id == picker.endpoint_id
            && self.snapshot.as_deref().is_some_and(|snapshot| {
                snapshot.boot_id == picker.boot_id
                    && match &picker.source {
                        ClientTransferSource::Tab {
                            tab_id,
                            workspace_id,
                        } => snapshot
                            .tabs
                            .iter()
                            .any(|tab| &tab.tab_id == tab_id && &tab.workspace_id == workspace_id),
                        ClientTransferSource::Workspace { workspace_id } => snapshot
                            .workspaces
                            .iter()
                            .any(|workspace| &workspace.workspace_id == workspace_id),
                    }
            });
        let index = picker
            .filtered_indices()
            .into_iter()
            .find(|index| *index == picker.selected);
        let Some(entry) = index.and_then(|index| picker.entries.get(index)) else {
            return;
        };
        let destination_valid = match &picker.source {
            ClientTransferSource::Tab { workspace_id, .. } => {
                entry.id != *workspace_id
                    && self.snapshot.as_deref().is_some_and(|snapshot| {
                        snapshot
                            .workspaces
                            .iter()
                            .any(|workspace| workspace.workspace_id == entry.id)
                    })
            }
            ClientTransferSource::Workspace { .. } => true,
        };
        if !valid || !destination_valid {
            if let Some(ClientShellOverlay::Transfer(picker)) = self.overlay.as_mut() {
                picker.error = Some(
                    "The source or destination changed. Close this picker and try again.".into(),
                );
            }
            outcome.repaint = true;
            return;
        }
        let method = picker.method(entry.id.clone());
        let kind = PendingEndpointKind::Transfer {
            picker_id: picker.picker_id,
            endpoint_id: picker.endpoint_id.clone(),
        };
        let submitted = self.push_endpoint_method_with_kind(method, kind, outcome);
        if let Some(ClientShellOverlay::Transfer(picker)) = self.overlay.as_mut() {
            picker.submitting = submitted;
            picker.error = (!submitted)
                .then(|| "Move unavailable. Check the server notice and try again.".into());
        }
        outcome.repaint = true;
    }

    fn reset_transfer_selection(picker: &mut ClientTransferOverlay) {
        picker.selected = picker.filtered_indices().first().copied().unwrap_or(0);
    }

    pub(super) fn insert_transfer_text(&mut self, text: &str) -> bool {
        let Some(ClientShellOverlay::Transfer(picker)) = self.overlay.as_mut() else {
            return false;
        };
        if picker.search_focused && !picker.submitting && picker.query.insert(text) {
            Self::reset_transfer_selection(picker);
        }
        true
    }

    fn move_transfer_selection(&mut self, delta: isize) {
        let Some(ClientShellOverlay::Transfer(picker)) = self.overlay.as_mut() else {
            return;
        };
        if picker.loading || picker.submitting {
            return;
        }
        let filtered = picker.filtered_indices();
        let position = filtered
            .iter()
            .position(|index| *index == picker.selected)
            .unwrap_or(0);
        let next = (position as isize + delta).clamp(0, filtered.len().saturating_sub(1) as isize)
            as usize;
        if let Some(index) = filtered.get(next) {
            picker.selected = *index;
        }
    }

    pub(super) fn route_transfer_key(
        &mut self,
        key: &crate::input::TerminalKey,
        outcome: &mut ClientShellInput,
    ) -> bool {
        let Some(ClientShellOverlay::Transfer(picker)) = self.overlay.as_mut() else {
            return false;
        };
        if picker.submitting {
            return true;
        }
        if picker.search_focused {
            if let Some(changed) = picker.query.handle_key(key) {
                if changed {
                    Self::reset_transfer_selection(picker);
                }
                outcome.repaint = true;
                return true;
            }
        }
        let (code, modifiers) = crate::config::normalize_key_combo((key.code, key.modifiers));
        match code {
            KeyCode::Esc => self.overlay = None,
            KeyCode::Enter => self.submit_transfer(outcome),
            KeyCode::Up => self.move_transfer_selection(-1),
            KeyCode::Down => self.move_transfer_selection(1),
            KeyCode::Char('n' | 'p') if modifiers == crossterm::event::KeyModifiers::CONTROL => {
                self.move_transfer_selection(if code == KeyCode::Char('n') { 1 } else { -1 })
            }
            KeyCode::Char('/') => picker.search_focused = true,
            _ => {}
        }
        outcome.repaint = true;
        true
    }

    pub(super) fn route_transfer_mouse(
        &mut self,
        mouse: crossterm::event::MouseEvent,
        outcome: &mut ClientShellInput,
    ) -> bool {
        use crossterm::event::{MouseButton, MouseEventKind};
        let Some(ClientShellOverlay::Transfer(picker)) = self.overlay.as_ref() else {
            return false;
        };
        if picker.submitting {
            return true;
        }
        let point = (mouse.column, mouse.row);
        match mouse.kind {
            MouseEventKind::ScrollUp => self.move_transfer_selection(-1),
            MouseEventKind::ScrollDown => self.move_transfer_selection(1),
            MouseEventKind::Down(MouseButton::Left) => {
                if contains(self.hits.overlay_cancel, point) {
                    self.overlay = None;
                } else if contains(self.hits.worktree_search, point) {
                    if let Some(ClientShellOverlay::Transfer(picker)) = self.overlay.as_mut() {
                        picker.search_focused = true;
                    }
                } else if let Some(index) = self
                    .hits
                    .worktree_rows
                    .iter()
                    .find(|(rect, _)| contains(*rect, point))
                    .map(|(_, index)| *index)
                {
                    if let Some(ClientShellOverlay::Transfer(picker)) = self.overlay.as_mut() {
                        picker.selected = index;
                        picker.search_focused = false;
                    }
                    self.submit_transfer(outcome);
                } else if contains(self.hits.overlay_primary, point) {
                    self.submit_transfer(outcome);
                }
            }
            _ => {}
        }
        outcome.repaint = true;
        true
    }
}
