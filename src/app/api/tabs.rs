use std::path::PathBuf;

use crate::api::schema::{
    EventData, EventEnvelope, EventKind, ResponseResult, TabCreateParams, TabListParams,
    TabMoveParams, TabRenameParams, TabTarget, TabTransferParams,
};
use crate::app::{App, Mode};

use super::responses::{encode_error, encode_success};

impl App {
    pub(super) fn handle_tab_list(&mut self, id: String, params: TabListParams) -> String {
        let tabs = if let Some(workspace_id) = params.workspace_id {
            let Some(ws_idx) = self.parse_workspace_id(&workspace_id) else {
                return workspace_not_found(id, &workspace_id);
            };
            let Some(_) = self.state.workspaces.get(ws_idx) else {
                return workspace_not_found(id, &workspace_id);
            };
            self.tab_list_info(ws_idx)
        } else {
            let mut tabs = Vec::new();
            for (ws_idx, ws) in self.state.workspaces.iter().enumerate() {
                for tab_idx in 0..ws.tabs.len() {
                    if let Some(tab) = self.tab_info(ws_idx, tab_idx) {
                        tabs.push(tab);
                    }
                }
            }
            tabs
        };

        encode_success(id, ResponseResult::TabList { tabs })
    }

    pub(super) fn handle_tab_get(&mut self, id: String, target: TabTarget) -> String {
        let Some((ws_idx, tab_idx)) = self.parse_tab_id(&target.tab_id) else {
            return tab_not_found(id, &target.tab_id);
        };
        let Some(tab) = self.tab_info(ws_idx, tab_idx) else {
            return tab_not_found(id, &target.tab_id);
        };

        encode_success(id, ResponseResult::TabInfo { tab })
    }

    pub(super) fn handle_tab_create(&mut self, id: String, params: TabCreateParams) -> String {
        let TabCreateParams {
            workspace_id,
            cwd,
            focus,
            label,
            env,
        } = params;
        let ws_idx = if let Some(workspace_id) = workspace_id {
            let Some(ws_idx) = self.parse_workspace_id(&workspace_id) else {
                return workspace_not_found(id, &workspace_id);
            };
            ws_idx
        } else if let Some(active) = self.state.active {
            active
        } else {
            return encode_error(id, "workspace_not_found", "no active workspace");
        };
        let cwd = cwd.map(PathBuf::from).unwrap_or_else(|| {
            self.resolve_new_terminal_cwd(self.focused_pane_cwd_in_workspace(ws_idx))
        });
        let (rows, cols) = self.state.new_pane_size(crate::ui::NewPanePlacement::Alone);
        let default_shell = self.state.default_shell.clone();
        let scrollback_limit_bytes = self.state.pane_scrollback_limit_bytes;
        let host_terminal_theme = self.state.host_terminal_theme;
        let host_terminal_appearance = self.state.host_terminal_appearance;
        let extra_env = match super::env::normalize_launch_env(env) {
            Ok(env) => env,
            Err((code, message)) => return encode_error(id, &code, message),
        };
        let result = self
            .state
            .workspaces
            .get_mut(ws_idx)
            .ok_or_else(|| std::io::Error::other("workspace disappeared"))
            .and_then(|ws| {
                ws.create_tab(
                    rows,
                    cols,
                    cwd,
                    scrollback_limit_bytes,
                    host_terminal_theme,
                    host_terminal_appearance,
                    crate::pane::PaneShellConfig::new(&default_shell, self.state.shell_mode),
                    extra_env,
                )
            });
        match result {
            Ok((tab_idx, terminal, runtime)) => {
                self.terminal_runtimes.insert(terminal.id.clone(), runtime);
                self.state.terminals.insert(terminal.id.clone(), terminal);
                self.state.remove_alias_shadowed_by_new_pane(
                    self.state.workspaces[ws_idx].tabs[tab_idx].root_pane,
                );
                if let Some(label) = label {
                    let workspace_id = self.state.workspaces[ws_idx].id.clone();
                    let tab_id = self.public_tab_id(ws_idx, tab_idx).unwrap_or_else(|| {
                        crate::workspace::public_tab_id_for_number(&workspace_id, tab_idx + 1)
                    });
                    if let Some(tab) = self
                        .state
                        .workspaces
                        .get_mut(ws_idx)
                        .and_then(|ws| ws.tabs.get_mut(tab_idx))
                    {
                        tab.set_custom_name(label);
                        crate::logging::tab_renamed(&workspace_id, &tab_id);
                    }
                }
                if focus {
                    self.state.switch_workspace_tab(ws_idx, tab_idx);
                    self.state.mode = Mode::Terminal;
                }
                self.schedule_session_save();
                self.emit_tab_created_events(ws_idx, tab_idx);
                encode_success(
                    id,
                    self.tab_created_result(ws_idx, tab_idx)
                        .expect("new tab should produce a complete create response"),
                )
            }
            Err(err) => encode_error(id, "tab_create_failed", err.to_string()),
        }
    }

    pub(super) fn handle_tab_focus(&mut self, id: String, target: TabTarget) -> String {
        let Some((ws_idx, tab_idx)) = self.parse_tab_id(&target.tab_id) else {
            return tab_not_found(id, &target.tab_id);
        };
        self.state.switch_workspace_tab(ws_idx, tab_idx);
        let tab = self.tab_info(ws_idx, tab_idx).unwrap();

        encode_success(id, ResponseResult::TabInfo { tab })
    }

    pub(super) fn handle_tab_rename(&mut self, id: String, params: TabRenameParams) -> String {
        let Some((ws_idx, tab_idx)) = self.parse_tab_id(&params.tab_id) else {
            return tab_not_found(id, &params.tab_id);
        };
        let workspace_id = self.state.workspaces[ws_idx].id.clone();
        let tab_id = self.public_tab_id(ws_idx, tab_idx).unwrap_or_else(|| {
            crate::workspace::public_tab_id_for_number(&workspace_id, tab_idx + 1)
        });
        let Some(tab) = self
            .state
            .workspaces
            .get_mut(ws_idx)
            .and_then(|ws| ws.tabs.get_mut(tab_idx))
        else {
            return tab_not_found(id, &params.tab_id);
        };
        tab.set_custom_name(params.label.clone());
        crate::logging::tab_renamed(&workspace_id, &tab_id);
        self.schedule_session_save();
        self.emit_event(EventEnvelope {
            event: EventKind::TabRenamed,
            data: EventData::TabRenamed {
                tab_id: self.public_tab_id(ws_idx, tab_idx).unwrap(),
                workspace_id: self.public_workspace_id(ws_idx),
                label: params.label,
            },
        });
        let tab = self.tab_info(ws_idx, tab_idx).unwrap();

        encode_success(id, ResponseResult::TabInfo { tab })
    }

    pub(super) fn handle_tab_move(&mut self, id: String, params: TabMoveParams) -> String {
        let Some((ws_idx, tab_idx)) = self.parse_tab_id(&params.tab_id) else {
            return tab_not_found(id, &params.tab_id);
        };
        let Some(ws) = self.state.workspaces.get(ws_idx) else {
            return tab_not_found(id, &params.tab_id);
        };
        if params.insert_index > ws.tabs.len() {
            return encode_error(
                id,
                "tab_move_failed",
                format!("insert_index {} is out of bounds", params.insert_index),
            );
        }

        let tab_id = self
            .public_tab_id(ws_idx, tab_idx)
            .unwrap_or_else(|| crate::workspace::public_tab_id_for_number(&ws.id, tab_idx + 1));
        let workspace_id = self.public_workspace_id(ws_idx);
        let insert_index = params.insert_index;
        let moved = self
            .state
            .workspaces
            .get_mut(ws_idx)
            .is_some_and(|ws| ws.move_tab(tab_idx, insert_index));
        let tabs = self.tab_list_info(ws_idx);
        if moved {
            self.schedule_session_save();
            self.emit_event(EventEnvelope {
                event: EventKind::TabMoved,
                data: EventData::TabMoved {
                    tab_id,
                    workspace_id,
                    insert_index,
                    tabs: tabs.clone(),
                },
            });
        }

        encode_success(id, ResponseResult::TabList { tabs })
    }

    pub(super) fn handle_tab_transfer(&mut self, id: String, params: TabTransferParams) -> String {
        let Some((source_ws_idx, source_tab_idx)) = self.parse_tab_id(&params.tab_id) else {
            return tab_not_found(id, &params.tab_id);
        };
        let Some(destination_ws_idx) = self.parse_workspace_id(&params.workspace_id) else {
            return workspace_not_found(id, &params.workspace_id);
        };
        let Some(source_tab) = self.tab_info(source_ws_idx, source_tab_idx) else {
            return tab_not_found(id, &params.tab_id);
        };
        if source_ws_idx == destination_ws_idx {
            return encode_success(id, ResponseResult::TabInfo { tab: source_tab });
        }
        let source_workspace = self.workspace_info(source_ws_idx);
        let Some((ws_idx, tab_idx, source_empty)) = self.state.transfer_tab(
            source_ws_idx,
            source_tab_idx,
            destination_ws_idx,
            params.focus,
        ) else {
            return encode_error(id, "tab_transfer_failed", "tab could not be transferred");
        };
        self.schedule_session_save();
        self.emit_event(EventEnvelope {
            event: EventKind::TabClosed,
            data: EventData::TabClosed {
                tab_id: source_tab.tab_id,
                workspace_id: source_tab.workspace_id.clone(),
            },
        });
        if source_empty {
            self.emit_event(EventEnvelope {
                event: EventKind::WorkspaceClosed,
                data: EventData::WorkspaceClosed {
                    workspace_id: source_tab.workspace_id,
                    workspace: Some(source_workspace),
                },
            });
        }
        let Some(tab) = self.tab_info(ws_idx, tab_idx) else {
            return encode_error(id, "tab_transfer_failed", "transferred tab is unavailable");
        };
        self.emit_event(EventEnvelope {
            event: EventKind::TabCreated,
            data: EventData::TabCreated { tab: tab.clone() },
        });
        encode_success(id, ResponseResult::TabInfo { tab })
    }

    pub(super) fn handle_tab_close(&mut self, id: String, target: TabTarget) -> String {
        let Some((ws_idx, tab_idx)) = self.parse_tab_id(&target.tab_id) else {
            return tab_not_found(id, &target.tab_id);
        };
        let Some(tab_id) = self.public_tab_id(ws_idx, tab_idx) else {
            return tab_not_found(id, &target.tab_id);
        };
        let workspace_id = self.public_workspace_id(ws_idx);
        let Some(ws) = self.state.workspaces.get(ws_idx) else {
            return tab_not_found(id, &target.tab_id);
        };
        let closes_workspace = ws.tabs.len() <= 1;
        let terminal_ids = self.state.terminal_ids_for_tab(ws_idx, tab_idx);
        let pane_ids = ws
            .tabs
            .get(tab_idx)
            .map(|tab| tab.layout.pane_ids())
            .unwrap_or_default();

        if closes_workspace {
            if self.state.confirm_implicit_worktree_group_close(ws_idx) {
                return encode_error(
                    id,
                    "confirmation_required",
                    "closing this tab would close a worktree group",
                );
            }
            let workspace = self.workspace_info(ws_idx);
            self.state.selected = ws_idx;
            self.state.close_selected_workspace();
            self.state.remove_plugin_pane_records(pane_ids);
            self.shutdown_detached_terminal_runtimes();
            self.emit_event(EventEnvelope {
                event: EventKind::TabClosed,
                data: EventData::TabClosed {
                    tab_id,
                    workspace_id: workspace_id.clone(),
                },
            });
            self.emit_event(EventEnvelope {
                event: EventKind::WorkspaceClosed,
                data: EventData::WorkspaceClosed {
                    workspace_id,
                    workspace: Some(workspace),
                },
            });
            return encode_success(id, ResponseResult::Ok {});
        }

        let Some(ws) = self.state.workspaces.get_mut(ws_idx) else {
            return tab_not_found(id, &target.tab_id);
        };
        if !ws.close_tab(tab_idx) {
            return encode_error(
                id,
                "tab_close_failed",
                format!("tab {} could not be closed", target.tab_id),
            );
        }
        self.state.remove_plugin_pane_records(pane_ids);
        self.state.remove_unattached_terminal_ids(terminal_ids);
        self.shutdown_detached_terminal_runtimes();
        self.schedule_session_save();
        self.emit_event(EventEnvelope {
            event: EventKind::TabClosed,
            data: EventData::TabClosed {
                tab_id,
                workspace_id,
            },
        });

        encode_success(id, ResponseResult::Ok {})
    }

    fn tab_list_info(&self, ws_idx: usize) -> Vec<crate::api::schema::TabInfo> {
        self.state
            .workspaces
            .get(ws_idx)
            .map(|ws| {
                (0..ws.tabs.len())
                    .filter_map(|idx| self.tab_info(ws_idx, idx))
                    .collect()
            })
            .unwrap_or_default()
    }
}

fn workspace_not_found(id: String, workspace_id: &str) -> String {
    encode_error(
        id,
        "workspace_not_found",
        format!("workspace {workspace_id} not found"),
    )
}

fn tab_not_found(id: String, tab_id: &str) -> String {
    encode_error(id, "tab_not_found", format!("tab {tab_id} not found"))
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{exiting_test_command, shutdown_test_runtimes};
    use super::*;
    use crate::{
        api::schema::SuccessResponse,
        config::{Config, ShellModeConfig},
        workspace::Workspace,
    };

    fn transfer_test_app() -> (App, crate::api::EventHub) {
        let event_hub = crate::api::EventHub::default();
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let app = App::new(
            &Config::default(),
            crate::app::AppPolicy::TEST,
            None,
            api_rx,
            event_hub.clone(),
        );
        (app, event_hub)
    }

    fn transfer_snapshot(app: &App) -> serde_json::Value {
        serde_json::to_value(crate::persist::capture(
            &app.state.workspaces,
            &app.state.terminals,
            &app.terminal_runtimes,
            app.state.active,
            app.state.selected,
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn tab_transfer_preserves_zoomed_tree_terminals_viewport_and_all_aliases() {
        use crate::app::state::{
            PaneFocusTarget, PendingAgentNotification, ToastKind, ToastNotification, ToastTarget,
        };
        let (mut app, event_hub) = transfer_test_app();
        app.state = crate::app::AppState::test_with_adversarial_identity_state();
        app.state
            .workspaces
            .push(Workspace::test_adversarial_identity_state());
        let source_idx = app.state.workspaces[0].tabs.len() - 1;
        app.state.workspaces[0].tabs[source_idx].custom_name = Some("keep this label".into());
        app.state.workspaces[0].tabs[source_idx].zoomed = true;
        let pane_ids = app.state.workspaces[0].tabs[source_idx].layout.pane_ids();
        assert!(pane_ids.len() > 1);
        let focused = app.state.workspaces[0].tabs[source_idx].layout.focused();
        let root = app.state.workspaces[0].tabs[source_idx].root_pane;
        let layout = transfer_snapshot(&app)["workspaces"][0]["tabs"][source_idx]["layout"].clone();
        let terminal_ids = app.state.terminal_ids_for_tab(0, source_idx);
        let old_ids: Vec<_> = pane_ids
            .iter()
            .map(|&pane_id| app.public_pane_id(0, pane_id).unwrap())
            .collect();
        for &pane_id in &pane_ids {
            let pane = app.state.workspaces[0].tabs[source_idx]
                .panes
                .get_mut(&pane_id)
                .unwrap();
            pane.seen = false;
            pane.right_click_passthrough = true;
        }
        app.state.ensure_test_terminals();
        let runtime = crate::terminal::TerminalRuntime::test_with_scrollback_bytes(
            20,
            2,
            crate::config::DEFAULT_SCROLLBACK_LIMIT_BYTES,
            b"one\r\ntwo\r\nthree\r\nfour\r\nfive\r\n",
        );
        runtime.set_scroll_offset_from_bottom(1);
        let scroll_offset = runtime.scroll_metrics().unwrap().offset_from_bottom;
        assert!(scroll_offset > 0);
        let history = runtime.snapshot_history();
        app.terminal_runtimes
            .insert(terminal_ids[0].clone(), runtime);
        let runtime_address = app.terminal_runtimes.get(&terminal_ids[0]).unwrap()
            as *const crate::terminal::TerminalRuntime;
        let source_id = app.state.workspaces[0].id.clone();
        let target_id = app.state.workspaces[1].id.clone();
        app.state.previous_pane_focus = Some(PaneFocusTarget {
            workspace_id: source_id.clone(),
            pane_id: focused,
        });
        app.state.toast = Some(ToastNotification {
            kind: ToastKind::Finished,
            title: "finished".into(),
            context: "source".into(),
            position: None,
            target: Some(ToastTarget {
                workspace_id: source_id.clone(),
                pane_id: focused,
            }),
        });
        app.state.pending_agent_notifications.insert(
            focused,
            PendingAgentNotification {
                pane_id: focused,
                workspace_id: source_id,
                agent_label: "agent".into(),
                known_agent: None,
                kind: ToastKind::Finished,
                state: crate::detect::AgentState::Idle,
                deadline: std::time::Instant::now(),
            },
        );
        app.state.pane_id_aliases.insert(u32::MAX, focused);
        app.state
            .public_pane_id_aliases
            .insert("previous-location".into(), focused);
        let previous_view = app.state.current_pane_focus_target();
        let destination_active = app.state.workspaces[1].active_tab;
        let destination_tab_number = app.state.workspaces[1].next_public_tab_number;
        let destination_pane_number = app.state.workspaces[1].next_public_pane_number;
        app.state.assert_invariants_for_test();

        let response = app.handle_tab_transfer(
            "req".into(),
            TabTransferParams {
                tab_id: app.public_tab_id(0, source_idx).unwrap(),
                workspace_id: target_id.clone(),
                focus: false,
            },
        );

        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        let ResponseResult::TabInfo { tab } = success.result else {
            panic!("expected tab info")
        };
        let moved_idx = app.state.workspaces[1].tabs.len() - 1;
        let moved = &app.state.workspaces[1].tabs[moved_idx];
        assert_eq!(moved.root_pane, root);
        assert_eq!(moved.layout.focused(), focused);
        assert_eq!(
            transfer_snapshot(&app)["workspaces"][1]["tabs"][moved_idx]["layout"],
            layout
        );
        assert!(moved.zoomed);
        assert_eq!(moved.custom_name.as_deref(), Some("keep this label"));
        assert_eq!(moved.number, destination_tab_number);
        assert_eq!(app.state.terminal_ids_for_tab(1, moved_idx), terminal_ids);
        let runtime = app.terminal_runtimes.get(&terminal_ids[0]).unwrap();
        assert_eq!(
            runtime as *const crate::terminal::TerminalRuntime,
            runtime_address
        );
        assert_eq!(
            runtime.scroll_metrics().unwrap().offset_from_bottom,
            scroll_offset
        );
        assert_eq!(runtime.snapshot_history(), history);
        assert!(app.state.terminal_runtime_shutdowns.is_empty());
        for (offset, (&pane_id, old_id)) in pane_ids.iter().zip(&old_ids).enumerate() {
            assert_eq!(
                app.state.workspaces[1].public_pane_number(pane_id),
                Some(destination_pane_number + offset)
            );
            assert_eq!(app.parse_pane_id(old_id), Some((1, pane_id)));
            assert!(!moved.panes[&pane_id].seen);
            assert!(moved.panes[&pane_id].right_click_passthrough);
        }
        assert_eq!(app.state.pane_id_aliases.get(&u32::MAX), Some(&focused));
        assert_eq!(app.parse_pane_id("previous-location"), Some((1, focused)));
        assert_eq!(app.state.current_pane_focus_target(), previous_view);
        assert_eq!(app.state.workspaces[1].active_tab, destination_active);
        assert_eq!(
            app.state.previous_pane_focus.as_ref().unwrap().workspace_id,
            target_id
        );
        assert_eq!(
            app.state
                .toast
                .as_ref()
                .unwrap()
                .target
                .as_ref()
                .unwrap()
                .workspace_id,
            target_id
        );
        assert_eq!(
            app.state.pending_agent_notifications[&focused].workspace_id,
            target_id
        );
        assert_eq!(tab.workspace_id, target_id);
        assert_eq!(
            event_hub
                .events_after(0)
                .iter()
                .map(|(_, event)| event.event)
                .collect::<Vec<_>>(),
            [EventKind::TabClosed, EventKind::TabCreated]
        );
        app.state.assert_invariants_for_test();
        let intermediate_ids: Vec<_> = pane_ids
            .iter()
            .map(|&pane_id| app.public_pane_id(1, pane_id).unwrap())
            .collect();
        let response = app.handle_tab_transfer(
            "return".into(),
            TabTransferParams {
                tab_id: tab.tab_id,
                workspace_id: app.public_workspace_id(0),
                focus: true,
            },
        );
        let _: SuccessResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(app.state.active, Some(0));
        assert_eq!(app.state.selected, 0);
        assert_eq!(app.state.workspaces[0].focused_pane_id(), Some(focused));
        assert!(app.state.workspaces[0].active_tab().unwrap().zoomed);
        for ((old_id, intermediate_id), &pane_id) in
            old_ids.iter().zip(&intermediate_ids).zip(&pane_ids)
        {
            assert_eq!(app.parse_pane_id(old_id), Some((0, pane_id)));
            assert_eq!(app.parse_pane_id(intermediate_id), Some((0, pane_id)));
        }
        assert_eq!(app.state.pane_id_aliases.get(&u32::MAX), Some(&focused));
        app.state.assert_invariants_for_test();
        shutdown_test_runtimes(&mut app);
    }

    #[test]
    fn tab_transfer_last_tab_repairs_both_index_directions_without_closing_worktree_siblings() {
        for source_idx in [0, 2] {
            for focus in [false, true] {
                let (mut app, event_hub) = transfer_test_app();
                app.state.workspaces = vec![
                    Workspace::test_new("first"),
                    Workspace::test_new("unrelated"),
                    Workspace::test_new("last"),
                ];
                let destination_idx = 2 - source_idx;
                let membership = crate::workspace::WorktreeSpaceMembership {
                    key: "shared-repo".into(),
                    label: "repo".into(),
                    repo_root: "/repo".into(),
                    checkout_path: "/repo/checkout".into(),
                    is_linked_worktree: false,
                };
                app.state.workspaces[source_idx].worktree_space = Some(membership.clone());
                app.state.workspaces[1].worktree_space = Some(membership.clone());
                app.state.active = Some(1);
                app.state.selected = destination_idx;
                app.state.mode = Mode::Navigate;
                app.state.ensure_test_terminals();
                let unrelated_id = app.state.workspaces[1].id.clone();
                let destination_id = app.state.workspaces[destination_idx].id.clone();
                let moved_root = app.state.workspaces[source_idx].tabs[0].root_pane;
                app.state.previous_pane_focus = Some(crate::app::state::PaneFocusTarget {
                    workspace_id: app.state.workspaces[source_idx].id.clone(),
                    pane_id: moved_root,
                });
                let old_pane_id = app.public_pane_id(source_idx, moved_root).unwrap();
                let tab_id = app.public_tab_id(source_idx, 0).unwrap();
                app.state.assert_invariants_for_test();

                let response = app.handle_tab_transfer(
                    "req".into(),
                    TabTransferParams {
                        tab_id,
                        workspace_id: destination_id.clone(),
                        focus,
                    },
                );

                let _: SuccessResponse = serde_json::from_str(&response).unwrap();
                assert_eq!(app.state.workspaces.len(), 2);
                let target_idx = app.parse_workspace_id(&destination_id).unwrap();
                let sibling_idx = app.parse_workspace_id(&unrelated_id).unwrap();
                assert_eq!(
                    app.state.workspaces[sibling_idx].worktree_space,
                    Some(membership)
                );
                assert_eq!(
                    app.state.workspaces[target_idx].tabs[1].root_pane,
                    moved_root
                );
                assert_eq!(
                    app.parse_pane_id(&old_pane_id),
                    Some((target_idx, moved_root))
                );
                assert_eq!(app.state.selected, target_idx);
                if focus {
                    assert_eq!(app.state.active, Some(target_idx));
                    assert_eq!(app.state.workspaces[target_idx].active_tab, 1);
                    assert_eq!(app.state.mode, Mode::Terminal);
                } else {
                    assert_eq!(app.state.active, Some(sibling_idx));
                    assert_eq!(app.state.workspaces[target_idx].active_tab, 0);
                    assert_eq!(app.state.mode, Mode::Navigate);
                }
                assert_eq!(
                    event_hub
                        .events_after(0)
                        .iter()
                        .map(|(_, event)| event.event)
                        .collect::<Vec<_>>(),
                    [
                        EventKind::TabClosed,
                        EventKind::WorkspaceClosed,
                        EventKind::TabCreated
                    ]
                );
                app.state.assert_invariants_for_test();
            }
        }
    }

    #[test]
    fn tab_transfer_noop_and_invalid_targets_do_not_mutate() {
        let (mut app, event_hub) = transfer_test_app();
        app.state = crate::app::AppState::test_with_adversarial_identity_state();
        let tab_id = app.public_tab_id(0, 0).unwrap();
        let workspace_id = app.public_workspace_id(0);
        let before = transfer_snapshot(&app);
        let dirty = app.state.session_dirty;
        for (params, error_code) in [
            (
                TabTransferParams {
                    tab_id: tab_id.clone(),
                    workspace_id: workspace_id.clone(),
                    focus: true,
                },
                None,
            ),
            (
                TabTransferParams {
                    tab_id: "missing".into(),
                    workspace_id: workspace_id.clone(),
                    focus: false,
                },
                Some("tab_not_found"),
            ),
            (
                TabTransferParams {
                    tab_id,
                    workspace_id: "missing".into(),
                    focus: true,
                },
                Some("workspace_not_found"),
            ),
        ] {
            let response = app.handle_tab_transfer("req".into(), params);
            let response: serde_json::Value = serde_json::from_str(&response).unwrap();
            assert_eq!(response["error"]["code"].as_str(), error_code);
            if error_code.is_none() {
                assert!(response.get("result").is_some());
            }
            assert_eq!(transfer_snapshot(&app), before);
            assert_eq!(app.state.session_dirty, dirty);
            app.state.assert_invariants_for_test();
        }
        assert!(event_hub.events_after(0).is_empty());
    }

    #[test]
    fn tab_transfer_counter_exhaustion_is_validated_before_mutation() {
        let (mut app, event_hub) = transfer_test_app();
        app.state = crate::app::AppState::test_with_adversarial_identity_state();
        app.state
            .workspaces
            .push(Workspace::test_new("destination"));
        app.state.ensure_test_terminals();
        app.state.workspaces[1].next_public_pane_number = usize::MAX;
        let before = transfer_snapshot(&app);
        let response = app.handle_tab_transfer(
            "req".into(),
            TabTransferParams {
                tab_id: app.public_tab_id(0, 0).unwrap(),
                workspace_id: app.public_workspace_id(1),
                focus: true,
            },
        );
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(response["error"]["code"], "tab_transfer_failed");
        assert_eq!(transfer_snapshot(&app), before);
        assert!(event_hub.events_after(0).is_empty());
        app.state.assert_invariants_for_test();
    }

    #[test]
    fn tab_transfer_removing_active_selected_source_keeps_destination_existing_view() {
        for source_idx in [0, 1] {
            let (mut app, _) = transfer_test_app();
            app.state.workspaces = vec![Workspace::test_new("first"), Workspace::test_new("last")];
            app.state.active = Some(source_idx);
            app.state.selected = source_idx;
            app.state.mode = Mode::Navigate;
            app.state.ensure_test_terminals();
            let destination_idx = 1 - source_idx;
            let destination_root = app.state.workspaces[destination_idx].tabs[0].root_pane;
            let moved_root = app.state.workspaces[source_idx].tabs[0].root_pane;
            let response = app.handle_tab_transfer(
                "req".into(),
                TabTransferParams {
                    tab_id: app.public_tab_id(source_idx, 0).unwrap(),
                    workspace_id: app.public_workspace_id(destination_idx),
                    focus: false,
                },
            );
            let _: SuccessResponse = serde_json::from_str(&response).unwrap();
            assert_eq!(app.state.active, Some(0));
            assert_eq!(app.state.selected, 0);
            assert_eq!(
                app.state.workspaces[0].focused_pane_id(),
                Some(destination_root)
            );
            assert_eq!(app.state.workspaces[0].tabs[1].root_pane, moved_root);
            assert_eq!(app.state.mode, Mode::Navigate);
            app.state.assert_invariants_for_test();
        }
    }

    #[test]
    fn api_tab_close_last_tab_closes_workspace_and_emits_both_events() {
        let event_hub = crate::api::EventHub::default();
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &Config::default(),
            crate::app::AppPolicy::TEST,
            None,
            api_rx,
            event_hub.clone(),
        );
        app.state.workspaces = vec![Workspace::test_new("tabs")];
        app.state.active = Some(0);
        app.state.selected = 0;
        let tab_id = app.public_tab_id(0, 0).unwrap();
        let workspace_id = app.public_workspace_id(0);

        let response = app.handle_tab_close(
            "req".into(),
            TabTarget {
                tab_id: tab_id.clone(),
            },
        );

        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(success.result, ResponseResult::Ok {});
        assert!(app.state.workspaces.is_empty());
        assert!(app.state.active.is_none());
        let events = event_hub.events_after(0);
        assert_eq!(
            events
                .iter()
                .map(|(_, event)| event.event)
                .collect::<Vec<_>>(),
            [EventKind::TabClosed, EventKind::WorkspaceClosed]
        );
        assert!(matches!(
            &events[0].1.data,
            EventData::TabClosed {
                tab_id: closed_tab_id,
                workspace_id: closed_workspace_id,
            } if closed_tab_id == &tab_id && closed_workspace_id == &workspace_id
        ));
        assert!(matches!(
            &events[1].1.data,
            EventData::WorkspaceClosed {
                workspace_id: closed_workspace_id,
                workspace: Some(workspace),
            } if closed_workspace_id == &workspace_id
                && workspace.workspace_id == workspace_id
        ));
    }

    #[test]
    fn api_tab_move_reorders_tabs_in_target_workspace() {
        let event_hub = crate::api::EventHub::default();
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &Config::default(),
            crate::app::AppPolicy::TEST,
            None,
            api_rx,
            event_hub.clone(),
        );
        let mut workspace = Workspace::test_new("tabs");
        workspace.test_add_tab(Some("two"));
        workspace.test_add_tab(Some("three"));
        app.state.workspaces = vec![workspace];
        app.state.active = Some(0);
        app.state.selected = 0;
        let moved_root = app.state.workspaces[0].tabs[0].root_pane;
        let moved_id = app.public_tab_id(0, 0).unwrap();

        let response = app.handle_tab_move(
            "req".into(),
            TabMoveParams {
                tab_id: moved_id.clone(),
                insert_index: 3,
            },
        );

        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        let ResponseResult::TabList { tabs } = success.result else {
            panic!("expected tab list");
        };
        assert_eq!(app.state.workspaces[0].tabs[2].root_pane, moved_root);
        assert_eq!(tabs[2].tab_id, app.public_tab_id(0, 2).unwrap());
        let events = event_hub.events_after(0);
        assert!(events.iter().any(|(_, event)| {
            matches!(
                &event.data,
                EventData::TabMoved {
                    tab_id,
                    workspace_id,
                    insert_index: 3,
                    tabs,
                } if tab_id == &moved_id
                    && workspace_id == &app.public_workspace_id(0)
                    && tabs[2].tab_id == moved_id
            )
        }));
    }

    #[tokio::test]
    async fn tab_create_follows_cached_focused_pane_cwd_without_runtime() {
        let event_hub = crate::api::EventHub::default();
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &Config::default(),
            crate::app::AppPolicy::TEST,
            None,
            api_rx,
            event_hub,
        );
        app.state.default_shell = exiting_test_command().into();
        app.state.shell_mode = ShellModeConfig::NonLogin;
        let workspace = Workspace::test_new("tabs");
        let focused_pane = workspace.tabs[0].root_pane;
        app.state.workspaces = vec![workspace];
        app.state.active = Some(0);
        app.state.selected = 0;
        app.state.ensure_test_terminals();
        let cached_cwd = std::env::temp_dir();
        let terminal_id = app.state.workspaces[0]
            .terminal_id(focused_pane)
            .cloned()
            .unwrap();
        app.state.terminals.get_mut(&terminal_id).unwrap().cwd = cached_cwd.clone();

        let response = app.handle_tab_create(
            "req".into(),
            TabCreateParams {
                workspace_id: None,
                cwd: None,
                focus: false,
                label: None,
                env: Default::default(),
            },
        );

        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        assert!(matches!(success.result, ResponseResult::TabCreated { .. }));
        let created = &app.state.workspaces[0].tabs[1];
        let created_terminal_id = created.terminal_id(created.root_pane).unwrap();
        let created_cwd = &app.state.terminals.get(created_terminal_id).unwrap().cwd;
        assert_eq!(
            crate::worktree::canonical_or_original(created_cwd),
            crate::worktree::canonical_or_original(&cached_cwd)
        );
        shutdown_test_runtimes(&mut app);
    }
}
