use super::*;
use crate::api::schema::{Method, ResponseResult, SessionDestinationInfo};

fn state() -> ClientShellState {
    let mut state = ClientShellState::new(ClientShellConfig::from_config(&Config::default()));
    let mut snapshot = snapshot();
    for (id, label) in [("ws_2", "beta"), ("ws_3", "gamma")] {
        let mut workspace = snapshot.workspaces[0].clone();
        workspace.workspace_id = id.into();
        workspace.label = label.into();
        workspace.focused = false;
        snapshot.workspaces.push(workspace);
    }
    state.set_snapshot(Box::new(snapshot));
    state.set_pane_surface(surface());
    state.set_endpoint_methods(Some(vec![
        "tab.focus".into(),
        "tab.transfer".into(),
        "workspace.transfer".into(),
        "session.list".into(),
    ]));
    state
}

fn picker(state: &ClientShellState) -> &ClientTransferOverlay {
    let Some(ClientShellOverlay::Transfer(picker)) = state.overlay.as_ref() else {
        panic!("expected transfer picker");
    };
    picker
}

fn request(actions: &[ClientShellAction]) -> &crate::api::schema::Request {
    let [ClientShellAction::Endpoint { request, .. }] = actions else {
        panic!("expected exactly one endpoint request");
    };
    request
}

fn open_menu(state: &mut ClientShellState, tab: bool) -> usize {
    if tab {
        state.open_tab_context_menu("tab_1".into(), 10, 3);
    } else {
        state.open_workspace_context_menu("ws_1".into(), 10, 3);
    }
    let Some(ClientShellOverlay::ContextMenu(menu)) = state.overlay.as_ref() else {
        panic!("expected context menu");
    };
    let action = if tab {
        ClientContextMenuAction::MoveToSpace
    } else {
        ClientContextMenuAction::MoveToSession
    };
    menu.items()
        .iter()
        .position(|item| item.action == action)
        .expect("move menu entry")
}

fn open_picker(state: &mut ClientShellState, tab: bool) -> ClientShellInput {
    let index = open_menu(state, tab);
    let mut outcome = ClientShellInput::default();
    state.activate_context_menu_item(index, &mut outcome);
    outcome
}

fn sessions() -> ResponseResult {
    ResponseResult::SessionList {
        sessions: vec![
            SessionDestinationInfo {
                name: "current".into(),
                current: true,
                running: true,
            },
            SessionDestinationInfo {
                name: "stopped".into(),
                current: false,
                running: false,
            },
            SessionDestinationInfo {
                name: "".into(),
                current: false,
                running: true,
            },
            SessionDestinationInfo {
                name: "gamma".into(),
                current: false,
                running: true,
            },
            SessionDestinationInfo {
                name: "beta".into(),
                current: false,
                running: true,
            },
        ],
    }
}

fn populate(state: &mut ClientShellState, preparation: &ClientShellInput) {
    assert!(matches!(
        request(&preparation.actions).method,
        Method::SessionList(_)
    ));
    let (repaint, actions) =
        state.handle_endpoint_result("boot-1", &request(&preparation.actions).id, Ok(sessions()));
    assert!(repaint);
    assert!(actions.is_empty());
}

fn click(state: &mut ClientShellState, rect: Rect) -> ClientShellInput {
    state.handle_raw_events(vec![RawInputEvent::Mouse(MouseEvent {
        kind: MouseEventKind::Down(MouseButton::Left),
        column: rect.x + 1,
        row: rect.y,
        modifiers: KeyModifiers::empty(),
    })])
}

#[test]
fn transfer_menu_keyboard_open_and_cancel_never_focuses() {
    for tab in [true, false] {
        let mut state = state();
        let index = open_menu(&mut state, tab);
        let mut navigation = Vec::new();
        for _ in 0..index {
            navigation.extend(state.handle_input_bytes(b"\x1b[B").actions);
        }
        assert!(navigation.is_empty());
        let open = state.handle_input_bytes(b"\r");
        if tab {
            assert!(open.actions.is_empty());
        } else {
            assert!(matches!(
                request(&open.actions).method,
                Method::SessionList(_)
            ));
        }
        assert_eq!(picker(&state).endpoint_id, state.active_endpoint_id);
        let cancel = state.handle_input_bytes(b"\x1b");
        assert!(cancel.actions.is_empty());
        assert!(state.overlay.is_none());
        assert_eq!(
            state.snapshot.as_ref().unwrap().focused_tab_id.as_deref(),
            Some("tab_1")
        );
    }
}

#[test]
fn transfer_menu_and_picker_mouse_submit_the_selected_destination() {
    for tab in [true, false] {
        let mut state = state();
        let index = open_menu(&mut state, tab);
        state.compose(106, 30).expect("context menu frame");
        let rect = state
            .hits
            .context_menu_rows
            .iter()
            .find(|(_, item)| *item == index)
            .unwrap()
            .0;
        let preparation = click(&mut state, rect);
        if tab {
            assert!(preparation.actions.is_empty());
        } else {
            populate(&mut state, &preparation);
        }
        state.compose(106, 30).expect("picker frame");
        let rect = state.hits.worktree_rows[1].0;
        let submission = click(&mut state, rect);
        match &request(&submission.actions).method {
            Method::TabTransfer(params) if tab => {
                assert_eq!(params.tab_id, "tab_1");
                assert_eq!(params.workspace_id, "ws_3");
                assert!(params.focus);
            }
            Method::WorkspaceTransfer(params) if !tab => {
                assert_eq!(params.workspace_id, "ws_1");
                assert_eq!(params.session, "gamma");
            }
            _ => panic!("wrong transfer method"),
        }
        assert!(picker(&state).submitting);
        assert!(state.handle_input_bytes(b"\r").actions.is_empty());
    }
}

#[test]
fn transfer_keyboard_filter_navigation_and_mouse_cancel() {
    for tab in [true, false] {
        let mut state = state();
        let preparation = open_picker(&mut state, tab);
        if !tab {
            populate(&mut state, &preparation);
        }
        assert_eq!(picker(&state).entries.len(), if tab { 2 } else { 3 });
        state.handle_input_bytes(b"\x1b[B");
        assert_eq!(picker(&state).selected, 1);
        state.handle_input_bytes(b"/beta");
        assert_eq!(picker(&state).filtered_indices(), vec![0]);
        assert_eq!(picker(&state).selected, 0);
        state.compose(106, 30).expect("picker frame");
        let rect = state.hits.overlay_cancel;
        assert!(click(&mut state, rect).actions.is_empty());
        assert!(state.overlay.is_none());
    }
}

#[test]
fn transfer_keyboard_submit_uses_only_transfer_rpc() {
    for tab in [true, false] {
        let mut state = state();
        let preparation = open_picker(&mut state, tab);
        if !tab {
            populate(&mut state, &preparation);
        }
        state.handle_input_bytes(b"\x1b[B");
        let submission = state.handle_input_bytes(b"\r");
        assert!(
            matches!(&request(&submission.actions).method,
                Method::TabTransfer(params) if tab && params.workspace_id == "ws_3"
            ) || matches!(&request(&submission.actions).method,
                Method::WorkspaceTransfer(params) if !tab && params.session == "gamma"
            )
        );
    }
}

#[test]
fn transfer_missing_capabilities_are_local_and_cancellable() {
    for (tab, methods) in [
        (true, vec![]),
        (false, vec![]),
        (false, vec!["workspace.transfer".into()]),
    ] {
        let mut state = state();
        state.set_endpoint_methods(Some(methods));
        let preparation = open_picker(&mut state, tab);
        assert!(preparation.actions.is_empty());
        assert!(!picker(&state).loading);
        assert!(picker(&state).error.is_some());
        assert!(state.visible_endpoint_notice.is_some());
        assert!(state.handle_input_bytes(b"\r").actions.is_empty());
        assert!(state.handle_input_bytes(b"\x1b").actions.is_empty());
        assert!(state.overlay.is_none());
    }
}

#[test]
fn transfer_session_listing_error_and_wrong_result_leave_picker_cancellable() {
    for result in [
        Err(ClientShellEndpointError {
            code: Some("endpoint_timeout".into()),
            message: "session list timed out".into(),
        }),
        Ok(worktree_list_result(None)),
    ] {
        let mut state = state();
        let preparation = open_picker(&mut state, false);
        state.handle_endpoint_result("boot-1", &request(&preparation.actions).id, result);
        assert!(!picker(&state).loading);
        assert!(picker(&state).error.is_some());
        assert!(state.handle_input_bytes(b"\x1b").actions.is_empty());
        assert!(state.overlay.is_none());
    }
}

#[test]
fn transfer_cancelled_session_response_cannot_replace_reopened_picker() {
    let mut state = state();
    let old = open_picker(&mut state, false);
    state.handle_input_bytes(b"\x1b");
    let new = open_picker(&mut state, false);
    let current_id = picker(&state).picker_id;
    let (repaint, actions) =
        state.handle_endpoint_result("boot-1", &request(&old.actions).id, Ok(sessions()));
    assert!(!repaint);
    assert!(actions.is_empty());
    assert_eq!(picker(&state).picker_id, current_id);
    assert!(picker(&state).loading);
    populate(&mut state, &new);
}

#[test]
fn transfer_revalidates_tab_source_destination_and_boot_before_submit() {
    for change in 0..3 {
        let mut state = state();
        open_picker(&mut state, true);
        let mut snapshot = state.snapshot.clone().unwrap();
        match change {
            0 => snapshot.tabs[0].workspace_id = "ws_3".into(),
            1 => snapshot
                .workspaces
                .retain(|workspace| workspace.workspace_id != "ws_2"),
            _ => snapshot.boot_id = "different-boot".into(),
        }
        // Change the projection without reopening the picker, modeling a concurrent edit/restart.
        state.snapshot = Some(snapshot);
        let submission = state.handle_input_bytes(b"\r");
        assert!(submission.actions.is_empty());
        assert!(picker(&state).error.is_some());
    }
}

#[test]
fn transfer_rejection_retains_destination_and_can_be_retried() {
    for tab in [true, false] {
        let mut state = state();
        let preparation = open_picker(&mut state, tab);
        if !tab {
            populate(&mut state, &preparation);
        }
        let submission = state.handle_input_bytes(b"\r");
        state.handle_endpoint_result(
            "boot-1",
            &request(&submission.actions).id,
            Err(ClientShellEndpointError {
                code: Some("transfer_failed".into()),
                message: "destination failed to start".into(),
            }),
        );
        assert_eq!(
            picker(&state).error.as_deref(),
            Some("destination failed to start")
        );
        assert!(!picker(&state).submitting);
        assert_eq!(picker(&state).selected, 0);
        assert!(state.visible_endpoint_notice.is_some());
        let retry = state.handle_input_bytes(b"\r");
        request(&retry.actions);
    }
}

struct BrokenTransferTransport;

impl crate::client::endpoint::EndpointTransport for BrokenTransferTransport {
    fn send(&mut self, _: &ClientMessage) -> std::io::Result<()> {
        Err(std::io::ErrorKind::BrokenPipe.into())
    }
}

#[test]
fn transfer_submission_interruption_is_cancellable_and_late_completion_is_ignored() {
    use crate::client::endpoint::{EndpointNegotiation, EndpointRegistry};
    use crate::client::endpoint_commands::EndpointCommands;

    for tab in [true, false] {
        for interruption in ["frozen", "failed-send", "disconnect"] {
            let mut state = state();
            let preparation = open_picker(&mut state, tab);
            if !tab {
                populate(&mut state, &preparation);
            }
            let submission = state.handle_input_bytes(b"\r");
            let old_request = request(&submission.actions).id.clone();
            assert!(picker(&state).submitting);
            if interruption == "disconnect" {
                state.mark_endpoint_disconnected(&ClientEndpointId::Local);
            } else {
                let mut endpoints = EndpointRegistry::new(
                    BrokenTransferTransport,
                    1,
                    EndpointNegotiation::default(),
                );
                endpoints
                    .set_surface_active(&ClientEndpointId::Local, interruption == "failed-send");
                let mut commands = EndpointCommands::default();
                let mut scheduled = None;
                let (replay, repaint) =
                    crate::client::shell_runtime::dispatch_client_shell_actions(
                        submission.actions,
                        &mut commands,
                        &mut endpoints,
                        Some(&mut state),
                        &mut Vec::new(),
                        &mut scheduled,
                    )
                    .unwrap();
                assert!(repaint);
                assert!(replay.is_empty());
                assert!(scheduled.is_none());
                assert!(commands.disconnect(&ClientEndpointId::Local).is_empty());
            }
            assert!(state.pending_requests.is_empty());
            assert!(!picker(&state).submitting);
            assert!(picker(&state)
                .error
                .as_deref()
                .unwrap()
                .contains("interrupted"));
            assert!(state.handle_input_bytes(b"\x1b").actions.is_empty());
            assert!(state.overlay.is_none());
            state.set_endpoint_status(&ClientEndpointId::Local, ClientEndpointStatus::Online);
            open_picker(&mut state, tab);
            let current = picker(&state).picker_id;
            let (repaint, actions) =
                state.handle_endpoint_result("boot-1", &old_request, Ok(ResponseResult::Ok {}));
            assert!(!repaint);
            assert!(actions.is_empty());
            assert_eq!(picker(&state).picker_id, current);
            assert!(picker(&state).error.is_none());
            assert!(!picker(&state).submitting);
        }
    }
}

#[test]
fn transfer_inactive_tab_cancel_does_not_focus_the_source() {
    let mut state = state();
    let mut snapshot = state.snapshot.clone().unwrap();
    let mut tab = snapshot.tabs[0].clone();
    tab.tab_id = "tab_inactive".into();
    tab.focused = false;
    snapshot.tabs.push(tab);
    state.set_snapshot(snapshot);
    state.open_tab_context_menu("tab_inactive".into(), 10, 3);
    let Some(ClientShellOverlay::ContextMenu(menu)) = state.overlay.as_ref() else {
        panic!("context menu");
    };
    let index = menu
        .items()
        .iter()
        .position(|item| item.action == ClientContextMenuAction::MoveToSpace)
        .unwrap();
    let mut outcome = ClientShellInput::default();
    state.activate_context_menu_item(index, &mut outcome);
    assert!(outcome.actions.is_empty());
    assert!(state.handle_input_bytes(b"\x1b").actions.is_empty());
    assert_eq!(
        state.snapshot.as_ref().unwrap().focused_tab_id.as_deref(),
        Some("tab_1")
    );
}

#[test]
fn transfer_session_list_runs_on_source_ssh_endpoint_and_rejects_endpoint_change() {
    let mut state = state();
    let profile = SavedSshEndpoint {
        id: crate::client::endpoint::ProfileId::parse("0123456789abcdef0123456789abcdef").unwrap(),
        label: "remote".into(),
        target: "dev@remote.example".into(),
        session: "work".into(),
        enabled: true,
    };
    let remote = ClientEndpointId::Ssh(profile.id.clone());
    state.set_endpoint_catalog(&[profile]);
    state.set_endpoint_status(&remote, ClientEndpointStatus::Online);
    let mut snapshot = snapshot();
    snapshot.boot_id = "remote-boot".into();
    state.set_endpoint_snapshot(&remote, Box::new(snapshot));
    state.set_endpoint_methods_for(
        &remote,
        Some(vec!["workspace.transfer".into(), "session.list".into()]),
    );
    assert!(state.activate_endpoint_projection(&remote));
    let preparation = open_picker(&mut state, false);
    let [ClientShellAction::Endpoint {
        endpoint_id,
        boot_id,
        request,
    }] = &preparation.actions[..]
    else {
        panic!("one remote session list request");
    };
    assert_eq!(endpoint_id, &remote);
    assert_eq!(boot_id, "remote-boot");
    state.handle_endpoint_result("remote-boot", &request.id, Ok(sessions()));
    assert_eq!(picker(&state).entries.len(), 3);
    assert_eq!(picker(&state).endpoint_id, remote);
    // Even a matching boot string cannot make a picker operate on another endpoint.
    state.active_endpoint_id = ClientEndpointId::Local;
    let submission = state.handle_input_bytes(b"\r");
    assert!(submission.actions.is_empty());
    assert!(picker(&state).error.is_some());
}

#[test]
fn transfer_response_from_previous_boot_or_endpoint_is_ignored() {
    for boot_change in [true, false] {
        let mut state = state();
        let preparation = open_picker(&mut state, false);
        if boot_change {
            state.snapshot.as_mut().unwrap().boot_id = "restarted".into();
        } else {
            let id = crate::client::endpoint::ProfileId::parse("0123456789abcdef0123456789abcdef")
                .unwrap();
            state.active_endpoint_id = ClientEndpointId::Ssh(id);
        }
        let (repaint, actions) = state.handle_endpoint_result(
            "boot-1",
            &request(&preparation.actions).id,
            Ok(sessions()),
        );
        assert!(!repaint);
        assert!(actions.is_empty());
        assert!(picker(&state).entries.is_empty());
    }
}

#[test]
fn transfer_success_closes_picker_without_a_followup_focus_request() {
    for tab in [true, false] {
        let mut state = state();
        let preparation = open_picker(&mut state, tab);
        if !tab {
            populate(&mut state, &preparation);
        }
        let submission = state.handle_input_bytes(b"\r");
        let result = if tab {
            ResponseResult::TabInfo {
                tab: crate::api::schema::TabInfo {
                    tab_id: "ws_2:t2".into(),
                    workspace_id: "ws_2".into(),
                    number: 2,
                    label: "moved".into(),
                    focused: true,
                    pane_count: 1,
                    agent_status: crate::api::schema::AgentStatus::Idle,
                },
            }
        } else {
            ResponseResult::WorkspaceTransferred {
                workspace_id: "destination-space".into(),
                session: "beta".into(),
            }
        };
        let (repaint, actions) =
            state.handle_endpoint_result("boot-1", &request(&submission.actions).id, Ok(result));
        assert!(repaint);
        assert!(actions.is_empty());
        assert!(state.overlay.is_none());
    }
}

#[test]
fn transfer_filters_typed_before_session_list_completion_select_a_matching_destination() {
    let mut state = state();
    let preparation = open_picker(&mut state, false);
    state.handle_input_bytes(b"/gamma");
    populate(&mut state, &preparation);
    assert_eq!(picker(&state).entries[picker(&state).selected].id, "gamma");
    let submission = state.handle_input_bytes(b"\r");
    assert!(matches!(&request(&submission.actions).method,
        Method::WorkspaceTransfer(params) if params.session == "gamma"));
}

#[test]
fn transfer_stopped_sessions_are_visible_and_can_be_selected() {
    let mut state = state();
    let preparation = open_picker(&mut state, false);
    populate(&mut state, &preparation);
    assert!(picker(&state)
        .entries
        .iter()
        .all(|entry| entry.id != "current" && !entry.id.is_empty()));
    let stopped = picker(&state)
        .entries
        .iter()
        .find(|entry| entry.id == "stopped")
        .unwrap();
    assert!(stopped.detail.contains("starts when moved"));
    state.handle_input_bytes(b"/stopped");
    let submission = state.handle_input_bytes(b"\r");
    assert!(matches!(&request(&submission.actions).method,
        Method::WorkspaceTransfer(params) if params.session == "stopped"));
    assert!(picker(&state).submitting);
}

#[test]
fn transfer_unexpected_result_keeps_picker_open_with_an_error() {
    for tab in [true, false] {
        let mut state = state();
        let preparation = open_picker(&mut state, tab);
        if !tab {
            populate(&mut state, &preparation);
        }
        let submission = state.handle_input_bytes(b"\r");
        state.handle_endpoint_result("boot-1", &request(&submission.actions).id, Ok(sessions()));
        assert!(!picker(&state).submitting);
        assert_eq!(
            picker(&state).error.as_deref(),
            Some("Server returned an unexpected transfer result.")
        );
    }
}

#[test]
fn transfer_empty_destinations_and_small_screen_are_cancellable() {
    for tab in [true, false] {
        let mut state = state();
        if tab {
            let mut snapshot = state.snapshot.clone().unwrap();
            snapshot.workspaces.truncate(1);
            state.set_snapshot(snapshot);
        }
        let preparation = open_picker(&mut state, tab);
        if !tab {
            state.handle_endpoint_result(
                "boot-1",
                &request(&preparation.actions).id,
                Ok(ResponseResult::SessionList { sessions: vec![] }),
            );
        }
        assert!(picker(&state).entries.is_empty());
        assert!(state.handle_input_bytes(b"\r").actions.is_empty());
        state.compose(40, 8).expect("small picker frame");
        assert!(state.handle_input_bytes(b"\x1b").actions.is_empty());
        assert!(state.overlay.is_none());
    }
}

#[test]
fn transfer_removed_workspace_source_is_rejected_locally() {
    let mut state = state();
    let preparation = open_picker(&mut state, false);
    populate(&mut state, &preparation);
    state
        .snapshot
        .as_mut()
        .unwrap()
        .workspaces
        .retain(|workspace| workspace.workspace_id != "ws_1");
    assert!(state.handle_input_bytes(b"\r").actions.is_empty());
    assert!(picker(&state).error.is_some());
}
