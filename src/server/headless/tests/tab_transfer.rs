use super::*;
use crate::api::schema::{Method, Request, ResponseResult, SuccessResponse, TabTransferParams};

fn transfer_server() -> HeadlessServer {
    let mut server = test_headless_server();
    let mut source = crate::workspace::Workspace::test_new("source");
    source.test_add_tab(Some("remaining"));
    source.switch_tab(0);
    server.app.state.workspaces = vec![source, crate::workspace::Workspace::test_new("target")];
    server.app.state.ensure_test_terminals();
    server.app.state.active = Some(0);
    server.app.state.selected = 0;
    server.app.state.mode = crate::app::Mode::Terminal;
    server
}

fn transfer_request(server: &HeadlessServer, focus: bool) -> Request {
    Request {
        id: "transfer-tab".into(),
        method: Method::TabTransfer(TabTransferParams {
            tab_id: server.app.public_tab_id(0, 0).unwrap(),
            workspace_id: server.app.public_workspace_id(1),
            focus,
        }),
    }
}

#[tokio::test]
async fn public_tab_transfer_focus_follows_terminal_even_when_target_indices_do_not_change() {
    let mut server = transfer_server();
    let pane_id = server.app.state.workspaces[0].tabs[0].root_pane;
    let terminal_id = server.app.state.workspaces[0].tabs[0]
        .terminal_id(pane_id)
        .unwrap()
        .clone();
    let (runtime, mut input_rx) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
    server.app.terminal_runtimes.insert(terminal_id, runtime);
    // Source removal shifts the target into a different workspace's old numeric
    // coordinates. Successful focus must still follow identity, not the indices.
    server.app.state.workspaces[0].tabs.truncate(1);
    let mut spectator = crate::workspace::Workspace::test_new("spectator");
    spectator.test_add_tab(Some("other view"));
    server.app.state.workspaces.insert(1, spectator);
    server.app.state.ensure_test_terminals();
    server.app.state.switch_workspace_tab(1, 1);
    let source_tab = server.app.public_tab_id(0, 0).unwrap();
    let request = Request {
        id: "transfer-tab".into(),
        method: Method::TabTransfer(TabTransferParams {
            tab_id: source_tab.clone(),
            workspace_id: server.app.public_workspace_id(2),
            focus: true,
        }),
    };
    let (_control, _render) = connect_test_shell(&mut server, 9, 80, 23);
    assert!(server.focus_shell_client_on_tab(9, &source_tab));
    let target_before = server.default_shell_target();
    let (respond_to, response_rx) = std::sync::mpsc::channel();
    server.handle_api_request_with_shutdown_check(api::ApiRequestMessage {
        request,
        respond_to,
        response_write_complete: None,
    });
    let SuccessResponse {
        result: ResponseResult::TabInfo { tab },
        ..
    } = serde_json::from_str(&response_rx.recv().unwrap()).unwrap()
    else {
        panic!("tab transfer response")
    };
    assert_eq!(server.default_shell_target(), target_before);
    assert_eq!(
        server.shell_tab_id_for_client(9).as_deref(),
        Some(tab.tab_id.as_str())
    );
    let new_pane_id = server.app.public_pane_id(1, pane_id).unwrap();
    server.handle_server_event(ServerEvent::ClientShellPaneInput {
        client_id: 9,
        pane_id: new_pane_id,
        events: vec![crate::protocol::ClientPaneInputEvent::TextCommit(
            "x".into(),
        )],
    });
    assert_eq!(input_rx.try_recv().unwrap(), Bytes::from_static(b"x"));
    shutdown_test_runtimes(&mut server);
}

#[tokio::test]
async fn client_tab_transfer_only_focuses_requesting_client() {
    let mut server = transfer_server();
    let remaining = server.app.public_tab_id(0, 1).unwrap();
    let request = transfer_request(&server, true);
    let (_first_control, _first_render) = connect_test_shell(&mut server, 9, 80, 23);
    let (_second_control, _second_render) = connect_test_shell(&mut server, 10, 80, 23);
    assert!(server.focus_shell_client_on_tab(10, &remaining));
    let second_before = server.clients[&10].shell_location.clone();
    let (respond_to, response_rx) = std::sync::mpsc::channel();
    server.handle_client_shell_api_request(
        9,
        api::ApiRequestMessage {
            request,
            respond_to,
            response_write_complete: None,
        },
    );
    let SuccessResponse {
        result: ResponseResult::TabInfo { tab },
        ..
    } = serde_json::from_str(&response_rx.recv().unwrap()).unwrap()
    else {
        panic!("tab transfer response")
    };
    assert_eq!(
        server.shell_tab_id_for_client(9).as_deref(),
        Some(tab.tab_id.as_str())
    );
    assert_eq!(server.clients[&10].shell_location, second_before);
    shutdown_test_runtimes(&mut server);
}

#[tokio::test]
async fn public_tab_transfer_without_focus_preserves_other_client_views() {
    let mut server = transfer_server();
    let remaining = server.app.public_tab_id(0, 1).unwrap();
    let request = transfer_request(&server, false);
    let (_control, _render) = connect_test_shell(&mut server, 9, 80, 23);
    assert!(server.focus_shell_client_on_tab(9, &remaining));
    let before = server.clients[&9].shell_location.clone();
    let (respond_to, response_rx) = std::sync::mpsc::channel();
    server.handle_api_request_with_shutdown_check(api::ApiRequestMessage {
        request,
        respond_to,
        response_write_complete: None,
    });
    let response: SuccessResponse = serde_json::from_str(&response_rx.recv().unwrap()).unwrap();
    assert!(matches!(response.result, ResponseResult::TabInfo { .. }));
    assert_eq!(server.clients[&9].shell_location, before);
    shutdown_test_runtimes(&mut server);
}

#[tokio::test]
async fn public_tab_transfer_noop_does_not_redirect_other_clients() {
    let mut server = transfer_server();
    let (_control, _render) = connect_test_shell(&mut server, 9, 80, 23);
    let other_tab = server.app.public_tab_id(1, 0).unwrap();
    assert!(server.focus_shell_client_on_tab(9, &other_tab));
    let before = server.clients[&9].shell_location.clone();
    let (respond_to, response_rx) = std::sync::mpsc::channel();
    server.handle_api_request_with_shutdown_check(api::ApiRequestMessage {
        request: Request {
            id: "noop-transfer".into(),
            method: Method::TabTransfer(TabTransferParams {
                tab_id: server.app.public_tab_id(0, 0).unwrap(),
                workspace_id: server.app.public_workspace_id(0),
                focus: true,
            }),
        },
        respond_to,
        response_write_complete: None,
    });
    let response: SuccessResponse = serde_json::from_str(&response_rx.recv().unwrap()).unwrap();
    assert!(matches!(response.result, ResponseResult::TabInfo { .. }));
    assert_eq!(server.clients[&9].shell_location, before);
    shutdown_test_runtimes(&mut server);
}

#[tokio::test]
async fn tab_transfer_preserves_owned_popup_and_input_on_both_request_paths() {
    for client_local in [false, true] {
        let mut server = transfer_server();
        let source_tab = server.app.public_tab_id(0, 0).unwrap();
        let request = transfer_request(&server, true);
        let (_control, _render) = connect_test_shell(&mut server, 9, 80, 23);
        let (runtime, mut input) = crate::terminal::TerminalRuntime::test_with_channel(40, 12);
        let (_, terminal) = server.app.install_test_popup_runtime(runtime);
        server.popup_owner_tab_id = Some(source_tab);
        let (respond_to, response_rx) = std::sync::mpsc::channel();
        let message = api::ApiRequestMessage {
            request,
            respond_to,
            response_write_complete: None,
        };
        if client_local {
            server.handle_client_shell_api_request(9, message);
        } else {
            server.handle_api_request_with_shutdown_check(message);
        }
        let response: SuccessResponse = serde_json::from_str(&response_rx.recv().unwrap()).unwrap();
        let ResponseResult::TabInfo { tab } = response.result else {
            panic!("tab transfer response");
        };
        assert_eq!(
            server.popup_owner_tab_id.as_deref(),
            Some(tab.tab_id.as_str())
        );
        assert!(server.app.state.popup_pane.is_some());
        assert!(server.app.terminal_runtimes.get(&terminal).is_some());
        server.handle_server_event(ServerEvent::ClientShellPopupInput {
            client_id: 9,
            terminal_id: terminal.to_string(),
            events: vec![crate::protocol::ClientPaneInputEvent::TextCommit(
                "popup input".into(),
            )],
        });
        assert_eq!(
            input.try_recv().unwrap(),
            Bytes::from_static(b"popup input")
        );
        shutdown_test_runtimes(&mut server);
    }
}

#[tokio::test]
async fn tab_transfer_retargets_existing_toast_without_delivering_it_again() {
    let mut server = transfer_server();
    let pane = server.app.state.workspaces[0].tabs[0].root_pane;
    let request = transfer_request(&server, true);
    let (control, _render) = connect_test_shell(&mut server, 9, 80, 23);
    server.foreground_client_id = Some(9);
    let _ = control.try_iter().collect::<Vec<_>>();
    server.app.state.toast_config.delivery = crate::config::ToastDelivery::System;
    server.app.state.toast = Some(crate::app::state::ToastNotification {
        kind: crate::app::ToastKind::Finished,
        title: "completed".into(),
        context: "existing notification".into(),
        position: None,
        target: Some(crate::app::state::ToastTarget {
            workspace_id: server.app.public_workspace_id(0),
            pane_id: pane,
        }),
    });
    let (respond_to, response_rx) = std::sync::mpsc::channel();
    server.handle_api_request_with_shutdown_check(api::ApiRequestMessage {
        request,
        respond_to,
        response_write_complete: None,
    });
    let response: SuccessResponse = serde_json::from_str(&response_rx.recv().unwrap()).unwrap();
    assert!(matches!(response.result, ResponseResult::TabInfo { .. }));
    assert_eq!(
        server
            .app
            .state
            .toast
            .as_ref()
            .unwrap()
            .target
            .as_ref()
            .unwrap()
            .workspace_id,
        server.app.public_workspace_id(1)
    );
    for message in control.try_iter() {
        assert!(!matches!(
            read_server_message(message),
            ServerMessage::Notify { .. } | ServerMessage::SemanticNotification(_)
        ));
    }
    shutdown_test_runtimes(&mut server);
}

#[tokio::test]
async fn client_tab_transfer_background_failure_and_noop_preserve_view_identity_and_input() {
    for destination in ["background", "invalid", "same-space"] {
        let mut server = transfer_server();
        let first_tab = server.app.public_tab_id(0, 1).unwrap();
        let second_tab = server.app.public_tab_id(1, 0).unwrap();
        let mut receivers = Vec::new();
        for (workspace, tab) in [(0, 1), (1, 0)] {
            let pane = server.app.state.workspaces[workspace].tabs[tab].root_pane;
            let terminal = server.app.state.workspaces[workspace].tabs[tab]
                .terminal_id(pane)
                .unwrap()
                .clone();
            let (runtime, receiver) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
            runtime.test_process_pty_bytes(b"\x1b[?1004h");
            server.app.terminal_runtimes.insert(terminal, runtime);
            receivers.push(receiver);
        }
        let (_first_control, _first_render) = connect_test_shell(&mut server, 9, 80, 23);
        let (_second_control, _second_render) = connect_test_shell(&mut server, 10, 80, 23);
        assert!(server.focus_shell_client_on_tab(9, &first_tab));
        assert!(server.focus_shell_client_on_tab(10, &second_tab));
        for input in &mut receivers {
            while input.try_recv().is_ok() {}
        }
        let first_before = server.shell_focus_target(9);
        let second_before = server.shell_focus_target(10);
        let mut request = transfer_request(&server, destination != "background");
        let Method::TabTransfer(params) = &mut request.method else {
            unreachable!()
        };
        if destination == "invalid" {
            params.workspace_id = "w999999".into();
        } else if destination == "same-space" {
            params.workspace_id = server.app.public_workspace_id(0);
        }
        let (respond_to, response_rx) = std::sync::mpsc::channel();
        server.handle_client_shell_api_request(
            9,
            api::ApiRequestMessage {
                request,
                respond_to,
                response_write_complete: None,
            },
        );
        let response: serde_json::Value =
            serde_json::from_str(&response_rx.recv().unwrap()).unwrap();
        assert_eq!(response.get("error").is_some(), destination == "invalid");
        assert_eq!(server.shell_focus_target(9), first_before);
        assert_eq!(server.shell_focus_target(10), second_before);
        for (index, client) in [9, 10].into_iter().enumerate() {
            assert!(
                receivers[index].try_recv().is_err(),
                "unexpected focus input: {destination}"
            );
            let focus = server.shell_focus_target(client).unwrap();
            let pane = server
                .app
                .public_pane_id(focus.workspace_index, focus.pane_id)
                .unwrap();
            server.handle_server_event(ServerEvent::ClientShellPaneInput {
                client_id: client,
                pane_id: pane,
                events: vec![crate::protocol::ClientPaneInputEvent::TextCommit(
                    "unchanged view".into(),
                )],
            });
            assert_eq!(
                receivers[index].try_recv().unwrap(),
                Bytes::from_static(b"unchanged view")
            );
        }
        shutdown_test_runtimes(&mut server);
    }
}
