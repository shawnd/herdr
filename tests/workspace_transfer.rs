#![cfg(unix)]

pub mod support;

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::{symlink, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

struct Sandbox {
    base: PathBuf,
}

impl Sandbox {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let base = std::env::temp_dir().join(format!(
            "hwx-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&base).unwrap();
        support::register_runtime_dir(&base);
        Self { base }
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        support::cleanup_test_base(&self.base);
    }
}

struct Server {
    child: Child,
    socket: PathBuf,
    session_dir: PathBuf,
}

/// Owns only a daemon started within this test's throwaway session namespace.
struct StartedDestination {
    socket: PathBuf,
    pids: Vec<u32>,
}

impl Drop for StartedDestination {
    fn drop(&mut self) {
        let _ = try_rpc(&self.socket, "server.stop", json!({}));
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if UnixStream::connect(&self.socket).is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        for pid in &self.pids {
            support::unregister_spawned_herdr_pid(Some(*pid));
        }
    }
}

fn app_dir() -> &'static str {
    if cfg!(debug_assertions) {
        "herdr-dev"
    } else {
        "herdr"
    }
}

impl Server {
    fn start(base: &Path, config_name: &str, session: &str) -> Self {
        let home = base.join(config_name);
        let config = home.join(app_dir());
        let runtime = base.join(format!("rt-{config_name}"));
        fs::create_dir_all(&config).unwrap();
        fs::create_dir_all(&runtime).unwrap();
        support::register_runtime_dir(&runtime);
        fs::write(config.join("config.toml"), "onboarding = false\n").unwrap();
        let session_dir = config.join("sessions").join(session);
        let socket = session_dir.join("herdr.sock");
        let child = Command::new(env!("CARGO_BIN_EXE_herdr"))
            .args(["--session", session, "server"])
            .env("XDG_CONFIG_HOME", &home)
            .env("XDG_RUNTIME_DIR", &runtime)
            .env("XDG_STATE_HOME", base.join(format!("state-{config_name}")))
            .env("HERDR_CONFIG_PATH", config.join("config.toml"))
            .env("SHELL", "/bin/sh")
            .env_remove("HERDR_SESSION")
            .env_remove("HERDR_SOCKET_PATH")
            .env_remove("HERDR_CLIENT_SOCKET_PATH")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        support::register_spawned_herdr_pid(Some(child.id()));
        support::wait_for_socket(&socket, Duration::from_secs(10));
        Self {
            child,
            socket,
            session_dir,
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = try_rpc(&self.socket, "server.stop", json!({}));
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if self.child.try_wait().ok().flatten().is_some() {
                support::unregister_spawned_herdr_pid(Some(self.child.id()));
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        support::unregister_spawned_herdr_pid(Some(self.child.id()));
    }
}

fn try_rpc(socket: &Path, method: &str, params: Value) -> std::io::Result<Value> {
    let mut stream = UnixStream::connect(socket)?;
    stream.set_read_timeout(Some(Duration::from_secs(25)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    writeln!(
        stream,
        "{}",
        json!({"id":"workspace-transfer-test", "method":method,"params":params})
    )?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line)?;
    serde_json::from_str(&line).map_err(std::io::Error::other)
}

fn rpc(socket: &Path, method: &str, params: Value) -> Value {
    let value = try_rpc(socket, method, params).unwrap();
    assert!(value.get("error").is_none(), "{method}: {value}");
    value
}

fn create(server: &Server, cwd: &Path) -> (String, String) {
    let response = rpc(
        &server.socket,
        "workspace.create",
        json!({"cwd":cwd,"focus":true}),
    );
    (
        response["result"]["workspace"]["workspace_id"]
            .as_str()
            .unwrap()
            .into(),
        response["result"]["root_pane"]["pane_id"]
            .as_str()
            .unwrap()
            .into(),
    )
}

fn send(socket: &Path, pane: &str, text: &str) {
    rpc(
        socket,
        "pane.send_input",
        json!({"pane_id":pane,"text":text,"keys":["Enter"]}),
    );
}

fn wait_output(socket: &Path, pane: &str, needle: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let value = rpc(
            socket,
            "pane.read",
            json!({"pane_id":pane,"source":"visible","lines":100,"format":"text","strip_ansi":true}),
        );
        let text = value["result"]["read"]["text"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if text.contains(needle) {
            return text;
        }
        assert!(Instant::now() < deadline, "missing {needle:?} in {text:?}");
        thread::sleep(Duration::from_millis(30));
    }
}

fn shell_pid(socket: &Path, pane: &str) -> u64 {
    rpc(socket, "pane.process_info", json!({"pane_id":pane}))["result"]["process_info"]["shell_pid"]
        .as_u64()
        .unwrap()
}

#[test]
fn move_preserves_multi_pane_processes_io_numbering_and_inherited_routes() {
    let sandbox = Sandbox::new();
    let mut source = Server::start(&sandbox.base, "source-config", "source");
    let mut target = Server::start(&sandbox.base, "target-config", "target");
    // Two independent throwaway config homes. Publish only this throwaway
    // target's session directory into the source's local-session namespace.
    symlink(
        &target.session_dir,
        source.session_dir.parent().unwrap().join("target"),
    )
    .unwrap();
    let (workspace, pane) = create(&source, &sandbox.base);
    let (collision, target_pane) = create(&target, &sandbox.base);
    assert_eq!(
        workspace, collision,
        "fixture must exercise colliding public identities"
    );
    send(&target.socket, &target_pane, "printf 'TARGET_UNTOUCHED\\n'");
    let split = rpc(
        &source.socket,
        "pane.split",
        json!({"workspace_id":workspace,"target_pane_id":pane,"direction":"right","focus":false}),
    );
    let second = split["result"]["pane"]["pane_id"]
        .as_str()
        .unwrap()
        .to_string();
    let script = sandbox.base.join("io.py");
    fs::write(&script, "import os,sys,time,select\nprint('READY',os.getpid(),flush=True)\ni=0\nwhile True:\n if select.select([sys.stdin],[],[],.05)[0]:\n  line=sys.stdin.readline().strip()\n  print('ACK',os.getpid(),line,flush=True)\n i+=1\n if i%20==0: print('TICK',os.getpid(),i,flush=True)\n").unwrap();
    let mut before = Vec::new();
    for id in [&pane, &second] {
        send(
            &source.socket,
            id,
            &format!("python3 -u '{}'", script.display()),
        );
        let ready = wait_output(&source.socket, id, "READY ");
        let pid = ready
            .lines()
            .find_map(|line| line.strip_prefix("READY "))
            .unwrap()
            .trim()
            .to_string();
        before.push((id.to_string(), shell_pid(&source.socket, id), pid));
    }
    rpc(
        &source.socket,
        "workspace.report_metadata",
        json!({"workspace_id":workspace,"source":"test","tokens":{"summary":"moved"},"seq":7}),
    );
    rpc(
        &source.socket,
        "pane.report_metadata",
        json!({"pane_id":pane,"source":"test","title":"transfer title","tokens":{"model":"transfer-model"},"seq":7}),
    );
    let moved = rpc(
        &source.socket,
        "workspace.transfer",
        json!({"workspace_id":workspace,"session":"target"}),
    );
    let new_id = moved["result"]["workspace_id"].as_str().unwrap();
    assert_ne!(new_id, collision);
    assert!(
        source.child.try_wait().unwrap().is_none(),
        "original source server must remain alive"
    );
    assert!(
        target.child.try_wait().unwrap().is_none(),
        "original target server must remain alive"
    );
    rpc(&source.socket, "ping", json!({}));
    rpc(&target.socket, "ping", json!({}));
    let workspaces = rpc(&target.socket, "workspace.list", json!({}));
    assert_eq!(
        workspaces["result"]["workspaces"].as_array().unwrap().len(),
        2
    );
    let moved_info = rpc(
        &target.socket,
        "workspace.get",
        json!({"workspace_id":new_id}),
    );
    assert_eq!(
        moved_info["result"]["workspace"]["tokens"]["summary"],
        "moved"
    );
    let canonical_pane = pane.replacen(&workspace, new_id, 1);
    let pane_info = rpc(
        &target.socket,
        "pane.get",
        json!({"pane_id":canonical_pane}),
    );
    assert_eq!(
        pane_info["result"]["pane"]["tokens"]["model"],
        "transfer-model"
    );
    assert_eq!(pane_info["result"]["pane"]["title"], "transfer title");
    rpc(
        &source.socket,
        "workspace.report_metadata",
        json!({"workspace_id":workspace,"source":"test","tokens":{"summary":"forwarded"},"seq":8}),
    );
    assert_eq!(
        rpc(
            &target.socket,
            "workspace.get",
            json!({"workspace_id":new_id})
        )["result"]["workspace"]["tokens"]["summary"],
        "forwarded"
    );
    assert!(rpc(
        &target.socket,
        "workspace.get",
        json!({"workspace_id":collision})
    )["result"]["workspace"]["tokens"]
        .get("summary")
        .is_none());
    for (index, (old, shell, pid)) in before.iter().enumerate() {
        let canonical = old.replacen(&workspace, new_id, 1);
        assert_eq!(shell_pid(&target.socket, &canonical), *shell);
        // CLI/hooks still use their inherited source socket and old pane ID.
        send(&source.socket, old, &format!("after-move-{index}"));
        wait_output(
            &target.socket,
            &canonical,
            &format!("ACK {pid} after-move-{index}"),
        );
        wait_output(&target.socket, &canonical, &format!("TICK {pid}"));
    }
    wait_output(&target.socket, &target_pane, "TARGET_UNTOUCHED");
    let second_move = try_rpc(
        &target.socket,
        "workspace.transfer",
        json!({"workspace_id":new_id,"session":"source"}),
    )
    .unwrap();
    assert!(
        second_move.get("error").is_some(),
        "retransfer must not create a routing loop"
    );
    let save_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let saved = fs::read_to_string(target.session_dir.join("session.json"))
            .ok()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok());
        if saved
            .as_ref()
            .is_some_and(|saved| saved["workspaces"].as_array().is_some_and(|w| w.len() == 2))
            && !source.session_dir.join("session.json").exists()
        {
            break;
        }
        assert!(
            Instant::now() < save_deadline,
            "ordered transfer persistence did not settle"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn write_frame(stream: &mut UnixStream, value: &Value) {
    let bytes = serde_json::to_vec(value).unwrap();
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .unwrap();
    stream.write_all(&bytes).unwrap();
}

fn read_frame(stream: &mut UnixStream) -> Value {
    let mut length = [0; 4];
    stream.read_exact(&mut length).unwrap();
    let mut bytes = vec![0; u32::from_be_bytes(length) as usize];
    stream.read_exact(&mut bytes).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn refusing_destination(
    source: &Server,
    expected_panes: usize,
) -> (mpsc::Receiver<()>, mpsc::Sender<()>, thread::JoinHandle<()>) {
    let refusing_dir = source.session_dir.parent().unwrap().join("refusing");
    fs::create_dir_all(&refusing_dir).unwrap();
    let socket = refusing_dir.join("herdr.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
    let (invited, invitation) = mpsc::channel();
    let (release, proceed) = mpsc::channel();
    let protocol = rpc(&source.socket, "ping", json!({}))["result"]["protocol"]
        .as_u64()
        .unwrap();
    let fake = thread::spawn(move || {
        let (mut reader, request) = loop {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line.is_empty() {
                continue;
            }
            let request: Value = serde_json::from_str(&line).unwrap();
            match request["method"].as_str().unwrap() {
                "ping" => {
                    writeln!(reader.get_mut(),"{}",json!({"id":request["id"],"result":{"type":"pong","version":"test","protocol":protocol}})).unwrap();
                }
                "workspace.list" => {
                    writeln!(reader.get_mut(),"{}",json!({"id":request["id"],"result":{"type":"workspace_list","workspaces":[]}})).unwrap();
                }
                "workspace.transfer.import" => break (reader, request),
                other => panic!("unexpected fake-destination method: {other}"),
            }
        };
        invited.send(()).unwrap();
        proceed.recv_timeout(Duration::from_secs(5)).unwrap();
        writeln!(
            reader.get_mut(),
            "{}",
            json!({"id":request["id"],"result":{"type":"ok"}})
        )
        .unwrap();
        let mut transport =
            UnixStream::connect(request["params"]["socket_path"].as_str().unwrap()).unwrap();
        write_frame(&mut transport, &request["params"]["token"]);
        let manifest = read_frame(&mut transport);
        assert_eq!(manifest["panes"].as_array().unwrap().len(), expected_panes);
        write_frame(&mut transport, &json!("rejected"));
    });
    (invitation, release, fake)
}

#[test]
fn unavailable_and_rejected_destination_roll_back_without_blocking_other_panes() {
    let sandbox = Sandbox::new();
    let source = Server::start(&sandbox.base, "source-config", "source");
    let (workspace, pane) = create(&source, &sandbox.base);
    let (other_workspace, other_pane) = create(&source, &sandbox.base);
    let pid = shell_pid(&source.socket, &pane);
    let missing = try_rpc(
        &source.socket,
        "workspace.transfer",
        json!({"workspace_id":workspace,"session":"missing"}),
    )
    .unwrap();
    assert!(missing.get("error").is_some());
    send(&source.socket, &pane, "printf 'AFTER_UNAVAILABLE\\n'");
    wait_output(&source.socket, &pane, "AFTER_UNAVAILABLE");
    let (invitation, release, fake) = refusing_destination(&source, 1);
    let source_socket = source.socket.clone();
    let old_workspace = workspace.clone();
    let transfer = thread::spawn(move || {
        try_rpc(
            &source_socket,
            "workspace.transfer",
            json!({"workspace_id":old_workspace,"session":"refusing"}),
        )
        .unwrap()
    });
    invitation.recv_timeout(Duration::from_secs(5)).unwrap();
    let start = Instant::now();
    rpc(&source.socket, "ping", json!({}));
    send(&source.socket, &other_pane, "printf 'OTHER_RESPONSIVE\\n'");
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "transport must not block unrelated API/input"
    );
    let frozen = try_rpc(
        &source.socket,
        "pane.send_input",
        json!({"pane_id":pane,"text":"should-not-run","keys":["Enter"]}),
    )
    .unwrap();
    assert!(frozen.get("error").is_some());
    let stop = try_rpc(&source.socket, "server.stop", json!({})).unwrap();
    assert_eq!(stop["error"]["code"], "workspace_transfer_failed", "{stop}");
    rpc(&source.socket, "ping", json!({}));
    release.send(()).unwrap();
    fake.join().unwrap();
    assert!(transfer.join().unwrap().get("error").is_some());
    assert_eq!(shell_pid(&source.socket, &pane), pid);
    send(&source.socket, &pane, "printf 'AFTER_ROLLBACK\\n'");
    wait_output(&source.socket, &pane, "AFTER_ROLLBACK");
    wait_output(&source.socket, &other_pane, "OTHER_RESPONSIVE");
    let listed = rpc(&source.socket, "workspace.list", json!({}));
    let ids: Vec<_> = listed["result"]["workspaces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| w["workspace_id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&workspace.as_str()) && ids.contains(&other_workspace.as_str()));
}

#[test]
fn tab_alias_survives_workspace_session_transfer_without_redirecting_the_original_workspace() {
    let sandbox = Sandbox::new();
    let source = Server::start(&sandbox.base, "source-config", "source");
    let target = Server::start(&sandbox.base, "target-config", "target");
    symlink(
        &target.session_dir,
        source.session_dir.parent().unwrap().join("target"),
    )
    .unwrap();
    let (original_workspace, inherited_pane) = create(&source, &sandbox.base);
    let keep = rpc(
        &source.socket,
        "tab.create",
        json!({"workspace_id":original_workspace,"focus":false}),
    );
    let local_pane = keep["result"]["root_pane"]["pane_id"]
        .as_str()
        .unwrap()
        .to_string();
    let (export_workspace, _) = create(&source, &sandbox.base);
    let (collision_workspace, collision_pane) = create(&target, &sandbox.base);
    assert_eq!(
        inherited_pane, collision_pane,
        "target must contain the exact inherited-ID collision"
    );
    let pid = shell_pid(&source.socket, &inherited_pane);
    rpc(
        &source.socket,
        "tab.transfer",
        json!({"tab_id":format!("{original_workspace}:t1"),"workspace_id":export_workspace,"focus":false}),
    );
    let before = rpc(
        &source.socket,
        "pane.current",
        json!({"caller_pane_id":inherited_pane}),
    );
    let local_canonical = before["result"]["pane"]["pane_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(local_canonical.starts_with(&format!("{export_workspace}:")));
    assert_ne!(local_canonical, inherited_pane);

    // Alias-based input is frozen during prepare, while the still-local
    // original workspace remains usable. Rollback retains the local alias.
    let (invitation, release, fake) = refusing_destination(&source, 2);
    let socket = source.socket.clone();
    let exporting = export_workspace.clone();
    let transfer = thread::spawn(move || {
        try_rpc(
            &socket,
            "workspace.transfer",
            json!({"workspace_id":exporting,"session":"refusing"}),
        )
        .unwrap()
    });
    invitation.recv_timeout(Duration::from_secs(5)).unwrap();
    let frozen = try_rpc(
        &source.socket,
        "pane.send_input",
        json!({"pane_id":inherited_pane,"text":"alias-must-be-frozen","keys":["Enter"]}),
    )
    .unwrap();
    assert!(
        frozen.get("error").is_some(),
        "pending guard must resolve tab-transfer aliases"
    );
    send(
        &source.socket,
        &local_pane,
        "printf 'LOCAL_STILL_RESPONSIVE\\n'",
    );
    let socket = source.socket.clone();
    let report_pane = inherited_pane.clone();
    let report = thread::spawn(move || {
        rpc(
            &socket,
            "pane.report_metadata",
            json!({"pane_id":report_pane,"source":"alias-test","tokens":{"route":"rollback"},"seq":1}),
        )
    });
    release.send(()).unwrap();
    fake.join().unwrap();
    assert!(transfer.join().unwrap().get("error").is_some());
    report.join().unwrap();
    assert_eq!(shell_pid(&source.socket, &inherited_pane), pid);
    assert_eq!(
        rpc(
            &source.socket,
            "pane.get",
            json!({"pane_id":inherited_pane})
        )["result"]["pane"]["tokens"]["route"],
        "rollback"
    );

    let moved = rpc(
        &source.socket,
        "workspace.transfer",
        json!({"workspace_id":export_workspace,"session":"target"}),
    );
    let target_workspace = moved["result"]["workspace_id"].as_str().unwrap();
    let canonical = local_canonical.replacen(&export_workspace, target_workspace, 1);
    let current = rpc(
        &source.socket,
        "pane.current",
        json!({"caller_pane_id":inherited_pane}),
    );
    assert_eq!(current["result"]["pane"]["pane_id"], canonical);
    assert_eq!(shell_pid(&target.socket, &canonical), pid);
    rpc(
        &source.socket,
        "pane.report_metadata",
        json!({"pane_id":inherited_pane,"source":"alias-test","title":"inherited hook","tokens":{"route":"destination"},"seq":2}),
    );
    assert_eq!(
        rpc(&target.socket, "pane.get", json!({"pane_id":canonical}))["result"]["pane"]["tokens"]
            ["route"],
        "destination"
    );
    assert!(rpc(
        &target.socket,
        "pane.get",
        json!({"pane_id":collision_pane})
    )["result"]["pane"]["tokens"]
        .get("route")
        .is_none());
    assert!(
        rpc(&source.socket, "pane.get", json!({"pane_id":local_pane}))["result"]["pane"]["tokens"]
            .get("route")
            .is_none()
    );
    send(
        &source.socket,
        &inherited_pane,
        "printf 'INHERITED=%s\\n' \"$HERDR_PANE_ID\"",
    );
    wait_output(
        &target.socket,
        &canonical,
        &format!("INHERITED={inherited_pane}"),
    );
    wait_output(&source.socket, &local_pane, "LOCAL_STILL_RESPONSIVE");
    rpc(
        &source.socket,
        "workspace.rename",
        json!({"workspace_id":original_workspace,"label":"still local"}),
    );
    assert_eq!(
        rpc(
            &source.socket,
            "workspace.get",
            json!({"workspace_id":original_workspace})
        )["result"]["workspace"]["label"],
        "still local"
    );
    assert_ne!(
        rpc(
            &target.socket,
            "workspace.get",
            json!({"workspace_id":collision_workspace})
        )["result"]["workspace"]["label"],
        "still local"
    );
}

#[test]
fn duplicate_live_agent_name_rejects_before_commit_and_source_stays_interactive() {
    let sandbox = Sandbox::new();
    let source = Server::start(&sandbox.base, "source-config", "source");
    let target = Server::start(&sandbox.base, "target-config", "target");
    symlink(
        &target.session_dir,
        source.session_dir.parent().unwrap().join("target"),
    )
    .unwrap();
    let (workspace, source_pane) = create(&source, &sandbox.base);
    let (_, target_pane) = create(&target, &sandbox.base);
    let source_pid = shell_pid(&source.socket, &source_pane);
    let target_pid = shell_pid(&target.socket, &target_pane);
    for (server, pane, session) in [
        (&source, &source_pane, "source-agent"),
        (&target, &target_pane, "target-agent"),
    ] {
        rpc(
            &server.socket,
            "pane.report_agent",
            json!({"pane_id":pane,"source":"test","agent":"pi","state":"working","agent_session_id":session,"seq":1}),
        );
        rpc(
            &server.socket,
            "agent.rename",
            json!({"target":pane,"name":"reviewer"}),
        );
    }
    let rejected = try_rpc(
        &source.socket,
        "workspace.transfer",
        json!({"workspace_id":workspace,"session":"target"}),
    )
    .unwrap();
    let error = rejected["error"]["message"]
        .as_str()
        .expect("duplicate-name rejection");
    assert!(
        error.contains("reviewer") && error.contains("destination"),
        "must surface the conflicting live agent name: {rejected}"
    );
    assert_eq!(shell_pid(&source.socket, &source_pane), source_pid);
    assert_eq!(shell_pid(&target.socket, &target_pane), target_pid);
    for (server, pane) in [(&source, &source_pane), (&target, &target_pane)] {
        assert_eq!(
            rpc(&server.socket, "workspace.list", json!({}))["result"]["workspaces"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            rpc(&server.socket, "agent.get", json!({"target":pane}))["result"]["agent"]["name"],
            "reviewer"
        );
    }
    send(
        &source.socket,
        &source_pane,
        "printf 'AFTER_NAME_CONFLICT\\n'",
    );
    wait_output(&source.socket, &source_pane, "AFTER_NAME_CONFLICT");
    send(
        &target.socket,
        &target_pane,
        "printf 'TARGET_NAME_UNCHANGED\\n'",
    );
    wait_output(&target.socket, &target_pane, "TARGET_NAME_UNCHANGED");
}

#[test]
fn inherited_agent_wait_observes_destination_status_without_source_events() {
    let sandbox = Sandbox::new();
    let source = Server::start(&sandbox.base, "source-config", "source");
    let target = Server::start(&sandbox.base, "target-config", "target");
    symlink(
        &target.session_dir,
        source.session_dir.parent().unwrap().join("target"),
    )
    .unwrap();
    let (workspace, pane) = create(&source, &sandbox.base);
    let (local_workspace, _) = create(&source, &sandbox.base);
    create(&target, &sandbox.base);
    rpc(
        &source.socket,
        "pane.report_agent",
        json!({"pane_id":pane,"source":"test","agent":"pi","state":"working","agent_session_id":"wait-transfer","seq":1}),
    );
    let old_socket = source.socket.clone();
    let old_pane = pane.clone();
    let (finished, outstanding_response) = mpsc::channel();
    let outstanding = thread::spawn(move || {
        finished
            .send(try_rpc(
                &old_socket,
                "agent.wait",
                json!({"target":old_pane,"until":["idle"]}),
            ))
            .unwrap();
    });
    assert!(matches!(
        outstanding_response.recv_timeout(Duration::from_millis(300)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    let transferred = rpc(
        &source.socket,
        "workspace.transfer",
        json!({"workspace_id":workspace,"session":"target"}),
    );
    let closed = outstanding_response
        .recv_timeout(Duration::from_secs(4))
        .expect("source wait must terminate on transfer-out events")
        .unwrap();
    assert_eq!(closed["error"]["code"], "agent_not_running", "{closed}");
    outstanding.join().unwrap();
    let destination_workspace = transferred["result"]["workspace_id"].as_str().unwrap();
    let destination_pane = format!("{destination_workspace}:p1");
    let routed = rpc(&source.socket, "agent.get", json!({"target":pane}));
    assert_eq!(routed["result"]["agent"]["agent_status"], "working");
    assert_eq!(routed["workspace_transfer_forwarded"], true, "{routed}");
    let mut workers = Vec::new();
    let (done, responses) = mpsc::channel();
    for timeout in [Value::Null, json!(30_000)] {
        let socket = source.socket.clone();
        let pane = pane.clone();
        let done = done.clone();
        workers.push(thread::spawn(move || {
            done.send(try_rpc(
                &socket,
                "agent.wait",
                json!({"target":pane,"until":["idle","done"],"timeout_ms":timeout}),
            ))
            .unwrap();
        }));
    }
    assert!(
        matches!(
            responses.recv_timeout(Duration::from_millis(300)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ),
        "working agent must keep both waits pending"
    );
    assert_eq!(
        local_workspace, destination_workspace,
        "fixture must exercise cross-server workspace ID collision"
    );
    rpc(
        &source.socket,
        "workspace.close",
        json!({"workspace_id":local_workspace}),
    );
    rpc(
        &target.socket,
        "pane.report_agent",
        json!({"pane_id":destination_pane,"source":"test","agent":"pi","state":"idle","agent_session_id":"wait-transfer","seq":2}),
    );
    let idle = rpc(&source.socket, "agent.get", json!({"target":pane}));
    assert!(
        matches!(
            idle["result"]["agent"]["agent_status"].as_str(),
            Some("idle" | "done")
        ),
        "{idle}"
    );
    for _ in 0..2 {
        let response = responses
            .recv_timeout(Duration::from_secs(4))
            .expect("inherited wait must observe remote idle before its deadline")
            .unwrap();
        assert!(response.get("error").is_none(), "{response}");
        assert!(
            matches!(
                response["result"]["agent"]["agent_status"].as_str(),
                Some("idle" | "done")
            ),
            "{response}"
        );
        assert_eq!(response["result"]["agent"]["pane_id"], destination_pane);
    }
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
fn moving_to_a_stopped_saved_session_starts_it_restores_spaces_and_appends_same_pid_space() {
    let sandbox = Sandbox::new();
    // Both names use the same isolated config/server context, exactly as local
    // session discovery does. Their persisted session directories are distinct.
    let source = Server::start(&sandbox.base, "shared-config", "source");
    let saved = Server::start(&sandbox.base, "shared-config", "saved");
    let (saved_workspace, _) = create(&saved, &sandbox.base);
    rpc(
        &saved.socket,
        "workspace.rename",
        json!({"workspace_id":saved_workspace,"label":"saved original space"}),
    );
    let saved_dir = saved.session_dir.clone();
    let destination_socket = saved.socket.clone();
    drop(saved);
    assert!(
        UnixStream::connect(&destination_socket).is_err(),
        "destination must be stopped"
    );
    let snapshot: Value = serde_json::from_str(
        &fs::read_to_string(saved_dir.join("session.json")).expect("saved destination snapshot"),
    )
    .unwrap();
    assert_eq!(snapshot["workspaces"].as_array().unwrap().len(), 1);
    let (incoming, pane) = create(&source, &sandbox.base);
    let shell = shell_pid(&source.socket, &pane);
    let script = sandbox.base.join("saved-destination-io.py");
    fs::write(&script,"import os,sys\nprint('STARTED',os.getpid(),flush=True)\nfor line in sys.stdin:\n print('ACK',os.getpid(),line.strip(),flush=True)\n").unwrap();
    send(
        &source.socket,
        &pane,
        &format!("python3 -u '{}'", script.display()),
    );
    let text = wait_output(&source.socket, &pane, "STARTED ");
    let process = text
        .lines()
        .find_map(|line| line.strip_prefix("STARTED "))
        .unwrap()
        .trim()
        .to_string();
    let mut started = StartedDestination {
        socket: destination_socket.clone(),
        pids: Vec::new(),
    };
    let moved = rpc(
        &source.socket,
        "workspace.transfer",
        json!({"workspace_id":incoming,"session":"saved"}),
    );
    let target_workspace = moved["result"]["workspace_id"].as_str().unwrap();
    assert_ne!(
        target_workspace, saved_workspace,
        "restored destination identity must not collide"
    );
    let workspaces = rpc(&destination_socket, "workspace.list", json!({}));
    let spaces = workspaces["result"]["workspaces"].as_array().unwrap();
    assert_eq!(
        spaces.len(),
        2,
        "saved space must be restored before appending the incoming space"
    );
    assert!(spaces
        .iter()
        .any(|workspace| workspace["workspace_id"] == saved_workspace
            && workspace["label"] == "saved original space"));
    let canonical = pane.replacen(&incoming, target_workspace, 1);
    assert_eq!(shell_pid(&destination_socket, &canonical), shell);
    send(&source.socket, &pane, "after-start-and-append");
    wait_output(
        &destination_socket,
        &canonical,
        &format!("ACK {process} after-start-and-append"),
    );
    #[cfg(target_os = "linux")]
    for pid in
        support::herdr_server_pids_for_runtime_dir(&sandbox.base.join("rt-shared-config")).unwrap()
    {
        if pid != source.child.id() {
            support::register_spawned_herdr_pid(Some(pid));
            started.pids.push(pid);
        }
    }
}
