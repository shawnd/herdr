//! Start an existing inactive destination through normal session bootstrap.
//! This runs on the export worker, before source readers are quiesced.

use std::io;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::{Duration, Instant};

use serde_json::json;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);
const STATUS_TIMEOUT: Duration = Duration::from_secs(2);

fn listening(socket: &Path) -> io::Result<bool> {
    match super::validate_socket(socket) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    }
    match UnixStream::connect(socket) {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

fn compatible(status: &crate::api::RuntimeStatus) -> io::Result<()> {
    if status.protocol != Some(crate::protocol::PROTOCOL_VERSION) {
        return Err(io::Error::other(format!("destination session uses incompatible server protocol {:?}; workspace transfer requires {}",status.protocol,crate::protocol::PROTOCOL_VERSION)));
    }
    Ok(())
}

pub(super) fn ensure_ready(session: &str, socket: &Path) -> io::Result<()> {
    let directory = socket
        .parent()
        .ok_or_else(|| io::Error::other("destination session directory missing"))?;
    if !directory.is_dir() {
        return Err(io::Error::other(format!(
            "destination session '{session}' does not exist"
        )));
    }
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    // A listening but slow/incompatible server is never replaced or restarted.
    if !listening(socket)? {
        let mut command = crate::server::autodetect::named_server_daemon_command(session)?;
        let mut child = command.spawn().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("could not start destination session '{session}': {error}"),
            )
        })?;
        let pid = child.id();
        tracing::info!(
            pid,
            session,
            "starting inactive workspace-transfer destination"
        );
        // The new server is user state. Reap its eventual exit, but never kill
        // or delete it on startup timeout, validation failure, or rollback.
        let _ = std::thread::Builder::new()
            .name("herdr-destination-waiter".into())
            .spawn(move || {
                let _ = child.wait();
            });
    }
    loop {
        let remaining=deadline.checked_duration_since(Instant::now()).ok_or_else(||io::Error::new(io::ErrorKind::TimedOut,format!("destination session '{session}' did not finish restoring within {} seconds",STARTUP_TIMEOUT.as_secs())))?;
        match crate::api::read_runtime_status_at(socket, remaining.min(STATUS_TIMEOUT))? {
            Some(status) => {
                compatible(&status)?;
                super::validate_socket(socket)?;
                // ping can reply before App bootstrap finishes. workspace.list
                // is a rendezvous with the runtime loop after saved restore.
                let remaining =
                    deadline
                        .checked_duration_since(Instant::now())
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::TimedOut,
                                "destination restore readiness deadline exceeded",
                            )
                        })?;
                let response=super::rpc_inner(socket,&json!({"id":"transfer:destination-ready","method":"workspace.list","params":{}}),remaining,Some(remaining)).map_err(|error|io::Error::other(error.to_string()))?;
                if response.get("error").is_some() || response["result"]["type"] != "workspace_list"
                {
                    return Err(io::Error::other(
                        "destination session could not complete workspace restore readiness check",
                    ));
                }
                return Ok(());
            }
            None => std::thread::sleep(remaining.min(Duration::from_millis(50))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_running_incompatible_destination_is_rejected_without_restart() {
        let status = crate::api::RuntimeStatus {
            version: None,
            protocol: Some(crate::protocol::PROTOCOL_VERSION + 1),
            capabilities: None,
        };
        assert!(compatible(&status)
            .expect_err("incompatible protocol")
            .to_string()
            .contains("incompatible"));
    }
}
