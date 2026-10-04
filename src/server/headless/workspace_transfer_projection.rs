use super::*;

impl HeadlessServer {
    /// Reconcile transfer completions on the runtime loop, preserving the same
    /// notification and request forwarding boundary as ordinary API operations.
    pub(super) fn poll_workspace_transfers(&mut self) -> bool {
        let mut changed = self.workspace_transfers.poll(&mut self.app);
        for event in self.workspace_transfers.take_replayed_events() {
            changed |= self.handle_internal_event_with_forwarding(event);
        }
        for request in self.workspace_transfers.take_replayed_requests() {
            changed |= self.handle_api_request_with_shutdown_check(request);
        }
        if changed {
            self.reconcile_client_shell_locations();
            self.reapply_controlled_shell_tab_geometry(false);
            self.app.render_dirty.request_generic();
            self.app.render_notify.notify_one();
        }
        changed
    }
}
