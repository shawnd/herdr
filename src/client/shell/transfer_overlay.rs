use super::*;

pub(super) fn render_transfer_overlay(
    b: &mut Buffer,
    picker: &ClientTransferOverlay,
    p: &Palette,
) -> Option<OverlayRender> {
    let is_tab = matches!(picker.source, ClientTransferSource::Tab { .. });
    let popup_height = (picker.entries.len().saturating_mul(2) + 7).clamp(12, 26) as u16;
    let popup = popup(b.area, 96, popup_height)?;
    let inner = panel(b, popup, p.accent, p.panel_bg)?;
    if inner.height < 8 || inner.width < 20 {
        return Some(OverlayRender {
            area: popup,
            ..OverlayRender::default()
        });
    }
    let normal = Style::default().fg(p.text).bg(p.panel_bg);
    let muted = Style::default().fg(p.overlay0).bg(p.panel_bg);
    put_text(
        b,
        inner.x,
        inner.y,
        inner.width,
        if is_tab {
            "move tab to space"
        } else {
            "move workspace to session"
        },
        normal.add_modifier(Modifier::BOLD),
    );
    let search = Rect::new(inner.x, inner.y + 1, inner.width, 1);
    put_text(
        b,
        search.x,
        search.y,
        search.width,
        &if picker.search_focused {
            " / ".into()
        } else if !picker.query.is_empty() {
            format!(" / {}", picker.query)
        } else {
            " / filter destinations".into()
        },
        muted,
    );
    let cursor = picker
        .search_focused
        .then(|| {
            text_editor::render(
                b,
                Rect::new(search.x + 3, search.y, search.width.saturating_sub(3), 1),
                &picker.query,
                normal,
            )
        })
        .flatten();
    put_text(
        b,
        inner.x,
        inner.y + 2,
        inner.width,
        &"─".repeat(inner.width as usize),
        Style::default().fg(p.surface1).bg(p.panel_bg),
    );
    let body = Rect::new(
        inner.x,
        inner.y + 3,
        inner.width,
        inner.height.saturating_sub(6),
    );
    let filtered = picker.filtered_indices();
    let visible_count = (body.height / 2).max(1) as usize;
    let position = filtered
        .iter()
        .position(|index| *index == picker.selected)
        .unwrap_or(0);
    let start = position
        .saturating_sub(visible_count.saturating_sub(1))
        .min(filtered.len().saturating_sub(visible_count));
    let mut rows = Vec::new();
    for (visible, index) in filtered
        .iter()
        .copied()
        .skip(start)
        .take(visible_count)
        .enumerate()
    {
        let entry = &picker.entries[index];
        let rect = Rect::new(body.x, body.y + visible as u16 * 2, body.width, 2);
        rows.push((rect, index));
        let selected = index == picker.selected;
        let style = if selected {
            Style::default().fg(contrast(p)).bg(p.accent)
        } else {
            normal
        };
        b.set_style(rect, style);
        put_text(
            b,
            rect.x,
            rect.y,
            rect.width,
            &format!(" {}", entry.label),
            style.add_modifier(Modifier::BOLD),
        );
        put_text(
            b,
            rect.x,
            rect.y + 1,
            rect.width,
            &format!(" {}", entry.detail),
            if selected { style } else { muted },
        );
    }
    if picker.loading || filtered.is_empty() {
        let message = if picker.loading {
            " loading local sessions…"
        } else if !picker.query.is_empty() {
            " no matching destinations"
        } else if is_tab {
            " no other spaces in this session"
        } else {
            " no other saved local sessions on this server"
        };
        put_text(b, body.x, body.y, body.width, message, muted);
    }
    if picker.submitting {
        put_text(
            b,
            inner.x,
            inner.bottom() - 3,
            inner.width,
            " moving…",
            Style::default().fg(p.accent).bg(p.panel_bg),
        );
    } else if let Some(error) = &picker.error {
        put_text(
            b,
            inner.x,
            inner.bottom() - 3,
            inner.width,
            &format!(" {error}"),
            Style::default().fg(p.red).bg(p.panel_bg),
        );
    }
    let buttons = row(inner, &[10, 12], 2, inner.height.saturating_sub(1));
    let [primary, cancel] = buttons.as_slice() else {
        return None;
    };
    button(
        b,
        *primary,
        " ↵ move ",
        Style::default()
            .fg(contrast(p))
            .bg(p.accent)
            .add_modifier(Modifier::BOLD),
    );
    button(
        b,
        *cancel,
        " esc cancel ",
        Style::default()
            .fg(p.text)
            .bg(p.surface0)
            .add_modifier(Modifier::BOLD),
    );
    Some(OverlayRender {
        area: popup,
        primary: *primary,
        cancel: *cancel,
        worktree_search: search,
        worktree_rows: rows,
        cursor: cursor.filter(|_| !picker.submitting),
        ..OverlayRender::default()
    })
}
