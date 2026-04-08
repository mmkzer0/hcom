//! Instance identity, bindings, launch context, and instance utilities.

use crate::db::{HcomDb, InstanceRow};
use crate::shared::time::{now_epoch_f64, now_epoch_i64};
use crate::shared::ST_INACTIVE;

pub fn is_remote_instance(data: &InstanceRow) -> bool {
    data.origin_device_id.is_some()
}

pub fn is_subagent_instance(data: &InstanceRow) -> bool {
    data.parent_session_id.is_some()
}

/// Check if a session/instance is in subagent context (Task active).
///
/// Accepts either a session_id or instance name. Returns true if the instance
/// has active running tasks (i.e. a Task subagent is executing).
pub fn in_subagent_context(db: &HcomDb, session_id: &str) -> bool {
    let instance_name = match db.get_session_binding(session_id) {
        Ok(Some(name)) => name,
        _ => session_id.to_string(),
    };

    let row: Option<Option<String>> = db
        .conn()
        .query_row(
            "SELECT running_tasks FROM instances WHERE name = ? LIMIT 1",
            rusqlite::params![instance_name],
            |row| row.get::<_, Option<String>>(0),
        )
        .ok();

    match row {
        Some(Some(rt_json)) if !rt_json.is_empty() => {
            let rt = parse_running_tasks(Some(&rt_json));
            rt.active
        }
        _ => false,
    }
}

pub fn is_launching_placeholder(data: &InstanceRow) -> bool {
    data.session_id.is_none()
        && data.status_context == "new"
        && (data.status == ST_INACTIVE || data.status == "pending")
}

/// Get full display name: "{tag}-{name}" if tag exists, else just "{name}".
pub fn get_full_name(data: &InstanceRow) -> String {
    match &data.tag {
        Some(tag) if !tag.is_empty() => format!("{}-{}", tag, data.name),
        _ => data.name.clone(),
    }
}

/// Get display name for a base name by loading instance data.
pub fn get_display_name(db: &HcomDb, base_name: &str) -> String {
    match db.get_instance_full(base_name) {
        Ok(Some(data)) => get_full_name(&data),
        _ => base_name.to_string(),
    }
}

/// Resolve base name or tag-name (e.g., "team-luna") to base name.
/// Handles multi-hyphen tags like "vc-p0-p1-parallel-vani" → tag="vc-p0-p1-parallel", name="vani".
pub fn resolve_display_name(db: &HcomDb, input_name: &str) -> Option<String> {
    // Direct match
    if let Ok(Some(_)) = db.get_instance_full(input_name) {
        return Some(input_name.to_string());
    }
    // Try all possible tag-name split points (tags can contain hyphens)
    for (i, _) in input_name.match_indices('-') {
        let tag = &input_name[..i];
        let name = &input_name[i + 1..];
        if name.is_empty() {
            continue;
        }
        if let Ok(Some(data)) = db.get_instance_full(name) {
            if data.tag.as_deref() == Some(tag) {
                return Some(name.to_string());
            }
        }
    }
    None
}

/// Resolve base name or tag-name using live instances first, then stopped snapshots.
pub fn resolve_display_name_or_stopped(db: &HcomDb, input_name: &str) -> Option<String> {
    if let Some(name) = resolve_display_name(db, input_name) {
        return Some(name);
    }

    // Direct stopped-instance match by base name.
    if db
        .conn()
        .query_row(
            "SELECT instance FROM events
             WHERE type = 'life'
               AND instance = ?1
               AND json_extract(data, '$.action') = 'stopped'
             LIMIT 1",
            rusqlite::params![input_name],
            |row| row.get::<_, String>(0),
        )
        .ok()
        .is_some()
    {
        return Some(input_name.to_string());
    }

    // Stopped tag-name match against the stored snapshot tag (try all split points).
    for (i, _) in input_name.match_indices('-') {
        let tag = &input_name[..i];
        let name = &input_name[i + 1..];
        if name.is_empty() {
            continue;
        }
        if db
            .conn()
            .query_row(
                "SELECT instance FROM events
                 WHERE type = 'life'
                   AND instance = ?1
                   AND json_extract(data, '$.action') = 'stopped'
                   AND json_extract(data, '$.snapshot.tag') = ?2
                 LIMIT 1",
                rusqlite::params![name, tag],
                |row| row.get::<_, String>(0),
            )
            .ok()
            .is_some()
        {
            return Some(name.to_string());
        }
    }

    None
}

/// Parsed running_tasks JSON field.
#[derive(Debug, Clone, Default)]
pub struct RunningTasks {
    pub active: bool,
    pub subagents: Vec<serde_json::Value>,
}

pub fn parse_running_tasks(json_str: Option<&str>) -> RunningTasks {
    let Some(s) = json_str else {
        return RunningTasks::default();
    };
    if s.is_empty() {
        return RunningTasks::default();
    }

    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(serde_json::Value::Object(obj)) => RunningTasks {
            active: obj.get("active").and_then(|v| v.as_bool()).unwrap_or(false),
            subagents: obj
                .get("subagents")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
        },
        _ => RunningTasks::default(),
    }
}

/// Update instance position atomically.
/// If instance doesn't exist, UPDATE silently affects 0 rows.
pub fn update_instance_position(
    db: &HcomDb,
    name: &str,
    updates: &serde_json::Map<String, serde_json::Value>,
) {
    // Convert booleans to integers for SQLite
    let mut update_copy = updates.clone();
    for bool_field in &["tcp_mode", "background", "name_announced"] {
        if let Some(val) = update_copy.get(*bool_field) {
            if let Some(b) = val.as_bool() {
                update_copy.insert(
                    (*bool_field).to_string(),
                    serde_json::json!(if b { 1 } else { 0 }),
                );
            }
        }
    }

    if let Err(e) = db.update_instance_fields(name, &update_copy) {
        crate::log::log_error(
            "core",
            "db.error",
            &format!("update_instance_position: {} - {}", name, e),
        );
    }
}

/// Persist terminal launch metadata without clobbering other launch_context fields.
///
/// The launcher owns the authoritative preset decision. launch_context is only
/// for late-bound metadata such as pane_id, terminal_id, and env snapshot.
pub fn persist_terminal_launch_context(
    db: &HcomDb,
    instance_name: &str,
    requested_preset: Option<&str>,
    effective_preset: &str,
    process_id: Option<&str>,
) {
    let mut ctx = db
        .get_instance_full(instance_name)
        .ok()
        .flatten()
        .and_then(|pos| pos.launch_context)
        .and_then(|json| {
            serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&json).ok()
        })
        .unwrap_or_default();

    if let Some(pid) = process_id.filter(|v| !v.is_empty()) {
        ctx.insert("process_id".into(), serde_json::json!(pid));
    }
    if !effective_preset.is_empty() {
        ctx.insert(
            "terminal_preset_effective".into(),
            serde_json::json!(effective_preset),
        );
        // Legacy compatibility for older readers and migration logic.
        ctx.insert(
            "terminal_preset".into(),
            serde_json::json!(effective_preset),
        );
    }
    if let Some(requested) = requested_preset.filter(|v| !v.is_empty() && *v != "default") {
        ctx.insert(
            "terminal_preset_requested".into(),
            serde_json::json!(requested),
        );
    }

    let mut updates = serde_json::Map::new();
    updates.insert(
        "terminal_preset_requested".into(),
        serde_json::json!(
            requested_preset
                .filter(|v| !v.is_empty() && *v != "default")
                .unwrap_or("")
        ),
    );
    updates.insert(
        "terminal_preset_effective".into(),
        serde_json::json!(effective_preset),
    );
    updates.insert(
        "launch_context".into(),
        serde_json::json!(serde_json::to_string(&ctx).unwrap_or_else(|_| "{}".to_string())),
    );
    update_instance_position(db, instance_name, &updates);
}

/// Capture environment context and store it for the instance.
/// Captures git branch, terminal program, tty, and relevant env vars.
pub fn capture_and_store_launch_context(db: &HcomDb, instance_name: &str) {
    let new_ctx = capture_context();

    // Preserve fields from prior context that can't be recaptured in hook env
    let preserve_keys = ["pane_id", "terminal_id", "kitty_listen_on", "process_id"];
    let mut ctx = new_ctx;

    let missing: Vec<&str> = preserve_keys
        .iter()
        .filter(|k| {
            ctx.get(**k)
                .and_then(|v| v.as_str())
                .is_none_or(|s| s.is_empty())
        })
        .copied()
        .collect();

    if !missing.is_empty() {
        if let Ok(Some(pos)) = db.get_instance_full(instance_name) {
            if let Some(old_json) = &pos.launch_context {
                if let Ok(old_ctx) = serde_json::from_str::<serde_json::Value>(old_json) {
                    for k in &missing {
                        if let Some(val) = old_ctx.get(*k) {
                            if let Some(s) = val.as_str() {
                                if !s.is_empty() {
                                    ctx.insert(k.to_string(), val.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let json = serde_json::to_string(&ctx).unwrap_or_else(|_| "{}".to_string());
    let mut updates = serde_json::Map::new();
    updates.insert("launch_context".into(), serde_json::json!(json));
    update_instance_position(db, instance_name, &updates);
}

/// Capture launch context snapshot.
fn capture_context() -> serde_json::Map<String, serde_json::Value> {
    let mut ctx = serde_json::Map::new();

    // Git branch
    let git_branch = std::process::Command::new("git")
        .args(["branch", "--show-current"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    ctx.insert("git_branch".into(), serde_json::json!(git_branch));

    // TTY
    let tty = std::process::Command::new("tty")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    ctx.insert("tty".into(), serde_json::json!(tty));

    // Env vars (only include if set)
    let env_keys = [
        "TERM_PROGRAM",
        "TERM_SESSION_ID",
        "WINDOWID",
        "ITERM_SESSION_ID",
        "KITTY_WINDOW_ID",
        "KITTY_PID",
        "KITTY_LISTEN_ON",
        "ALACRITTY_WINDOW_ID",
        "WEZTERM_PANE",
        "GNOME_TERMINAL_SCREEN",
        "KONSOLE_DBUS_WINDOW",
        "TERMINATOR_UUID",
        "TILIX_ID",
        "GUAKE_TAB_UUID",
        "WT_SESSION",
        "ConEmuHWND",
        "TMUX_PANE",
        "STY",
        "ZELLIJ_SESSION_NAME",
        "ZELLIJ_PANE_ID",
        "SSH_TTY",
        "SSH_CONNECTION",
        "WSL_DISTRO_NAME",
        "VSCODE_PID",
        "CURSOR_AGENT",
        "INSIDE_EMACS",
        "NVIM_LISTEN_ADDRESS",
        "CODESPACE_NAME",
        "GITPOD_WORKSPACE_ID",
        "CLOUD_SHELL",
        "REPL_ID",
    ];
    let mut env_map = serde_json::Map::new();
    for key in &env_keys {
        if let Ok(val) = std::env::var(key) {
            if !val.is_empty() {
                env_map.insert((*key).to_string(), serde_json::json!(val));
            }
        }
    }
    ctx.insert("env".into(), serde_json::Value::Object(env_map));

    // Pane IDs are late-bound. The launcher already persisted the effective preset.
    if let Ok(preset_name) = std::env::var("HCOM_LAUNCHED_PRESET") {
        if !preset_name.is_empty() {
            if let Some(pane_id_env) = crate::config::get_merged_preset_pane_id_env(&preset_name) {
                if let Ok(pane_id) = std::env::var(pane_id_env) {
                    if !pane_id.is_empty() {
                        ctx.insert("pane_id".into(), serde_json::json!(pane_id));
                    }
                }
            }
        }
    }

    // Process ID for kitty close-by-env matching
    if let Ok(pid) = std::env::var("HCOM_PROCESS_ID") {
        if !pid.is_empty() {
            ctx.insert("process_id".into(), serde_json::json!(pid));

            // Terminal ID from parent's stdout capture
            let id_file = crate::paths::hcom_dir()
                .join(".tmp")
                .join("terminal_ids")
                .join(&pid);
            if id_file.exists() {
                if let Ok(content) = std::fs::read_to_string(&id_file) {
                    let terminal_id = content.trim().to_string();
                    if !terminal_id.is_empty() {
                        ctx.insert("terminal_id".into(), serde_json::json!(terminal_id));
                    }
                }
                let _ = std::fs::remove_file(&id_file);
            }
        }
    }

    ctx
}

/// Bind session_id to canonical instance for process_id.
/// Handles 4 paths: canonical exists (with placeholder merge/switch), placeholder bind,
/// and two no-op paths.
pub fn bind_session_to_process(
    db: &HcomDb,
    session_id: &str,
    process_id: Option<&str>,
) -> Option<String> {
    if session_id.is_empty() {
        crate::log::log_info("binding", "bind_session_to_process.no_session_id", "");
        return None;
    }

    crate::log::log_info(
        "binding",
        "bind_session_to_process.entry",
        &format!("session_id={}, process_id={:?}", session_id, process_id),
    );

    // Find placeholder from process binding
    let (placeholder_name, placeholder_data) = if let Some(pid) = process_id {
        match db.get_process_binding(pid) {
            Ok(Some(name)) => {
                let data = match db.get_instance_full(&name) {
                    Ok(d) => d,
                    Err(e) => {
                        eprintln!("[hcom] warn: get_instance_full failed for {name}: {e}");
                        None
                    }
                };
                (Some(name), data)
            }
            _ => (None, None),
        }
    } else {
        (None, None)
    };

    // Find canonical from session binding
    let canonical = match db.get_session_binding(session_id) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[hcom] warn: get_session_binding failed for {session_id}: {e}");
            None
        }
    };

    // Path 1: Canonical exists (session already bound)
    if let Some(ref canonical_name) = canonical {
        crate::log::log_info(
            "binding",
            "bind_session_to_process.canonical_exists",
            &format!(
                "canonical={}, placeholder={:?}",
                canonical_name, placeholder_name
            ),
        );

        // Reset last_stop on resume
        let now = now_epoch_i64();
        let mut resume_updates = serde_json::Map::new();
        resume_updates.insert("last_stop".into(), serde_json::json!(now));

        if let Some(ref ph_name) = placeholder_name {
            if ph_name != canonical_name {
                // Always migrate notify_endpoints
                if let Err(e) = db.migrate_notify_endpoints(ph_name, canonical_name) {
                    crate::log::log_error(
                        "binding",
                        "bind_canonical.migrate_endpoints",
                        &format!("{e}"),
                    );
                }

                let is_true_placeholder = placeholder_data
                    .as_ref()
                    .map(|d| d.session_id.is_none())
                    .unwrap_or(false);

                if is_true_placeholder {
                    // Path 1a: True placeholder merge
                    if let Some(ref ph_data) = placeholder_data {
                        if let Some(ref tag) = ph_data.tag {
                            resume_updates.insert("tag".into(), serde_json::json!(tag));
                        }
                        if ph_data.background != 0 {
                            resume_updates
                                .insert("background".into(), serde_json::json!(ph_data.background));
                        }
                        if let Some(ref args) = ph_data.launch_args {
                            resume_updates.insert("launch_args".into(), serde_json::json!(args));
                        }
                        // Reset status_context for ready event
                        if std::env::var("HCOM_LAUNCHED").as_deref() == Ok("1") {
                            resume_updates
                                .insert("status_context".into(), serde_json::json!("new"));
                        }
                    }

                    // Delete true placeholder (temporary identity)
                    match db.delete_instance(ph_name) {
                        Ok(true) => {} // Success
                        Ok(false) => {
                            // Not found — rollback notify_endpoints migration
                            if let Err(e) = db.migrate_notify_endpoints(canonical_name, ph_name) {
                                crate::log::log_error(
                                    "binding",
                                    "bind_canonical.rollback_endpoints",
                                    &format!("{e}"),
                                );
                            }
                        }
                        Err(e) => {
                            crate::log::log_error(
                                "binding",
                                "bind_canonical.delete_placeholder",
                                &format!("{e}"),
                            );
                            if let Err(e) = db.migrate_notify_endpoints(canonical_name, ph_name) {
                                crate::log::log_error(
                                    "binding",
                                    "bind_canonical.rollback_endpoints",
                                    &format!("{e}"),
                                );
                            }
                        }
                    }
                } else {
                    // Path 1b: Session switch — mark old instance inactive
                    crate::instance_lifecycle::set_status(
                        db,
                        ph_name,
                        ST_INACTIVE,
                        "exit:session_switch",
                        Default::default(),
                    );
                    if let Err(e) = db.delete_session_bindings_for_instance(ph_name) {
                        crate::log::log_error(
                            "binding",
                            "bind_canonical.delete_session_bindings",
                            &format!("{e}"),
                        );
                    }
                }
            }
        }

        // Apply resume updates
        update_instance_position(db, canonical_name, &resume_updates);

        if let Some(pid) = process_id {
            if let Err(e) = db.set_process_binding(pid, session_id, canonical_name) {
                crate::log::log_error(
                    "binding",
                    "bind_canonical.set_process_binding",
                    &format!("{e}"),
                );
            }
        }

        return Some(canonical_name.clone());
    }

    // Path 2: No canonical, but placeholder exists — bind session to placeholder
    if let Some(ref ph_name) = placeholder_name {
        crate::log::log_info(
            "binding",
            "bind_session_to_process.bind_placeholder",
            &format!("placeholder={}, session_id={}", ph_name, session_id),
        );

        // Clear UNIQUE constraint conflicts
        if let Err(e) = db.clear_session_id_from_other_instances(session_id, ph_name) {
            crate::log::log_error("binding", "bind_placeholder.clear_session", &format!("{e}"));
        }

        let mut updates = serde_json::Map::new();
        updates.insert("session_id".into(), serde_json::json!(session_id));
        update_instance_position(db, ph_name, &updates);

        if let Err(e) = db.rebind_session(session_id, ph_name) {
            crate::log::log_error(
                "binding",
                "bind_placeholder.rebind_session",
                &format!("{e}"),
            );
        }
        if let Some(pid) = process_id {
            if let Err(e) = db.set_process_binding(pid, session_id, ph_name) {
                crate::log::log_error(
                    "binding",
                    "bind_placeholder.set_process_binding",
                    &format!("{e}"),
                );
            }
        }

        return Some(ph_name.clone());
    }

    // Path 3/4: No canonical, no placeholder — no-op
    crate::log::log_info("binding", "bind_session_to_process.return_none", "");
    None
}

/// Initialize instance in DB with required fields (idempotent).
#[allow(clippy::too_many_arguments)]
pub fn initialize_instance_in_position_file(
    db: &HcomDb,
    instance_name: &str,
    session_id: Option<&str>,
    parent_session_id: Option<&str>,
    parent_name: Option<&str>,
    agent_id: Option<&str>,
    transcript_path: Option<&str>,
    tool: Option<&str>,
    background: bool,
    tag: Option<&str>,
    wait_timeout: Option<i64>,
    subagent_timeout: Option<i64>,
    hints: Option<&str>,
    cwd_override: Option<&str>,
) -> bool {
    let cwd = cwd_override.map(|s| s.to_string()).unwrap_or_else(|| {
        std::env::current_dir()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default()
    });
    let is_launched = std::env::var("HCOM_LAUNCHED").as_deref() == Ok("1");

    // Check if already exists
    match db.get_instance_full(instance_name) {
        Ok(Some(existing)) => {
            let mut updates = serde_json::Map::new();
            updates.insert("directory".into(), serde_json::json!(cwd));

            if let Some(sid) = session_id {
                updates.insert("session_id".into(), serde_json::json!(sid));
            }
            if let Some(psid) = parent_session_id {
                updates.insert("parent_session_id".into(), serde_json::json!(psid));
            }
            if let Some(pn) = parent_name {
                updates.insert("parent_name".into(), serde_json::json!(pn));
            }
            if let Some(aid) = agent_id {
                updates.insert("agent_id".into(), serde_json::json!(aid));
            }
            if let Some(tp) = transcript_path {
                updates.insert("transcript_path".into(), serde_json::json!(tp));
            }
            if let Some(t) = tool {
                updates.insert("tool".into(), serde_json::json!(t));
            }
            if let Some(t) = tag {
                updates.insert("tag".into(), serde_json::json!(t));
            }
            if background {
                updates.insert("background".into(), serde_json::json!(1));
            }

            // Fix last_event_id for true placeholders
            let is_true_placeholder = existing.session_id.is_none();
            if existing.last_event_id == 0 && is_true_placeholder {
                let current_max = db.get_last_event_id();
                let launch_event_id = std::env::var("HCOM_LAUNCH_EVENT_ID")
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok());

                let eid = match launch_event_id {
                    Some(id) if id <= current_max => id,
                    _ => current_max,
                };
                updates.insert("last_event_id".into(), serde_json::json!(eid));
            }

            if is_launched {
                updates.insert("status_context".into(), serde_json::json!("new"));
            }

            if !updates.is_empty() {
                let _ = db.update_instance_fields(instance_name, &updates);
            }

            true
        }
        Ok(None) => {
            // New instance
            let now = now_epoch_f64();
            let current_max = db.get_last_event_id();
            let launch_event_id = std::env::var("HCOM_LAUNCH_EVENT_ID")
                .ok()
                .and_then(|s| s.parse::<i64>().ok());

            let initial_event_id = match launch_event_id {
                Some(id) if id <= current_max => id,
                _ => current_max,
            };

            let mut data = serde_json::Map::new();
            data.insert("name".into(), serde_json::json!(instance_name));
            data.insert("last_event_id".into(), serde_json::json!(initial_event_id));
            data.insert("directory".into(), serde_json::json!(cwd));
            data.insert("last_stop".into(), serde_json::json!(0));
            data.insert("created_at".into(), serde_json::json!(now));
            data.insert(
                "session_id".into(),
                match session_id {
                    Some(s) if !s.is_empty() => serde_json::json!(s),
                    _ => serde_json::Value::Null,
                },
            );
            data.insert("transcript_path".into(), serde_json::json!(""));
            data.insert("name_announced".into(), serde_json::json!(0));
            data.insert("tag".into(), serde_json::Value::Null);
            data.insert("status".into(), serde_json::json!(ST_INACTIVE));
            data.insert("status_time".into(), serde_json::json!(now_epoch_i64()));
            data.insert("status_context".into(), serde_json::json!("new"));
            data.insert("tool".into(), serde_json::json!(tool.unwrap_or("claude")));
            data.insert(
                "background".into(),
                serde_json::json!(if background { 1 } else { 0 }),
            );

            // Set tag: use provided tag, or fall back to config tag for real instances
            if let Some(t) = tag {
                data.insert("tag".into(), serde_json::json!(t));
            } else if session_id.is_some() || parent_session_id.is_some() || is_launched {
                if let Ok(hcom_config) = crate::config::HcomConfig::load(None) {
                    if !hcom_config.tag.is_empty() {
                        data.insert("tag".into(), serde_json::json!(hcom_config.tag));
                    }
                }
            }

            if let Some(wt) = wait_timeout {
                data.insert("wait_timeout".into(), serde_json::json!(wt));
            }
            if let Some(st) = subagent_timeout {
                data.insert("subagent_timeout".into(), serde_json::json!(st));
            }
            if let Some(h) = hints {
                data.insert("hints".into(), serde_json::json!(h));
            }
            if let Some(psid) = parent_session_id {
                data.insert("parent_session_id".into(), serde_json::json!(psid));
            }
            if let Some(pn) = parent_name {
                data.insert("parent_name".into(), serde_json::json!(pn));
            }
            if let Some(aid) = agent_id {
                data.insert("agent_id".into(), serde_json::json!(aid));
            }
            if let Some(tp) = transcript_path {
                data.insert("transcript_path".into(), serde_json::json!(tp));
            }

            match db.save_instance_named(instance_name, &data) {
                Ok(true) => {
                    // Log creation event
                    let launcher =
                        std::env::var("HCOM_LAUNCHED_BY").unwrap_or_else(|_| "unknown".to_string());
                    let event_data = serde_json::json!({
                        "action": "created",
                        "by": launcher,
                        "is_hcom_launched": is_launched,
                        "is_subagent": parent_session_id.is_some(),
                        "parent_name": parent_name.unwrap_or(""),
                    });
                    let _ = db.log_event("life", instance_name, &event_data);
                    // Auto-subscribe to default event presets from config
                    auto_subscribe_defaults(db, instance_name, tool.unwrap_or(""));
                    true
                }
                _ => true, // IntegrityError = another process won the race, treat as success
            }
        }
        Err(_) => false,
    }
}

/// Create orphaned PTY identity — called when process binding exists but session_id
/// is fresh (e.g., after /clear). Generates new name, creates instance, binds it.
pub fn create_orphaned_pty_identity(
    db: &HcomDb,
    session_id: &str,
    process_id: Option<&str>,
    tool: &str,
) -> Option<String> {
    let name = match crate::instance_names::generate_unique_name(db) {
        Ok(n) => n,
        Err(e) => {
            crate::log::log_error(
                "instances",
                "create_orphaned_pty_identity.name_gen",
                &e.to_string(),
            );
            return None;
        }
    };

    let success = initialize_instance_in_position_file(
        db,
        &name,
        Some(session_id),
        None, // parent_session_id
        None, // parent_name
        None, // agent_id
        None, // transcript_path
        Some(tool),
        false, // background
        None,  // tag
        None,  // wait_timeout
        None,  // subagent_timeout
        None,  // hints
        None,  // cwd_override
    );

    if !success {
        return None;
    }

    // Bind session
    if let Err(e) = db.rebind_session(session_id, &name) {
        eprintln!("[hcom] warn: rebind_session failed for {name}: {e}");
    }
    if let Some(pid) = process_id {
        if let Err(e) = db.set_process_binding(pid, session_id, &name) {
            eprintln!("[hcom] warn: set_process_binding failed for {name}: {e}");
        }
    }

    Some(name)
}

/// Resolve instance name for a process_id via process_bindings.
pub fn resolve_process_binding(db: &HcomDb, process_id: Option<&str>) -> Option<String> {
    let pid = process_id?;
    db.get_process_binding(pid).ok()?
}

/// Resolve instance via process binding, session binding, or transcript marker.
pub fn resolve_instance_from_binding(
    db: &HcomDb,
    session_id: Option<&str>,
    process_id: Option<&str>,
) -> Option<InstanceRow> {
    // Path 1: Process binding
    if let Some(pid) = process_id {
        if let Ok(Some(name)) = db.get_process_binding(pid) {
            if let Ok(Some(instance)) = db.get_instance_full(&name) {
                return Some(instance);
            }
        }
    }

    // Path 2: Session binding
    if let Some(sid) = session_id {
        if let Some(name) = db.get_session_binding(sid).ok().flatten() {
            if let Ok(Some(instance)) = db.get_instance_full(&name) {
                return Some(instance);
            }
        }
    }

    None
}

/// Auto-subscribe instance to default event subscriptions from config.
/// Called during instance creation.
fn auto_subscribe_defaults(db: &HcomDb, instance_name: &str, tool: &str) {
    if !matches!(tool, "claude" | "gemini" | "codex" | "opencode") {
        return;
    }

    // Clean up stale subscriptions from previously stopped instances with reused names
    let _ = db.cleanup_subscriptions(instance_name);
    let _ = db.cleanup_thread_memberships_for_name_reuse(instance_name);
    let config = match crate::config::HcomConfig::load(None) {
        Ok(c) => c,
        Err(_) => return,
    };
    if config.auto_subscribe.is_empty() {
        return;
    }

    use std::collections::HashMap;

    // Map preset names to filter flags
    let preset_to_flags: HashMap<&str, Vec<(&str, &str)>> = HashMap::from([
        ("collision", vec![("collision", "1")]),
        ("created", vec![("action", "created")]),
        ("stopped", vec![("action", "stopped")]),
        ("blocked", vec![("status", "blocked")]),
    ]);

    for preset in config
        .auto_subscribe
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        if let Some(flag_pairs) = preset_to_flags.get(preset) {
            let mut filters: HashMap<String, Vec<String>> = HashMap::new();
            for (key, val) in flag_pairs {
                filters
                    .entry(key.to_string())
                    .or_default()
                    .push(val.to_string());
            }
            let _ = crate::commands::events::create_filter_subscription(
                db,
                &filters,
                &[],
                instance_name,
                false,
                true,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance_names::{
        allocate_name, banned_names, gold_names, hash_to_name, is_too_similar, name_pool,
        score_name,
    };
    use rusqlite::Connection;
    use std::collections::HashSet;
    use std::path::PathBuf;

    fn setup_test_db() -> (HcomDb, PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_instances_{}_{}.db",
            std::process::id(),
            test_id
        ));

        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA foreign_keys=ON;
             PRAGMA journal_mode=WAL;

             CREATE TABLE events (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 timestamp TEXT NOT NULL,
                 type TEXT NOT NULL,
                 instance TEXT,
                 data TEXT NOT NULL
             );

             CREATE TABLE instances (
                 name TEXT PRIMARY KEY,
                 session_id TEXT UNIQUE,
                 parent_session_id TEXT,
                 parent_name TEXT,
                 tag TEXT,
                 last_event_id INTEGER DEFAULT 0,
                 status TEXT DEFAULT 'active',
                 status_time INTEGER DEFAULT 0,
                 status_context TEXT DEFAULT '',
                 status_detail TEXT DEFAULT '',
                 last_stop INTEGER DEFAULT 0,
                 directory TEXT,
                 created_at REAL NOT NULL DEFAULT 0,
                 transcript_path TEXT DEFAULT '',
                 tcp_mode INTEGER DEFAULT 0,
                 wait_timeout INTEGER DEFAULT 86400,
                 background INTEGER DEFAULT 0,
                 background_log_file TEXT DEFAULT '',
                 name_announced INTEGER DEFAULT 0,
                 agent_id TEXT UNIQUE,
                 running_tasks TEXT DEFAULT '',
                 origin_device_id TEXT DEFAULT '',
                 hints TEXT DEFAULT '',
                 subagent_timeout INTEGER,
                 tool TEXT DEFAULT 'claude',
                 launch_args TEXT DEFAULT '',
                 terminal_preset_requested TEXT DEFAULT '',
                 terminal_preset_effective TEXT DEFAULT '',
                 idle_since TEXT DEFAULT '',
                 pid INTEGER DEFAULT NULL,
                 launch_context TEXT DEFAULT '',
                 FOREIGN KEY (parent_session_id) REFERENCES instances(session_id) ON DELETE SET NULL
             );

             CREATE TABLE process_bindings (
                 process_id TEXT PRIMARY KEY,
                 session_id TEXT,
                 instance_name TEXT,
                 updated_at REAL NOT NULL
             );

             CREATE TABLE session_bindings (
                 session_id TEXT PRIMARY KEY,
                 instance_name TEXT NOT NULL,
                 created_at REAL NOT NULL,
                 FOREIGN KEY (instance_name) REFERENCES instances(name) ON DELETE CASCADE
             );

             CREATE TABLE notify_endpoints (
                 instance TEXT NOT NULL,
                 kind TEXT NOT NULL,
                 port INTEGER NOT NULL,
                 updated_at REAL NOT NULL,
                 PRIMARY KEY (instance, kind)
             );

             CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT);",
        )
        .unwrap();
        drop(conn);

        let db = HcomDb::open_raw(&db_path).unwrap();
        (db, db_path)
    }

    fn cleanup(path: PathBuf) {
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[test]
    fn test_name_pool_populated() {
        let pool = name_pool();
        assert!(pool.len() > 1000, "pool should have >1000 names");
        // Gold names should be at the top
        let top_100: HashSet<&str> = pool[..100].iter().map(|x| x.name.as_str()).collect();
        assert!(top_100.contains("luna"), "luna should be in top 100");
        assert!(top_100.contains("nova"), "nova should be in top 100");
    }

    #[test]
    fn test_banned_names_excluded() {
        let pool = name_pool();
        let all_names: HashSet<&str> = pool.iter().map(|x| x.name.as_str()).collect();
        assert!(!all_names.contains("help"));
        assert!(!all_names.contains("send"));
        assert!(!all_names.contains("list"));
        assert!(!all_names.contains("stop"));
    }

    #[test]
    fn test_gold_names_score_higher() {
        let gold = gold_names();
        let banned = banned_names();
        let gold_score = score_name("luna", &gold, &banned);
        let non_gold = score_name("bxzx", &gold, &banned); // unlikely to be gold
        assert!(gold_score > non_gold, "gold names should score higher");
    }

    #[test]
    fn test_hamming_similarity_check() {
        let mut alive = HashSet::new();
        alive.insert("luna".to_string());

        // 1 char different = too similar
        assert!(is_too_similar("lina", &alive));
        assert!(is_too_similar("luno", &alive));
        // 2 chars different = too similar
        assert!(is_too_similar("lino", &alive));
        // 3+ chars different = ok
        assert!(!is_too_similar("miso", &alive));
        assert!(!is_too_similar("kira", &alive));
    }

    #[test]
    fn test_allocate_name_avoids_taken() {
        let taken: HashSet<String> = ["luna", "nova", "kira"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let alive = taken.clone();
        let name = allocate_name(&|n| taken.contains(n), &alive, 200, 1200, 900.0).unwrap();
        assert!(!taken.contains(&name));
    }

    #[test]
    fn test_hash_to_name_deterministic() {
        let n1 = hash_to_name("device-123", 0);
        let n2 = hash_to_name("device-123", 0);
        assert_eq!(n1, n2);
    }

    #[test]
    fn test_hash_to_name_collision_avoidance() {
        let n1 = hash_to_name("device-123", 0);
        let n2 = hash_to_name("device-123", 1);
        assert_ne!(n1, n2);
    }

    #[test]
    fn test_persist_terminal_launch_context_stores_presets_in_launch_context() {
        let (db, path) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, tool, created_at) VALUES (?1, ?2, ?3)",
                rusqlite::params!["luna", "codex", 1.0f64],
            )
            .unwrap();

        persist_terminal_launch_context(&db, "luna", Some("kitty"), "kitty-tab", Some("proc-1"));

        let row = db.get_instance_full("luna").unwrap().unwrap();
        let ctx: serde_json::Value =
            serde_json::from_str(row.launch_context.as_deref().unwrap_or("{}")).unwrap();

        assert_eq!(
            ctx.get("terminal_preset_effective")
                .and_then(|v| v.as_str()),
            Some("kitty-tab")
        );
        assert_eq!(
            ctx.get("terminal_preset").and_then(|v| v.as_str()),
            Some("kitty-tab")
        );
        assert_eq!(
            ctx.get("terminal_preset_requested")
                .and_then(|v| v.as_str()),
            Some("kitty")
        );
        assert_eq!(
            ctx.get("process_id").and_then(|v| v.as_str()),
            Some("proc-1")
        );

        cleanup(path);
    }

    #[test]
    fn test_is_launching_placeholder() {
        let ph = InstanceRow {
            status: ST_INACTIVE.into(),
            status_context: "new".into(),
            session_id: None,
            ..default_instance()
        };
        assert!(is_launching_placeholder(&ph));

        let bound = InstanceRow {
            status: ST_INACTIVE.into(),
            status_context: "new".into(),
            session_id: Some("sid-123".into()),
            ..default_instance()
        };
        assert!(!is_launching_placeholder(&bound));
    }

    #[test]
    fn test_is_remote_instance() {
        let local = default_instance();
        assert!(!is_remote_instance(&local));

        let remote = InstanceRow {
            origin_device_id: Some("device-123".into()),
            ..default_instance()
        };
        assert!(is_remote_instance(&remote));
    }

    #[test]
    fn test_get_full_name() {
        let plain = InstanceRow {
            name: "luna".into(),
            tag: None,
            ..default_instance()
        };
        assert_eq!(get_full_name(&plain), "luna");

        let tagged = InstanceRow {
            name: "luna".into(),
            tag: Some("team".into()),
            ..default_instance()
        };
        assert_eq!(get_full_name(&tagged), "team-luna");
    }

    #[test]
    fn test_resolve_display_name_or_stopped_tagged_snapshot() {
        let (db, _path) = setup_test_db();
        db.conn()
            .execute(
                "INSERT INTO events (timestamp, type, instance, data)
                 VALUES (strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'life', 'luna', ?1)",
                rusqlite::params![
                    serde_json::json!({
                        "action": "stopped",
                        "snapshot": {
                            "tag": "team"
                        }
                    })
                    .to_string()
                ],
            )
            .unwrap();

        assert_eq!(
            resolve_display_name_or_stopped(&db, "team-luna").as_deref(),
            Some("luna")
        );
        assert_eq!(
            resolve_display_name_or_stopped(&db, "luna").as_deref(),
            Some("luna")
        );
    }

    #[test]
    fn test_parse_running_tasks() {
        assert!(!parse_running_tasks(None).active);
        assert!(!parse_running_tasks(Some("")).active);
        assert!(!parse_running_tasks(Some("invalid")).active);

        let rt = parse_running_tasks(Some(r#"{"active":true,"subagents":[{"agent_id":"a1"}]}"#));
        assert!(rt.active);
        assert_eq!(rt.subagents.len(), 1);
    }

    #[test]
    fn test_bind_session_path2_placeholder() {
        crate::config::Config::init();
        let (db, path) = setup_test_db();
        let now = now_epoch_i64();

        // Create placeholder instance (no session_id)
        let mut data = serde_json::Map::new();
        data.insert("name".into(), serde_json::json!("luna"));
        data.insert("status".into(), serde_json::json!("pending"));
        data.insert("status_context".into(), serde_json::json!("new"));
        data.insert("created_at".into(), serde_json::json!(now));
        db.save_instance_named("luna", &data).unwrap();

        // Create process binding
        db.set_process_binding("pid-123", "", "luna").unwrap();

        // Bind session to process
        let result = bind_session_to_process(&db, "sid-456", Some("pid-123"));
        assert_eq!(result, Some("luna".to_string()));

        // Verify session_id was set
        let inst = db.get_instance_full("luna").unwrap().unwrap();
        assert_eq!(inst.session_id.as_deref(), Some("sid-456"));

        // Verify session binding was created
        let binding = db.get_session_binding("sid-456").unwrap();
        assert_eq!(binding, Some("luna".to_string()));

        cleanup(path);
    }

    #[test]
    fn test_bind_session_path1a_true_placeholder_merge() {
        crate::config::Config::init();
        let (db, path) = setup_test_db();
        let now = now_epoch_i64();

        // Create canonical instance (with session_id)
        let mut canonical_data = serde_json::Map::new();
        canonical_data.insert("name".into(), serde_json::json!("miso"));
        canonical_data.insert("session_id".into(), serde_json::json!("sid-789"));
        canonical_data.insert("created_at".into(), serde_json::json!(now));
        canonical_data.insert("status".into(), serde_json::json!("listening"));
        db.save_instance_named("miso", &canonical_data).unwrap();
        db.rebind_session("sid-789", "miso").unwrap();

        // Create placeholder (no session_id, has tag)
        let mut ph_data = serde_json::Map::new();
        ph_data.insert("name".into(), serde_json::json!("temp"));
        ph_data.insert("tag".into(), serde_json::json!("team"));
        ph_data.insert("created_at".into(), serde_json::json!(now));
        ph_data.insert("status".into(), serde_json::json!("pending"));
        ph_data.insert("status_context".into(), serde_json::json!("new"));
        db.save_instance_named("temp", &ph_data).unwrap();

        // Process binding points to placeholder
        db.set_process_binding("pid-123", "", "temp").unwrap();

        // Bind session (session already has canonical "miso")
        let result = bind_session_to_process(&db, "sid-789", Some("pid-123"));
        assert_eq!(result, Some("miso".to_string()));

        // Placeholder should be deleted
        assert!(db.get_instance_full("temp").unwrap().is_none());

        // Tag should be merged to canonical
        let inst = db.get_instance_full("miso").unwrap().unwrap();
        assert_eq!(inst.tag.as_deref(), Some("team"));

        cleanup(path);
    }

    #[test]
    fn test_bind_session_path1b_session_switch_marks_old_inactive() {
        crate::config::Config::init();
        let (db, path) = setup_test_db();
        let now = now_epoch_i64();

        // Canonical instance already bound to sid-789
        let mut canonical_data = serde_json::Map::new();
        canonical_data.insert("name".into(), serde_json::json!("miso"));
        canonical_data.insert("session_id".into(), serde_json::json!("sid-789"));
        canonical_data.insert("created_at".into(), serde_json::json!(now));
        canonical_data.insert("status".into(), serde_json::json!("listening"));
        db.save_instance_named("miso", &canonical_data).unwrap();
        db.rebind_session("sid-789", "miso").unwrap();

        // Placeholder already has a different session_id, so this must go through path 1b.
        let mut ph_data = serde_json::Map::new();
        ph_data.insert("name".into(), serde_json::json!("temp"));
        ph_data.insert("session_id".into(), serde_json::json!("sid-old"));
        ph_data.insert("created_at".into(), serde_json::json!(now));
        ph_data.insert("status".into(), serde_json::json!("listening"));
        db.save_instance_named("temp", &ph_data).unwrap();
        db.rebind_session("sid-old", "temp").unwrap();
        db.set_process_binding("pid-123", "sid-old", "temp")
            .unwrap();

        let result = bind_session_to_process(&db, "sid-789", Some("pid-123"));
        assert_eq!(result, Some("miso".to_string()));

        // Placeholder is retained but marked inactive for session switch.
        let placeholder = db.get_instance_full("temp").unwrap().unwrap();
        assert_eq!(placeholder.status, ST_INACTIVE);
        assert_eq!(placeholder.status_context, "exit:session_switch");

        // Old session binding is cleared from the placeholder instance.
        assert_eq!(db.get_session_binding("sid-old").unwrap(), None);

        // Process binding now points to canonical.
        assert_eq!(
            db.get_process_binding("pid-123").unwrap(),
            Some("miso".to_string())
        );

        cleanup(path);
    }

    #[test]
    fn test_bind_session_no_match() {
        crate::config::Config::init();
        let (db, path) = setup_test_db();

        // No process binding, no session binding
        let result = bind_session_to_process(&db, "sid-999", None);
        assert_eq!(result, None);

        cleanup(path);
    }

    fn default_instance() -> InstanceRow {
        InstanceRow {
            name: String::new(),
            session_id: None,
            parent_session_id: None,
            parent_name: None,
            agent_id: None,
            tag: None,
            last_event_id: 0,
            last_stop: 0,
            status: ST_INACTIVE.into(),
            status_time: 0,
            status_context: String::new(),
            status_detail: String::new(),
            directory: String::new(),
            created_at: 0.0,
            transcript_path: String::new(),
            tool: "claude".into(),
            background: 0,
            background_log_file: String::new(),
            tcp_mode: 0,
            wait_timeout: None,
            subagent_timeout: None,
            hints: None,
            origin_device_id: None,
            pid: None,
            launch_args: None,
            terminal_preset_requested: None,
            terminal_preset_effective: None,
            launch_context: None,
            name_announced: 0,
            running_tasks: None,
            idle_since: None,
        }
    }

    #[test]
    fn test_create_orphaned_pty_identity_basic() {
        crate::config::Config::init();
        let (db, path) = setup_test_db();

        let result = create_orphaned_pty_identity(&db, "sess-orphan", Some("pid-orphan"), "claude");
        assert!(result.is_some(), "should create orphaned identity");

        let name = result.unwrap();
        // Verify instance exists with correct fields
        let inst = db.get_instance_full(&name).unwrap().unwrap();
        assert_eq!(inst.session_id.as_deref(), Some("sess-orphan"));
        assert_eq!(inst.tool, "claude");

        // Verify session binding created
        assert_eq!(
            db.get_session_binding("sess-orphan").unwrap(),
            Some(name.clone())
        );

        // Verify process binding created
        assert_eq!(db.get_process_binding("pid-orphan").unwrap(), Some(name));

        cleanup(path);
    }

    #[test]
    fn test_create_orphaned_pty_identity_no_process_id() {
        crate::config::Config::init();
        let (db, path) = setup_test_db();

        let result = create_orphaned_pty_identity(&db, "sess-orphan2", None, "gemini");
        assert!(result.is_some());

        let name = result.unwrap();
        let inst = db.get_instance_full(&name).unwrap().unwrap();
        assert_eq!(inst.tool, "gemini");

        // Session binding exists, no process binding
        assert_eq!(db.get_session_binding("sess-orphan2").unwrap(), Some(name));

        cleanup(path);
    }

    #[test]
    fn test_resolve_from_binding_process_binding() {
        crate::config::Config::init();
        let (db, path) = setup_test_db();
        let now = now_epoch_i64();

        let mut data = serde_json::Map::new();
        data.insert("name".into(), serde_json::json!("luna"));
        data.insert("session_id".into(), serde_json::json!("sess-1"));
        data.insert("created_at".into(), serde_json::json!(now));
        data.insert("status".into(), serde_json::json!("listening"));
        db.save_instance_named("luna", &data).unwrap();
        db.set_process_binding("pid-1", "sess-1", "luna").unwrap();

        let result = resolve_instance_from_binding(&db, None, Some("pid-1"));
        assert!(result.is_some());
        assert_eq!(result.unwrap().name, "luna");

        cleanup(path);
    }

    #[test]
    fn test_resolve_from_binding_session_binding() {
        crate::config::Config::init();
        let (db, path) = setup_test_db();
        let now = now_epoch_i64();

        let mut data = serde_json::Map::new();
        data.insert("name".into(), serde_json::json!("nova"));
        data.insert("session_id".into(), serde_json::json!("sess-2"));
        data.insert("created_at".into(), serde_json::json!(now));
        data.insert("status".into(), serde_json::json!("active"));
        db.save_instance_named("nova", &data).unwrap();
        db.rebind_session("sess-2", "nova").unwrap();

        let result = resolve_instance_from_binding(&db, Some("sess-2"), None);
        assert!(result.is_some());
        assert_eq!(result.unwrap().name, "nova");

        cleanup(path);
    }

    #[test]
    fn test_resolve_from_binding_process_over_session() {
        // Process binding should take priority over session binding
        crate::config::Config::init();
        let (db, path) = setup_test_db();
        let now = now_epoch_i64();

        let mut d1 = serde_json::Map::new();
        d1.insert("name".into(), serde_json::json!("luna"));
        d1.insert("created_at".into(), serde_json::json!(now));
        d1.insert("status".into(), serde_json::json!("active"));
        db.save_instance_named("luna", &d1).unwrap();
        db.set_process_binding("pid-1", "", "luna").unwrap();

        let mut d2 = serde_json::Map::new();
        d2.insert("name".into(), serde_json::json!("nova"));
        d2.insert("session_id".into(), serde_json::json!("sess-2"));
        d2.insert("created_at".into(), serde_json::json!(now));
        d2.insert("status".into(), serde_json::json!("active"));
        db.save_instance_named("nova", &d2).unwrap();
        db.rebind_session("sess-2", "nova").unwrap();

        let result = resolve_instance_from_binding(&db, Some("sess-2"), Some("pid-1"));
        assert_eq!(result.unwrap().name, "luna"); // process wins

        cleanup(path);
    }

    #[test]
    fn test_resolve_from_binding_process_binding_instance_deleted() {
        // Process binding exists but instance row was deleted — should fall through
        crate::config::Config::init();
        let (db, path) = setup_test_db();

        db.set_process_binding("pid-ghost", "", "ghost").unwrap();
        // No instance "ghost" exists

        let result = resolve_instance_from_binding(&db, None, Some("pid-ghost"));
        assert!(result.is_none());

        cleanup(path);
    }

    #[test]
    fn test_resolve_from_binding_no_match() {
        crate::config::Config::init();
        let (db, path) = setup_test_db();

        let result = resolve_instance_from_binding(&db, Some("nonexistent"), Some("nope"));
        assert!(result.is_none());

        cleanup(path);
    }

    #[test]
    fn test_session_binding_cascade_on_instance_delete() {
        crate::config::Config::init();
        let (db, path) = setup_test_db();
        let now = now_epoch_i64();

        let mut data = serde_json::Map::new();
        data.insert("name".into(), serde_json::json!("luna"));
        data.insert("session_id".into(), serde_json::json!("sess-1"));
        data.insert("created_at".into(), serde_json::json!(now));
        data.insert("status".into(), serde_json::json!("active"));
        db.save_instance_named("luna", &data).unwrap();
        db.rebind_session("sess-1", "luna").unwrap();

        // Confirm binding exists
        assert_eq!(
            db.get_session_binding("sess-1").unwrap(),
            Some("luna".to_string())
        );

        // Delete instance — CASCADE should remove session binding
        db.delete_instance("luna").unwrap();
        assert_eq!(db.get_session_binding("sess-1").unwrap(), None);

        cleanup(path);
    }

    #[test]
    fn test_hamming_distance_with_many_alive() {
        // With many alive names, similarity rejection should still work
        let mut alive = HashSet::new();
        alive.insert("luna".to_string());
        alive.insert("nova".to_string());
        alive.insert("miso".to_string());
        alive.insert("kira".to_string());
        alive.insert("duma".to_string());

        // 1 char different from "luna"
        assert!(is_too_similar("lina", &alive));
        // 1 char different from "nova"
        assert!(is_too_similar("nava", &alive));
        // 3+ chars different from all
        assert!(!is_too_similar("bize", &alive));
    }

    #[test]
    fn test_hamming_distance_different_lengths() {
        // Names of different length are never considered too similar
        let mut alive = HashSet::new();
        alive.insert("luna".to_string());

        assert!(!is_too_similar("lu", &alive));
        assert!(!is_too_similar("lunaa", &alive));
        assert!(!is_too_similar("l", &alive));
    }

    #[test]
    fn test_bind_session_idempotent_same_session() {
        // Calling bind_session_to_process twice with same session+process should be idempotent
        crate::config::Config::init();
        let (db, path) = setup_test_db();
        let now = now_epoch_i64();

        let mut data = serde_json::Map::new();
        data.insert("name".into(), serde_json::json!("luna"));
        data.insert("status".into(), serde_json::json!("pending"));
        data.insert("status_context".into(), serde_json::json!("new"));
        data.insert("created_at".into(), serde_json::json!(now));
        db.save_instance_named("luna", &data).unwrap();
        db.set_process_binding("pid-1", "", "luna").unwrap();

        // First bind
        let r1 = bind_session_to_process(&db, "sess-1", Some("pid-1"));
        assert_eq!(r1, Some("luna".to_string()));

        // Second bind with same session — should still resolve to luna (now via canonical path)
        let r2 = bind_session_to_process(&db, "sess-1", Some("pid-1"));
        assert_eq!(r2, Some("luna".to_string()));

        // Instance still intact
        let inst = db.get_instance_full("luna").unwrap().unwrap();
        assert_eq!(inst.session_id.as_deref(), Some("sess-1"));

        cleanup(path);
    }

    #[test]
    fn test_auto_subscribe_creates_collision_subscription() {
        let (db, path) = setup_test_db();

        // Directly test create_filter_subscription (the core of auto_subscribe_defaults)
        use std::collections::HashMap;
        let mut filters: HashMap<String, Vec<String>> = HashMap::new();
        filters.insert("collision".to_string(), vec!["1".to_string()]);

        let result = crate::commands::events::create_filter_subscription(
            &db,
            &filters,
            &[],
            "test-agent",
            false,
            true,
        );
        assert_eq!(result, 0, "subscription creation should succeed");

        // Verify subscription was stored in kv
        let rows: Vec<String> = db
            .conn()
            .prepare("SELECT key FROM kv WHERE key LIKE 'events_sub:%'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert_eq!(rows.len(), 1, "should have 1 subscription");

        cleanup(path);
    }

    #[test]
    fn test_auto_subscribe_silent_no_stdout() {
        let (db, path) = setup_test_db();

        use std::collections::HashMap;
        let mut filters: HashMap<String, Vec<String>> = HashMap::new();
        filters.insert("action".to_string(), vec!["created".to_string()]);

        // Silent mode should not panic or produce errors
        let result = crate::commands::events::create_filter_subscription(
            &db,
            &filters,
            &[],
            "test-agent",
            false,
            true,
        );
        assert_eq!(result, 0);

        cleanup(path);
    }

    #[test]
    fn test_auto_subscribe_duplicate_is_noop() {
        let (db, path) = setup_test_db();

        use std::collections::HashMap;
        let mut filters: HashMap<String, Vec<String>> = HashMap::new();
        filters.insert("collision".to_string(), vec!["1".to_string()]);

        // First call creates
        let r1 = crate::commands::events::create_filter_subscription(
            &db,
            &filters,
            &[],
            "test-agent",
            false,
            true,
        );
        assert_eq!(r1, 0);

        // Second call with same filters is a no-op (duplicate)
        let r2 = crate::commands::events::create_filter_subscription(
            &db,
            &filters,
            &[],
            "test-agent",
            false,
            true,
        );
        assert_eq!(r2, 0);

        // Still only 1 subscription
        let count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "duplicate should not create second subscription");

        cleanup(path);
    }

    #[test]
    fn test_auto_subscribe_skips_non_tool() {
        // auto_subscribe_defaults guards on tool type — non-tools should be skipped
        let (db, path) = setup_test_db();

        auto_subscribe_defaults(&db, "test-agent", "unknown_tool");

        let count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM kv WHERE key LIKE 'events_sub:%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "non-tool should not create subscriptions");

        cleanup(path);
    }

    #[test]
    fn test_auto_subscribe_name_reuse_cleans_thread_memberships() {
        let (db, path) = setup_test_db();

        let normal = serde_json::json!({
            "id": "sub-normal",
            "caller": "test-agent",
            "sql": "type = 'message'",
            "last_id": 0
        });
        let thread_member = serde_json::json!({
            "id": "sub-thread",
            "caller": "test-agent",
            "thread_name": "debate-1",
            "auto_thread_member": true,
            "delivery_only": true,
            "created": 1000.0,
            "last_id": 0,
            "once": false
        });
        db.kv_set("events_sub:sub-normal", Some(&normal.to_string()))
            .unwrap();
        db.kv_set("events_sub:sub-thread", Some(&thread_member.to_string()))
            .unwrap();

        auto_subscribe_defaults(&db, "test-agent", "codex");

        assert!(db.kv_get("events_sub:sub-normal").unwrap().is_none());
        assert!(db.kv_get("events_sub:sub-thread").unwrap().is_none());

        cleanup(path);
    }
}
