//! Incoming message handling — route state vs control topics, apply remote state.
//!
//! Handles MQTT messages received from remote devices:
//! - State messages: upsert remote instances, import events
//! - Control messages: process stop/kill commands
//! - Empty payloads: device gone (LWT or graceful cleanup)

use rusqlite::params;
use serde_json::Value;

use crate::db::HcomDb;
use crate::instance_lifecycle;
use crate::log;

use super::{device_short_id, safe_kv_get, safe_kv_set};

/// Handle device gone (empty retained payload = LWT or graceful disconnect).
/// Removes all instances belonging to the disconnected device.
pub fn handle_device_gone(db: &HcomDb, device_id: &str) {
    if let Err(e) = db.conn().execute(
        "DELETE FROM instances WHERE origin_device_id = ?",
        params![device_id],
    ) {
        log::log_error("relay", "relay.device_gone_err", &format!("{}", e));
        return;
    }
    safe_kv_set(db, &format!("relay_sync_time_{}", device_id), None);
    // Clear short_id mapping
    clear_short_id(db, device_id);
    log::log_info(
        "relay",
        "relay.device_gone",
        &format!("device={}", &device_id[..8.min(device_id.len())]),
    );
}

/// Handle a control message from the control topic.
pub fn handle_control_message(db: &HcomDb, payload: &[u8], own_device: &str) {
    let data: Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => {
            log::log_warn("relay", "relay.bad_payload", &format!("{}", e));
            return;
        }
    };

    let source_device = data
        .get("from_device")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    // Ignore own control messages
    if source_device == own_device {
        return;
    }

    let own_short_id = device_short_id(own_device);
    let events = if let Some(arr) = data.get("events").and_then(|v| v.as_array()) {
        arr.clone()
    } else if data.get("type").and_then(|v| v.as_str()) == Some("control") {
        vec![data.clone()]
    } else {
        vec![]
    };

    super::control::handle_control_events(db, &events, &own_short_id, source_device);
}

/// Handle a state message from a remote device.
pub fn handle_state_message(db: &HcomDb, device_id: &str, payload: &[u8], own_device: &str) {
    let t0 = std::time::Instant::now();

    let data: Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => {
            log::log_warn("relay", "relay.bad_payload", &format!("{}", e));
            return;
        }
    };

    let state = data
        .get("state")
        .cloned()
        .unwrap_or(Value::Object(Default::default()));
    let events = data
        .get("events")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let short_id = state
        .get("short_id")
        .and_then(|v| v.as_str())
        .unwrap_or(&device_id[..4.min(device_id.len())])
        .to_uppercase();
    let reset_ts = state
        .get("reset_ts")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    // Check short_id collision (two different devices with same short_id)
    let cached_device = safe_kv_get(db, &format!("relay_short_{}", short_id));
    if let Some(ref cached) = cached_device {
        if cached != device_id {
            log::log_warn(
                "relay",
                "relay.collision",
                &format!(
                    "short_id={} existing={} incoming={}",
                    short_id,
                    &cached[..8.min(cached.len())],
                    &device_id[..8.min(device_id.len())]
                ),
            );
            return; // Skip to prevent data corruption
        }
    } else {
        safe_kv_set(db, &format!("relay_short_{}", short_id), Some(device_id));
    }

    // Check for device reset — clean old data before importing
    let cached_reset: f64 = safe_kv_get(db, &format!("relay_reset_{}", device_id))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);

    if reset_ts > cached_reset {
        if let Err(e) = db.conn().execute(
            "DELETE FROM instances WHERE origin_device_id = ?",
            params![device_id],
        ) {
            log::log_warn(
                "relay",
                "pull.reset_instances",
                &format!("failed to delete instances for device {device_id}: {e}"),
            );
        }
        if let Err(e) = db.conn().execute(
            "DELETE FROM events WHERE json_extract(data, '$._relay.device') = ?",
            params![device_id],
        ) {
            log::log_warn(
                "relay",
                "pull.reset_events",
                &format!("failed to delete events for device {device_id}: {e}"),
            );
        }
        safe_kv_set(
            db,
            &format!("relay_reset_{}", device_id),
            Some(&reset_ts.to_string()),
        );
        safe_kv_set(db, &format!("relay_events_{}", device_id), Some("0"));
        log::log_info("relay", "relay.reset", &format!("device={}", short_id));
    }

    // Get local reset timestamp for filtering stale data.
    // Check KV first, then fall back to events table.
    let mut local_reset_ts: f64 = safe_kv_get(db, "relay_local_reset_ts")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);

    if local_reset_ts == 0.0 {
        // Fallback: query events table for last reset event
        let ts_opt = db
            .conn()
            .query_row(
                "SELECT timestamp FROM events
             WHERE type='life' AND instance='_device'
               AND json_extract(data, '$.action')='reset'
               AND json_extract(data, '$._relay') IS NULL
             ORDER BY id DESC LIMIT 1",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .ok()
            .flatten();

        if let Some(ts_str) = ts_opt {
            let ts = parse_ts(Some(&serde_json::Value::String(ts_str)));
            if ts > 0.0 {
                local_reset_ts = ts;
                // Cache in KV for future calls
                safe_kv_set(db, "relay_local_reset_ts", Some(&ts.to_string()));
            }
        }
    }

    // Upsert remote instances
    let own_short_id = device_short_id(own_device);
    let instances = state
        .get("instances")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();

    let mut seen_instances = std::collections::HashSet::new();

    for (name, inst) in &instances {
        let status_time = inst
            .get("status_time")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let status_time_i64 = status_time as i64;

        // Local reset wins: ignore remote snapshots older than our reset so
        // cleared instances don't reappear from broker-retained state.
        if local_reset_ts > 0.0 && status_time < local_reset_ts {
            continue;
        }

        let namespaced = super::add_device_suffix(name, &short_id);
        seen_instances.insert(namespaced.clone());

        let parent = inst
            .get("parent")
            .and_then(|v| v.as_str())
            .map(|p| super::add_device_suffix(p, &short_id));

        let now = crate::shared::time::now_epoch_f64();

        let _ = db.conn().execute(
            "INSERT INTO instances (
                name, origin_device_id, status, status_context, status_detail, status_time,
                parent_name, directory, transcript_path, created_at,
                session_id, parent_session_id, agent_id, wait_timeout, last_stop, tcp_mode,
                tag, tool, background
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                status = excluded.status,
                status_context = excluded.status_context, status_detail = excluded.status_detail,
                status_time = excluded.status_time,
                parent_name = excluded.parent_name,
                directory = excluded.directory, transcript_path = excluded.transcript_path,
                session_id = excluded.session_id, parent_session_id = excluded.parent_session_id,
                agent_id = excluded.agent_id, wait_timeout = excluded.wait_timeout,
                last_stop = excluded.last_stop, tcp_mode = excluded.tcp_mode,
                tag = excluded.tag, tool = excluded.tool, background = excluded.background",
            params![
                namespaced,
                device_id,
                inst.get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown"),
                inst.get("context").and_then(|v| v.as_str()).unwrap_or(""),
                inst.get("detail").and_then(|v| v.as_str()).unwrap_or(""),
                status_time_i64,
                parent,
                inst.get("directory").and_then(|v| v.as_str()),
                inst.get("transcript").and_then(|v| v.as_str()),
                now,
                Option::<String>::None,
                Option::<String>::None,
                Option::<String>::None,
                inst.get("wait_timeout")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(86400),
                inst.get("last_stop")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                inst.get("tcp_mode")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                inst.get("tag").and_then(|v| v.as_str()),
                inst.get("tool")
                    .and_then(|v| v.as_str())
                    .unwrap_or("claude"),
                inst.get("background")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
            ],
        );
    }

    // Remove stale instances (no longer in remote state)
    let current_remote: Vec<String> = db
        .conn()
        .prepare("SELECT name FROM instances WHERE origin_device_id = ?")
        .ok()
        .map(|mut stmt| {
            stmt.query_map(params![device_id], |row| row.get::<_, String>(0))
                .ok()
                .map(|rows| rows.filter_map(|r| r.ok()).collect())
                .unwrap_or_default()
        })
        .unwrap_or_default();

    for name in &current_remote {
        if !seen_instances.contains(name) {
            let _ = db
                .conn()
                .execute("DELETE FROM instances WHERE name = ?", params![name]);
        }
    }

    // Handle control events in the events payload
    super::control::handle_control_events(db, &events, &own_short_id, device_id);

    // Import remote events with dedup
    import_remote_events(
        db,
        device_id,
        &short_id,
        &events,
        local_reset_ts,
        &own_short_id,
    );

    // Update sync timestamp
    let now = crate::shared::time::now_epoch_f64();
    safe_kv_set(
        db,
        &format!("relay_sync_time_{}", device_id),
        Some(&now.to_string()),
    );

    // Update relay_device_count and relay_last_sync
    let device_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(DISTINCT origin_device_id) FROM instances \
             WHERE origin_device_id IS NOT NULL AND origin_device_id != ''",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    safe_kv_set(db, "relay_device_count", Some(&device_count.to_string()));
    safe_kv_set(db, "relay_last_sync", Some(&now.to_string()));

    let apply_ms = t0.elapsed().as_millis();
    log::log_with_fields(
        "INFO",
        "relay",
        "relay.recv",
        "",
        &[
            ("device", &short_id),
            ("events", &events.len().to_string()),
            ("instances", &instances.len().to_string()),
            ("apply_ms", &apply_ms.to_string()),
            ("payload_bytes", &payload.len().to_string()),
        ],
    );

    // Wake local TCP instances so they see new messages immediately.
    //
    instance_lifecycle::notify_all_instances(db);
}

/// Import remote events with cursor-based dedup.
fn import_remote_events(
    db: &HcomDb,
    device_id: &str,
    short_id: &str,
    events: &[Value],
    local_reset_ts: f64,
    own_short_id: &str,
) {
    let mut last_event_id: i64 = safe_kv_get(db, &format!("relay_events_{}", device_id))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Detect ID regression (remote DB recreated without proper reset event)
    if !events.is_empty() && last_event_id > 0 {
        let remote_max_id: i64 = events
            .iter()
            .filter(|e| e.get("type").and_then(|v| v.as_str()) != Some("control"))
            .filter_map(|e| e.get("id").and_then(|v| v.as_i64()))
            .max()
            .unwrap_or(0);

        if remote_max_id > 0 && remote_max_id < last_event_id {
            // Cursor regression: remote DB was recreated/reset. Drop cached
            // state and reimport from zero, otherwise the stale cursor would
            // skip the entire new history.
            log::log_info(
                "relay",
                "relay.reset",
                &format!(
                    "device={} reason=id_regression:{}<{}",
                    short_id, remote_max_id, last_event_id
                ),
            );
            let _ = db.conn().execute(
                "DELETE FROM instances WHERE origin_device_id = ?",
                params![device_id],
            );
            let _ = db.conn().execute(
                "DELETE FROM events WHERE json_extract(data, '$._relay.device') = ?",
                params![device_id],
            );
            last_event_id = 0;
            safe_kv_set(db, &format!("relay_events_{}", device_id), Some("0"));
        }
    }

    let mut max_event_id = last_event_id;

    for event in events {
        // Skip control events (handled separately)
        if event.get("type").and_then(|v| v.as_str()) == Some("control") {
            continue;
        }
        // Skip _device events
        if event.get("instance").and_then(|v| v.as_str()) == Some("_device") {
            continue;
        }

        let event_id = match event.get("id").and_then(|v| v.as_i64()) {
            Some(id) => id,
            None => {
                log::log_warn(
                    "relay",
                    "relay.bad_event_id",
                    &format!("Skipping event with bad/missing id: {:?}", event.get("id")),
                );
                continue;
            }
        };
        if event_id <= last_event_id {
            continue; // Already imported
        }

        // Skip events from before our reset
        let event_ts = parse_ts(event.get("ts"));
        if local_reset_ts > 0.0 && event_ts > 0.0 && event_ts < local_reset_ts {
            continue;
        }

        // Namespace instance name
        let instance = event.get("instance").and_then(|v| v.as_str()).unwrap_or("");
        let namespaced_instance =
            if !instance.is_empty() && !instance.contains(':') && !instance.starts_with('_') {
                super::add_device_suffix(instance, short_id)
            } else {
                instance.to_string()
            };

        // Clone and namespace data fields
        let mut data = event
            .get("data")
            .cloned()
            .unwrap_or(Value::Object(Default::default()));

        // Namespace asymmetry by design:
        // - `instance` / `from` keep the remote short_id suffix -> globally unique history
        // - `mentions` / `delivered_to` strip *our own* suffix -> local delivery still matches
        if let Some(obj) = data.as_object_mut() {
            // Namespace 'from' field
            if let Some(from) = obj.get("from").and_then(|v| v.as_str()).map(String::from) {
                if !from.contains(':') {
                    obj.insert(
                        "from".to_string(),
                        Value::String(super::add_device_suffix(&from, short_id)),
                    );
                }
            }

            // Strip own device suffix from mentions and delivered_to
            for field in &["mentions", "delivered_to"] {
                if let Some(arr) = obj.get(*field).and_then(|v| v.as_array()).cloned() {
                    let fixed: Vec<Value> = arr
                        .iter()
                        .filter_map(|v| v.as_str())
                        .map(|name| Value::String(strip_device_suffix(name, own_short_id)))
                        .collect();
                    obj.insert(field.to_string(), Value::Array(fixed));
                }
            }

            // Store relay origin
            obj.insert(
                "_relay".to_string(),
                serde_json::json!({
                    "device": device_id,
                    "short": short_id,
                    "id": event_id,
                }),
            );
        }

        // Insert event
        let ts_str = match event.get("ts") {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Number(n)) => n.to_string(),
            _ => String::new(),
        };
        let event_type = event
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let _ = db.log_event_with_ts(event_type, &namespaced_instance, &data, Some(&ts_str));

        // Log per-message latency for message events
        if event_type == "message" && event_ts > 0.0 {
            let now = crate::shared::time::now_epoch_f64();
            let latency_ms = ((now - event_ts) * 1000.0) as i64;
            log::log_with_fields(
                "INFO",
                "relay",
                "relay.msg_recv",
                "",
                &[
                    ("device", short_id),
                    ("instance", &namespaced_instance),
                    ("latency_ms", &latency_ms.to_string()),
                ],
            );
        }

        max_event_id = max_event_id.max(event_id);
    }

    if max_event_id > last_event_id {
        safe_kv_set(
            db,
            &format!("relay_events_{}", device_id),
            Some(&max_event_id.to_string()),
        );
    }
}

/// Strip own device suffix from a name (case-insensitive).
/// e.g. "nuvi:RIVA" with own_short_id="RIVA" → "nuvi"
fn strip_device_suffix(name: &str, own_short_id: &str) -> String {
    let suffix = format!(":{}", own_short_id);
    if name.len() > suffix.len() && name[name.len() - suffix.len()..].eq_ignore_ascii_case(&suffix)
    {
        name[..name.len() - suffix.len()].to_string()
    } else {
        name.to_string()
    }
}

/// Parse timestamp (float or ISO string) to f64 epoch seconds.
fn parse_ts(value: Option<&Value>) -> f64 {
    match value {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
        Some(Value::String(s)) => chrono::DateTime::parse_from_rfc3339(s)
            .or_else(|_| chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ"))
            .ok()
            .map(|dt| dt.timestamp() as f64)
            .unwrap_or(0.0),
        _ => 0.0,
    }
}

/// Clear short_id mapping for a device (reverse lookup by value).
fn clear_short_id(db: &HcomDb, device_id: &str) {
    if let Ok(entries) = db.kv_prefix("relay_short_") {
        for (key, val) in entries {
            if val == device_id {
                safe_kv_set(db, &key, None);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::test_helpers::isolated_test_env;
    use serde_json::json;

    #[test]
    fn test_handle_state_message_drops_remote_unique_identity_fields() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();

        let payload = json!({
            "state": {
                "short_id": "ABCD",
                "reset_ts": 0.0,
                "instances": {
                    "orla": {
                        "status": "active",
                        "context": "",
                        "detail": "",
                        "status_time": crate::shared::time::now_epoch_f64(),
                        "parent": serde_json::Value::Null,
                        "directory": "/tmp/demo-parent",
                        "transcript": "/tmp/demo-parent/transcript.jsonl",
                        "wait_timeout": 42,
                        "last_stop": 0.0,
                        "tcp_mode": false,
                        "tag": "demo",
                        "tool": "codex",
                        "background": false
                    },
                    "luna": {
                        "status": "active",
                        "context": "",
                        "detail": "",
                        "status_time": crate::shared::time::now_epoch_f64(),
                        "parent": "orla",
                        "directory": "/tmp/demo",
                        "transcript": "/tmp/demo/transcript.jsonl",
                        "wait_timeout": 42,
                        "last_stop": 0.0,
                        "tcp_mode": false,
                        "tag": "demo",
                        "tool": "codex",
                        "background": false
                    }
                }
            },
            "events": []
        });

        handle_state_message(
            &db,
            "device-1234",
            &serde_json::to_vec(&payload).unwrap(),
            "own-device-5678",
        );

        let row = db
            .get_instance_full("luna:ABCD")
            .unwrap()
            .expect("remote row");
        assert_eq!(row.parent_name.as_deref(), Some("orla:ABCD"));
        assert_eq!(row.session_id, None);
        assert_eq!(row.parent_session_id, None);
        assert_eq!(row.agent_id, None);
        assert_eq!(row.tool, "codex");
    }
}
