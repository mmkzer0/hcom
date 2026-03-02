//! Push loop — build state snapshot and events, publish via MQTT.
//!
//! Batches up to 100 events per publish with a 10s drain budget.
//! Tracks progress via KV cursor `relay_last_push_id`.

use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::Client;
use serde_json::{json, Value};
use std::time::Instant;

use crate::db::HcomDb;
use crate::log;

use super::{device_short_id, safe_kv_get, safe_kv_set, set_relay_status, state_topic};

/// Build current instance state snapshot for publishing.
/// Only includes local instances (no origin_device_id).
pub fn build_state(db: &HcomDb) -> Value {
    let device_uuid = super::read_device_uuid();
    let short_id = device_short_id(&device_uuid);

    let instances = match db.conn().prepare(
        "SELECT name, status, status_context, status_detail, status_time, parent_name,
                session_id, parent_session_id, agent_id, directory, transcript_path,
                wait_timeout, last_stop, tcp_mode, tag, tool, background
         FROM instances WHERE COALESCE(origin_device_id, '') = ''",
    ) {
        Ok(mut stmt) => {
            let rows: Vec<_> = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,           // name
                        row.get::<_, Option<String>>(1)?,   // status
                        row.get::<_, Option<String>>(2)?,   // status_context
                        row.get::<_, Option<String>>(3)?,   // status_detail
                        row.get::<_, Option<f64>>(4)?,      // status_time
                        row.get::<_, Option<String>>(5)?,   // parent_name
                        row.get::<_, Option<String>>(6)?,   // session_id
                        row.get::<_, Option<String>>(7)?,   // parent_session_id
                        row.get::<_, Option<String>>(8)?,   // agent_id
                        row.get::<_, Option<String>>(9)?,   // directory
                        row.get::<_, Option<String>>(10)?,  // transcript_path
                        row.get::<_, Option<i64>>(11)?,     // wait_timeout
                        row.get::<_, Option<f64>>(12)?,     // last_stop
                        row.get::<_, Option<bool>>(13)?,    // tcp_mode
                        row.get::<_, Option<String>>(14)?,  // tag
                        row.get::<_, Option<String>>(15)?,  // tool
                        row.get::<_, Option<bool>>(16)?,    // background
                    ))
                })
                .ok()
                .map(|rows| rows.filter_map(|r| r.ok()).collect())
                .unwrap_or_default();

            let mut map = serde_json::Map::new();
            for row in rows {
                let name = &row.0;
                // Skip internal instances
                if name.starts_with('_') || name.starts_with("sys_") {
                    continue;
                }
                map.insert(
                    name.clone(),
                    json!({
                        "enabled": true,
                        "status": row.1.as_deref().unwrap_or("unknown"),
                        "context": row.2.as_deref().unwrap_or(""),
                        "status_time": row.4.unwrap_or(0.0),
                        "parent": row.5,
                        "session_id": row.6,
                        "parent_session_id": row.7,
                        "agent_id": row.8,
                        "directory": row.9,
                        "transcript": row.10,
                        "wait_timeout": row.11.unwrap_or(86400),
                        "last_stop": row.12.unwrap_or(0.0),
                        "tcp_mode": row.13.unwrap_or(false),
                        "tag": row.14,
                        "tool": row.15.as_deref().unwrap_or("claude"),
                        "background": row.16.unwrap_or(false),
                        "detail": row.3.as_deref().unwrap_or(""),
                    }),
                );
            }
            Value::Object(map)
        }
        Err(_) => json!({}),
    };

    // Get reset timestamp (local only — exclude imported events)
    let reset_ts = db
        .conn()
        .query_row(
            "SELECT timestamp FROM events
             WHERE type = 'life' AND instance = '_device'
             AND json_extract(data, '$.action') = 'reset'
             AND json_extract(data, '$._relay') IS NULL
             ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .ok()
        .flatten()
        .and_then(|ts| parse_iso_timestamp_to_epoch(&ts))
        .unwrap_or(0.0);

    json!({
        "instances": instances,
        "short_id": short_id,
        "reset_ts": reset_ts,
    })
}

/// Build push payload: state + events, returning (state, events, max_event_id, has_more).
/// Fetches 101 rows, sends first 100 — has_more=true if 101st exists.
pub fn build_push_payload(db: &HcomDb) -> (Value, Vec<Value>, i64, bool) {
    let state = build_state(db);

    let last_push_id: i64 = safe_kv_get(db, "relay_last_push_id")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let rows: Vec<(i64, String, String, String, String)> = db
        .conn()
        .prepare(
            "SELECT id, timestamp, type, instance, data FROM events
             WHERE id > ? AND instance NOT LIKE '%:%'
             AND instance != '_device'
             AND json_extract(data, '$._relay') IS NULL
             ORDER BY id LIMIT 101",
        )
        .ok()
        .map(|mut stmt| {
            stmt.query_map(rusqlite::params![last_push_id], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .ok()
            .map(|rows| rows.filter_map(|r| r.ok()).collect())
            .unwrap_or_default()
        })
        .unwrap_or_default();

    let has_more = rows.len() > 100;
    let send_rows = &rows[..rows.len().min(100)];

    let mut events = Vec::new();
    let mut max_id = last_push_id;

    for (id, ts, event_type, instance, data_str) in send_rows {
        let data: Value = serde_json::from_str(data_str).unwrap_or(json!({}));
        events.push(json!({
            "id": id,
            "ts": ts,
            "type": event_type,
            "instance": instance,
            "data": data,
        }));
        max_id = max_id.max(*id);
    }

    (state, events, max_id, has_more)
}

/// Push state and events via MQTT. Returns (success, has_more).
/// `is_worker` should be true when called from the daemon relay thread.
pub fn push(
    db: &HcomDb,
    client: &Client,
    relay_id: &str,
    device_uuid: &str,
    is_worker: bool,
) -> Result<(bool, bool), String> {
    let (state, events, max_id, has_more) = build_push_payload(db);

    let payload = json!({
        "state": state,
        "events": events,
    });
    let payload_bytes = serde_json::to_vec(&payload).map_err(|e| format!("json: {}", e))?;
    let payload_len = payload_bytes.len();

    let topic = state_topic(relay_id, device_uuid);
    let t0 = Instant::now();

    // Blocking publish — waits for rumqttc's internal channel to accept the message.
    // rumqttc handles retransmission via QoS::AtLeastOnce, so if the broker eventually
    // acks, the message is delivered. If the process crashes before PUBACK, the cursor
    // stays at the old position and events are re-sent on next push cycle.
    // Note: rumqttc's Client API
    // doesn't expose per-message PUBACK tracking, so we advance the cursor after
    // successful enqueue and rely on rumqttc's QoS retransmission for reliability.
    client
        .publish(&topic, QoS::AtLeastOnce, true, payload_bytes)
        .map_err(|e| format!("publish: {}", e))?;

    let publish_ms = t0.elapsed().as_millis();

    // Advance cursor after publish enqueued (rumqttc QoS retransmission handles reliability)
    let now = crate::shared::constants::now_epoch_f64();
    safe_kv_set(db, "relay_last_push", Some(&now.to_string()));
    safe_kv_set(db, "relay_last_push_id", Some(&max_id.to_string()));
    safe_kv_set(db, "relay_last_sync", Some(&now.to_string()));
    set_relay_status(db, "ok", None, is_worker);

    log::log_with_fields(
        "INFO",
        "relay",
        "relay.push",
        "",
        &[
            ("events", &events.len().to_string()),
            ("publish_ms", &publish_ms.to_string()),
            ("payload_bytes", &payload_len.to_string()),
        ],
    );

    Ok((true, has_more))
}

/// Parse ISO 8601 timestamp to Unix epoch seconds.
fn parse_iso_timestamp_to_epoch(ts: &str) -> Option<f64> {
    chrono::DateTime::parse_from_rfc3339(ts)
        .or_else(|_| chrono::DateTime::parse_from_str(ts, "%Y-%m-%dT%H:%M:%SZ"))
        .ok()
        .map(|dt| dt.timestamp() as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_iso_timestamp_to_epoch() {
        // RFC 3339
        let ts = parse_iso_timestamp_to_epoch("2024-01-01T00:00:00+00:00");
        assert!(ts.is_some());
        assert!(ts.unwrap() > 0.0);

        // Simple ISO format
        let ts = parse_iso_timestamp_to_epoch("2024-01-01T00:00:00Z");
        assert!(ts.is_some());

        // Invalid
        assert!(parse_iso_timestamp_to_epoch("not a date").is_none());
    }
}
