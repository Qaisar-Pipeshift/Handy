//! Fork addition: transcribe on a self-hosted dictation server instead of a local model.
//!
//! Speaks the AssemblyAI-Dictation-compatible route the server exposes: `POST {base}/v1/transcribe/live` with a
//! multipart body — a JSON `config` part, then the recording as raw S16LE PCM. `"llm": null` asks for the raw
//! transcript only, so Handy's own post-processing (the `transcribe_with_post_process` shortcut) stays the one place
//! text gets rewritten. Enabled by `remote_enabled` + `remote_base_url` in the settings store; when on, no local
//! model is loaded (see `actions.rs`).

use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use serde::Deserialize;

use crate::settings::AppSettings;

/// Handy's recorder delivers 16 kHz mono f32, the same input `TranscriptionManager::transcribe` takes.
const SAMPLE_RATE: u32 = 16_000;
/// The server's cap on key terms (AssemblyAI's documented `keyterms_prompt` limit).
const MAX_KEYTERMS: usize = 100;

#[derive(Deserialize)]
struct DictationResponse {
    text: String,
}

/// One client for the process, so the connection opened by `warm` while the user speaks is reused on release.
fn client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(90))
            .pool_idle_timeout(Duration::from_secs(60))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    })
}

pub fn enabled(settings: &AppSettings) -> bool {
    settings.remote_enabled && !settings.remote_base_url.trim().is_empty()
}

fn base_url(settings: &AppSettings) -> &str {
    settings.remote_base_url.trim().trim_end_matches('/')
}

/// Opens the connection while the user is still speaking, so the TLS handshake is off the release path.
/// `/warm` is an unauthenticated no-op on the server; failures are ignored (the real request reconnects).
pub async fn warm(settings: AppSettings) {
    if enabled(&settings) {
        let _ = client().get(format!("{}/warm", base_url(&settings))).send().await;
    }
}

/// Sends the whole recording and returns the server's raw transcript ("" when the server heard no speech).
pub async fn transcribe(settings: &AppSettings, samples: &[f32]) -> Result<String> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let boundary = format!("handy-remote-{nanos:x}");
    let keyterms: Vec<&str> = settings
        .custom_words
        .iter()
        .map(|w| w.trim())
        .filter(|w| !w.is_empty())
        .take(MAX_KEYTERMS)
        .collect();
    let config = serde_json::json!({
        "sample_rate": SAMPLE_RATE,
        "channels": 1,
        "stt_prompt": "",
        "keyterms_prompt": keyterms,
        "llm": null,
    });

    let mut body = Vec::with_capacity(samples.len() * 2 + 1024);
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"config\"\r\n\
             Content-Type: application/json\r\n\r\n{config}\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"audio\"; filename=\"audio.pcm\"\r\n\
             Content-Type: audio/pcm\r\n\r\n"
        )
        .as_bytes(),
    );
    for sample in samples {
        body.extend_from_slice(&((sample.clamp(-1.0, 1.0) * 32767.0) as i16).to_le_bytes());
    }
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

    let response = client()
        .post(format!("{}/v1/transcribe/live", base_url(settings)))
        .header(reqwest::header::AUTHORIZATION, settings.remote_api_key.expose())
        .header(
            reqwest::header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await
        .map_err(|e| anyhow!("remote transcription request failed: {e}"))?;
    let status = response.status();
    if !status.is_success() {
        let detail: String = response
            .text()
            .await
            .unwrap_or_default()
            .chars()
            .take(200)
            .collect();
        return Err(anyhow!("remote transcription failed: HTTP {status}: {detail}"));
    }
    let parsed: DictationResponse = response
        .json()
        .await
        .map_err(|e| anyhow!("remote transcription returned an unreadable response: {e}"))?;
    Ok(parsed.text)
}

// ---- Live streaming (E-09) ----------------------------------------------------------------------------------------
//
// `wss://{base}/v1/realtime?intent=transcription` speaks OpenAI's Realtime transcription events. While the key is held
// the recorder's 16 kHz frames go up in ~100 ms `input_audio_buffer.append` events; the server answers with
// `transcription.preview` {committed, tentative} for the overlay. On release `input_audio_buffer.commit` returns
// `...transcription.completed` with the final text (offline-quality per pause on the server). Any failure returns
// an error, and the caller falls back to the one-shot upload above, so a take is never lost.

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

/// Samples per `append` event: 100 ms at 16 kHz.
const APPEND_SAMPLES: usize = 1_600;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Release → final text. Kept well under Handy's 30 s finalize timeout so the upload fallback still has time.
pub const LIVE_FINAL_TIMEOUT: Duration = Duration::from_secs(12);

pub enum LiveAudio {
    Frame(Vec<f32>),
    Commit,
    Cancel,
}

pub fn streaming_enabled(settings: &AppSettings) -> bool {
    enabled(settings) && settings.remote_streaming
}

fn live_url(settings: &AppSettings) -> Result<String> {
    let base = base_url(settings);
    let rest = base
        .strip_prefix("https://")
        .map(|r| format!("wss://{r}"))
        .or_else(|| base.strip_prefix("http://").map(|r| format!("ws://{r}")))
        .ok_or_else(|| anyhow!("remote_base_url must start with http:// or https://"))?;
    Ok(format!("{rest}/v1/realtime?intent=transcription"))
}

fn pcm16_b64(samples: &[f32]) -> String {
    use base64::Engine as _;
    let mut bytes = Vec::with_capacity(samples.len() * 2);
    for s in samples {
        bytes.extend_from_slice(&((s.clamp(-1.0, 1.0) * 32767.0) as i16).to_le_bytes());
    }
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// One live dictation: streams `audio` until `Commit`, returns the server's final raw transcript ("" = no speech).
/// `on_preview(committed, tentative)` is called for every live update.
pub async fn live_session<F>(settings: AppSettings, mut audio: UnboundedReceiver<LiveAudio>, on_preview: F) -> Result<String>
where
    F: Fn(&str, &str) + Send + 'static,
{
    let mut request = live_url(&settings)?
        .into_client_request()
        .map_err(|e| anyhow!("bad live URL: {e}"))?;
    let key = HeaderValue::from_str(settings.remote_api_key.expose())
        .map_err(|_| anyhow!("remote_api_key is not a valid header value"))?;
    request.headers_mut().insert("authorization", key);
    let (socket, _) = tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(request))
        .await
        .map_err(|_| anyhow!("live connection timed out"))?
        .map_err(|e| anyhow!("live connection failed: {e}"))?;
    let (mut tx, mut rx) = socket.split();

    let keyterms: Vec<&str> = settings
        .custom_words
        .iter()
        .map(|w| w.trim())
        .filter(|w| !w.is_empty())
        .take(MAX_KEYTERMS)
        .collect();
    let update = serde_json::json!({
        "type": "session.update",
        "session": {"type": "transcription", "audio": {"input": {
            "format": {"type": "audio/pcm", "rate": SAMPLE_RATE},
            "transcription": {"model": "qwen3-asr", "prompt": keyterms.join(", ")},
            "turn_detection": null,
        }}},
    });
    tx.send(Message::Text(update.to_string().into()))
        .await
        .map_err(|e| anyhow!("live send failed: {e}"))?;

    let mut pending: Vec<f32> = Vec::with_capacity(APPEND_SAMPLES * 2);
    let mut committed = false;
    loop {
        tokio::select! {
            cmd = audio.recv(), if !committed => match cmd {
                Some(LiveAudio::Frame(frame)) => {
                    pending.extend_from_slice(&frame);
                    if pending.len() >= APPEND_SAMPLES {
                        let event = serde_json::json!({"type": "input_audio_buffer.append", "audio": pcm16_b64(&pending)});
                        pending.clear();
                        tx.send(Message::Text(event.to_string().into()))
                            .await
                            .map_err(|e| anyhow!("live send failed: {e}"))?;
                    }
                }
                Some(LiveAudio::Commit) => {
                    if !pending.is_empty() {
                        let event = serde_json::json!({"type": "input_audio_buffer.append", "audio": pcm16_b64(&pending)});
                        pending.clear();
                        tx.send(Message::Text(event.to_string().into()))
                            .await
                            .map_err(|e| anyhow!("live send failed: {e}"))?;
                    }
                    tx.send(Message::Text(r#"{"type":"input_audio_buffer.commit"}"#.into()))
                        .await
                        .map_err(|e| anyhow!("live send failed: {e}"))?;
                    committed = true;
                }
                Some(LiveAudio::Cancel) | None => {
                    let _ = tx.send(Message::Close(None)).await;
                    return Err(anyhow!("live dictation cancelled"));
                }
            },
            msg = rx.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let event: serde_json::Value =
                        serde_json::from_str(text.as_str()).unwrap_or(serde_json::Value::Null);
                    let kind = event["type"].as_str().unwrap_or_default();
                    if kind == "transcription.preview" {
                        on_preview(
                            event["committed"].as_str().unwrap_or_default(),
                            event["tentative"].as_str().unwrap_or_default(),
                        );
                    } else if kind.ends_with("input_audio_transcription.completed") {
                        let _ = tx.send(Message::Close(None)).await;
                        return Ok(event["transcript"].as_str().unwrap_or_default().to_string());
                    } else if kind == "error" {
                        let message = event["error"]["message"].as_str().unwrap_or("server error").to_string();
                        // Fewer than 100 ms of audio: the user said nothing.
                        if committed && message.contains("buffer too small") {
                            let _ = tx.send(Message::Close(None)).await;
                            return Ok(String::new());
                        }
                        return Err(anyhow!("live server error: {message}"));
                    }
                }
                Some(Ok(Message::Close(_))) | None => return Err(anyhow!("live connection closed")),
                Some(Err(e)) => return Err(anyhow!("live connection error: {e}")),
                Some(Ok(_)) => {}
            },
        }
    }
}

// ---- Settings commands (fork): Settings → Advanced → "Remote server" ------------------------------------------------
// Same store the app reads at every recording, so a change applies from the next recording; the tray menu is rebuilt
// so its "Live dictation" item matches.

#[tauri::command]
#[specta::specta]
pub fn change_remote_enabled_setting(app: tauri::AppHandle, enabled: bool) -> std::result::Result<(), String> {
    let mut settings = crate::settings::get_settings(&app);
    settings.remote_enabled = enabled;
    crate::settings::write_settings(&app, settings);
    crate::tray::update_tray_menu(&app);
    Ok(())
}

#[tauri::command]
#[specta::specta]
pub fn change_remote_base_url_setting(app: tauri::AppHandle, url: String) -> std::result::Result<(), String> {
    let url = url.trim().trim_end_matches('/').to_string();
    if !url.is_empty() && !(url.starts_with("https://") || url.starts_with("http://")) {
        return Err("The server URL must start with https:// or http://".to_string());
    }
    let mut settings = crate::settings::get_settings(&app);
    settings.remote_base_url = url;
    crate::settings::write_settings(&app, settings);
    crate::tray::update_tray_menu(&app);
    Ok(())
}

#[tauri::command]
#[specta::specta]
pub fn change_remote_api_key_setting(app: tauri::AppHandle, key: String) -> std::result::Result<(), String> {
    let mut settings = crate::settings::get_settings(&app);
    settings.remote_api_key = crate::settings::RemoteApiKey::new(key.trim().to_string());
    crate::settings::write_settings(&app, settings);
    Ok(())
}

#[tauri::command]
#[specta::specta]
pub fn change_remote_streaming_setting(app: tauri::AppHandle, enabled: bool) -> std::result::Result<(), String> {
    let mut settings = crate::settings::get_settings(&app);
    settings.remote_streaming = enabled;
    crate::settings::write_settings(&app, settings);
    crate::tray::update_tray_menu(&app);
    Ok(())
}

/// Result of "Test connection" in the Remote settings page; the frontend turns `status` into a translated message.
#[derive(Clone, Debug, serde::Serialize, specta::Type)]
pub struct RemoteCheck {
    pub status: RemoteCheckStatus,
    pub latency_ms: u32,
    /// Raw detail for the tooltip (HTTP status or error text); never contains the key.
    pub detail: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, specta::Type)]
#[serde(rename_all = "snake_case")]
pub enum RemoteCheckStatus {
    Ok,
    NoUrl,
    Unauthorized,
    HttpError,
    Unreachable,
}

/// Reaches the server with the saved URL and key (the key-check route answers 200 for a valid key, 401 otherwise).
#[tauri::command]
#[specta::specta]
pub async fn test_remote_connection(app: tauri::AppHandle) -> std::result::Result<RemoteCheck, String> {
    let settings = crate::settings::get_settings(&app);
    if settings.remote_base_url.trim().is_empty() {
        return Ok(RemoteCheck { status: RemoteCheckStatus::NoUrl, latency_ms: 0, detail: String::new() });
    }
    let started = std::time::Instant::now();
    let response = client()
        .get(format!("{}/v2/transcript", base_url(&settings)))
        .header(reqwest::header::AUTHORIZATION, settings.remote_api_key.expose())
        .timeout(Duration::from_secs(10))
        .send()
        .await;
    let latency_ms = started.elapsed().as_millis().min(u32::MAX as u128) as u32;
    let (status, detail) = match response {
        Ok(r) if r.status().is_success() => (RemoteCheckStatus::Ok, r.status().to_string()),
        Ok(r) if r.status() == reqwest::StatusCode::UNAUTHORIZED => {
            (RemoteCheckStatus::Unauthorized, r.status().to_string())
        }
        Ok(r) => (RemoteCheckStatus::HttpError, r.status().to_string()),
        Err(e) => (RemoteCheckStatus::Unreachable, e.to_string()),
    };
    Ok(RemoteCheck { status, latency_ms, detail })
}

// ---- Speculative cleanup (fork, opt-in: remote_speculative_cleanup) -------------------------------------------------
//
// With the post-process shortcut and live dictation, every time the speaker pauses with all text final (the server's
// preview has no tentative part), Handy runs its own post-processing call on the whole text so far, in the
// background — the same provider, prompt and model as the normal path. On release, if the final text equals what
// was cleaned, that result is used at once; otherwise the normal post-processing runs. Quality is identical by
// construction (always the full transcript); only the timing changes.

struct Speculation {
    input: String,
    handle: tauri::async_runtime::JoinHandle<Option<String>>,
}

static SPEC_ARMED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static SPECULATION: std::sync::Mutex<Option<Speculation>> = std::sync::Mutex::new(None);

/// Called at recording start: arm for this dictation (post-process shortcut + live + opt-in) and drop any leftover.
pub fn arm_speculation(on: bool) {
    SPEC_ARMED.store(on, std::sync::atomic::Ordering::Release);
    if let Some(old) = SPECULATION.lock().unwrap_or_else(|e| e.into_inner()).take() {
        old.handle.abort();
    }
}

/// Called for a live preview whose text is all final: clean the whole text so far in the background.
pub fn speculate(app: &tauri::AppHandle, committed_raw: &str) {
    if !SPEC_ARMED.load(std::sync::atomic::Ordering::Acquire) {
        return;
    }
    let settings = crate::settings::get_settings(app);
    let input = crate::managers::transcription::finish_remote_text(committed_raw.to_string(), &settings);
    if input.trim().is_empty() {
        return;
    }
    let mut slot = SPECULATION.lock().unwrap_or_else(|e| e.into_inner());
    if slot.as_ref().is_some_and(|s| s.input == input) {
        return;
    }
    if let Some(stale) = slot.take() {
        stale.handle.abort();
    }
    let text = input.clone();
    let handle = tauri::async_runtime::spawn(async move {
        crate::actions::post_process_transcription(&settings, &text).await
    });
    *slot = Some(Speculation { input, handle });
}

/// At release: the cleaned text if the last speculation ran on exactly `final_text` (waiting for it if it is still
/// running — it is the same request the normal path would make), else None and the normal path runs.
pub async fn take_speculation(final_text: &str) -> Option<String> {
    SPEC_ARMED.store(false, std::sync::atomic::Ordering::Release);
    let spec = SPECULATION.lock().unwrap_or_else(|e| e.into_inner()).take()?;
    if spec.input != final_text {
        spec.handle.abort();
        return None;
    }
    match tokio::time::timeout(Duration::from_secs(20), spec.handle).await {
        Ok(Ok(cleaned)) => cleaned,
        _ => None,
    }
}

#[tauri::command]
#[specta::specta]
pub fn change_remote_speculative_cleanup_setting(
    app: tauri::AppHandle,
    enabled: bool,
) -> std::result::Result<(), String> {
    let mut settings = crate::settings::get_settings(&app);
    settings.remote_speculative_cleanup = enabled;
    crate::settings::write_settings(&app, settings);
    Ok(())
}
