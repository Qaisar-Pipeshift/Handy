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
