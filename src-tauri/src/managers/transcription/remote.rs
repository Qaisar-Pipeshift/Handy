//! Fork: the self-hosted dictation server (`crate::remote_transcription`) as the transcription
//! engine. A recording's live decode streams to the server instead of a local model, and a
//! recording with no live result is uploaded in one shot; either way the server's raw text
//! finishes in `complete`, like a local model's, so the text clean-up that runs after any engine
//! applies to it too.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Instant;

use anyhow::Result;
use log::{info, warn};
use transcribe_cpp::RunOptions;

use super::{
    echoes_vocabulary, emit_stream_text, RunOutcome, RunSetup, StreamCmd, Transcription,
    TranscriptionManager,
};
use crate::language_detection::{LanguageDecision, LanguageSource};
use crate::remote_transcription::{live_session, LiveAudio, LIVE_FINAL_TIMEOUT};
use crate::settings::{get_settings, AppSettings};

impl RunOutcome {
    /// A transcript from the remote server. The server was given the custom words as key terms, so
    /// they count as prompted (no fuzzy correction on top), and a reply that only reads them back
    /// is not speech (#2506). The server decides the language itself.
    fn remote(mut text: String, settings: &AppSettings) -> Self {
        if echoes_vocabulary(&text, &settings.custom_words) {
            info!("The server returned the custom words, not speech; dropping them");
            text.clear();
        }
        Self {
            setup: RunSetup {
                run_options: RunOptions {
                    vocabulary: settings.custom_words.clone(),
                    ..Default::default()
                },
                language: LanguageDecision {
                    language: None,
                    source: LanguageSource::Model,
                },
                languages: Vec::new(),
                applied_language_hint: None,
                output_was_translated: false,
            },
            text,
            model_detected_language: None,
        }
    }
}

impl TranscriptionManager {
    /// Transcribe a finished recording on the remote server: what its live decode produced, or else
    /// the recording uploaded in one shot. The remote counterpart of `transcribe_recording`; async
    /// because the upload is a request.
    pub async fn transcribe_remote_recording(&self, samples: Vec<f32>) -> Result<Transcription> {
        let started = Instant::now();
        // Finalizing waits for the live decode's reply, so it runs off the async runtime.
        let manager = self.clone();
        let recorded_samples = samples.len();
        let live = tauri::async_runtime::spawn_blocking(move || {
            manager.finalize_live(recorded_samples)
        })
        .await??;
        let outcome = match live {
            Some(outcome) => outcome,
            None => {
                let settings = get_settings(&self.app_handle);
                let text = crate::remote_transcription::transcribe(&settings, &samples).await?;
                RunOutcome::remote(text, &settings)
            }
        };
        self.complete(started, samples, Some(outcome))
    }

    /// A recording's live decode on the remote server (`remote_transcription::live_session`):
    /// frames go up as they arrive and the overlay shows the server's live text. Finalize replies
    /// with the server's final text, including "" when it heard no speech (uploading the same
    /// audio again would only repeat that), or with no result on any failure, so the recording is
    /// uploaded in one shot instead and a take is never lost.
    pub(super) fn run_remote_stream(
        &self,
        rx: mpsc::Receiver<StreamCmd>,
        cancelled: Arc<AtomicBool>,
        settings: AppSettings,
    ) {
        let (audio_tx, audio_rx) = tokio::sync::mpsc::unbounded_channel();
        let (done_tx, done_rx) = mpsc::channel::<Result<String>>();
        let app = self.app_handle.clone();
        let session = tauri::async_runtime::spawn(async move {
            let result = live_session(settings, audio_rx, move |committed: &str, tentative: &str| {
                emit_stream_text(&app, committed, tentative);
            })
            .await;
            let _ = done_tx.send(result);
        });
        self.stream_active.store(true, Ordering::Release);
        info!("Live remote transcription started");

        while let Ok(cmd) = rx.recv() {
            match cmd {
                StreamCmd::Feed(pcm) => {
                    // Audio queued ahead of a cancel is dropped, not sent.
                    if cancelled.load(Ordering::Acquire) {
                        continue;
                    }
                    self.touch_activity();
                    let _ = audio_tx.send(LiveAudio::Frame(pcm));
                }
                StreamCmd::Finalize { reply, .. } => {
                    let _ = audio_tx.send(LiveAudio::Commit);
                    let result = match done_rx.recv_timeout(LIVE_FINAL_TIMEOUT) {
                        Ok(Ok(text)) => Some(RunOutcome::remote(
                            text,
                            &get_settings(&self.app_handle),
                        )),
                        Ok(Err(e)) => {
                            warn!("Live remote transcription failed ({e}); uploading the recording instead");
                            None
                        }
                        Err(_) => {
                            warn!("Live remote transcription timed out; uploading the recording instead");
                            session.abort();
                            None
                        }
                    };
                    let _ = reply.send(Ok(result));
                    return;
                }
                StreamCmd::Cancel => {
                    let _ = audio_tx.send(LiveAudio::Cancel);
                    return;
                }
            }
        }
        // The route was cleared without a finalize/cancel handshake.
        let _ = audio_tx.send(LiveAudio::Cancel);
    }
}
