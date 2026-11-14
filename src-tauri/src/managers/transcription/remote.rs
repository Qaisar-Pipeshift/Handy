//! Fork: the self-hosted dictation server (`crate::remote_transcription`) as the transcription
//! engine. A recording with no live result is uploaded in one shot; either way the server's raw
//! text finishes in `complete`, like a local model's, so the text clean-up that runs after any
//! engine applies to it too.

use std::time::Instant;

use anyhow::Result;
use log::info;
use transcribe_cpp::RunOptions;

use super::{echoes_vocabulary, RunOutcome, RunSetup, Transcription, TranscriptionManager};
use crate::language_detection::{LanguageDecision, LanguageSource};
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
}
