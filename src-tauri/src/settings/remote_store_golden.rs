//! Fork: the owner's real settings store, as Handy Remote wrote it on v0.9.8 (API keys replaced by
//! placeholders), must come through this version's load path unchanged. Every difference the
//! load makes is listed in `INTENDED` with its reason; any other difference is a regression.
//!
//! The one-time migrations in `apply_settings_migrations`, against this store:
//! - onboarding, What's New, shortcut activation, Chinese script and overlay style: their keys are
//!   all stored, so each leaves the store alone (`overlay_style` stays "live").
//! - spoken languages: backfilled once, since onboarding is done and none are stored (`INTENDED`).
//! - schema steps: the store is at 2, so only the step to 3 runs. It only raises a Linux paste
//!   delay of 60 ms for non-clipboard pasting; this store pastes with Ctrl+V after 30 ms.
//! - the retired generic GPU choice: the accelerator is "auto".
//!
//! Then every read merges missing bindings (all three are stored) and syncs the built-in
//! post-processing providers: their structured-output flags already match, and the new
//! `api_format` is added (`INTENDED`). The Custom provider keeps the OpenAI format it uses.

use super::*;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Change {
    /// The load adds a key the store didn't have.
    Added,
    /// The load drops a key this version no longer has.
    Removed,
    /// The load gives a stored key another value.
    Changed,
}

/// Every intended difference: a path (`*` matches any one segment), the change, and why.
const INTENDED: &[(&str, Change, &str)] = &[
    (
        "/settings_schema_version",
        Change::Changed,
        "2 -> 3: the marker moves forward; the step to 3 leaves this store alone",
    ),
    (
        "/ort_accelerator",
        Change::Removed,
        "ONNX Runtime was removed in 0.10.0; transcribe.cpp runs every model",
    ),
    (
        "/spoken_languages",
        Change::Added,
        "0.10 backfills an onboarded user's languages once; only local models read them",
    ),
    (
        "/airpods_mode",
        Change::Added,
        "new in 0.10 (macOS AirPods microphone fix), off by default",
    ),
    (
        "/long_form_chunking",
        Change::Added,
        "new in 0.10 at its default; only local decoding reads it",
    ),
    (
        "/language_detection",
        Change::Added,
        "new in 0.10 at its default; only local decoding reads it",
    ),
    (
        "/language_detection_window_secs",
        Change::Added,
        "new in 0.10 at its default; only local decoding reads it",
    ),
    (
        "/post_process_providers/*/api_format",
        Change::Added,
        "new in 0.10 (#2257): OpenAI for every provider, Custom included; the built-in Anthropic one speaks Messages",
    ),
];

/// The settings object of the owner's store file.
fn stored() -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/handy-remote-0.9.8-settings-store.json");
    let file: Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("fixture is readable"))
            .expect("fixture is valid JSON");
    file["settings"].clone()
}

/// What `get_settings` makes of a stored settings object, and whether it rewrites the store.
fn load(stored: &Value) -> (AppSettings, bool) {
    let (mut settings, mut updated) = settings_from_store(stored);
    updated |= ensure_post_process_defaults(&mut settings);
    (settings, updated)
}

/// Every difference between `stored` and `loaded`, as (path, change).
fn differences(path: &str, stored: &Value, loaded: &Value, out: &mut Vec<(String, Change)>) {
    match (stored, loaded) {
        (Value::Object(before), Value::Object(after)) => {
            for (key, value) in before {
                let child = format!("{path}/{key}");
                match after.get(key) {
                    Some(other) => differences(&child, value, other, out),
                    None => out.push((child, Change::Removed)),
                }
            }
            for key in after.keys().filter(|key| !before.contains_key(*key)) {
                out.push((format!("{path}/{key}"), Change::Added));
            }
        }
        (Value::Array(before), Value::Array(after)) if before.len() == after.len() => {
            for (index, (value, other)) in before.iter().zip(after).enumerate() {
                differences(&format!("{path}/{index}"), value, other, out);
            }
        }
        _ if stored == loaded => {}
        _ => out.push((path.to_string(), Change::Changed)),
    }
}

fn matches(pattern: &str, path: &str) -> bool {
    let pattern: Vec<&str> = pattern.split('/').collect();
    let path: Vec<&str> = path.split('/').collect();
    pattern.len() == path.len()
        && pattern
            .iter()
            .zip(&path)
            .all(|(want, got)| *want == "*" || want == got)
}

#[test]
fn owner_store_from_v0_9_8_loads_unchanged_but_for_intended_changes() {
    let stored = stored();
    // No stored field is dropped by salvage.
    serde_json::from_value::<AppSettings>(stored.clone())
        .expect("the v0.9.8 store must keep parsing strictly");

    let (settings, updated) = load(&stored);
    assert!(updated, "the intended changes are written back once");
    let loaded = serde_json::to_value(&settings).unwrap();

    let mut found = Vec::new();
    differences("", &stored, &loaded, &mut found);
    for (path, change) in &found {
        assert!(
            INTENDED
                .iter()
                .any(|(pattern, intended, _)| intended == change && matches(pattern, path)),
            "unintended change to the owner's settings: {change:?} {path}"
        );
    }
    for (pattern, change, reason) in INTENDED {
        assert!(
            found
                .iter()
                .any(|(path, found)| found == change && matches(pattern, path)),
            "listed change no longer happens, drop it: {change:?} {pattern} ({reason})"
        );
    }

    // What the fork depends on, spelled out.
    for key in [
        "remote_enabled",
        "remote_base_url",
        "remote_api_key",
        "remote_streaming",
        "remote_speculative_cleanup",
        "bindings",
        "shortcut_activation",
        "custom_words",
        "overlay_style",
        "overlay_position",
        "post_process_enabled",
        "post_process_provider_id",
        "post_process_api_keys",
        "post_process_models",
        "post_process_prompts",
        "post_process_selected_prompt_id",
        "selected_model",
        "onboarding_completed",
        "whats_new_last_seen_version",
    ] {
        assert_eq!(loaded[key], stored[key], "{key} changed");
    }
    let providers = stored["post_process_providers"].as_array().unwrap();
    for (index, provider) in providers.iter().enumerate() {
        for (key, value) in provider.as_object().unwrap() {
            assert_eq!(
                &loaded["post_process_providers"][index][key], value,
                "post_process_providers/{index}/{key} changed"
            );
        }
    }
    assert_eq!(settings.overlay_style, OverlayStyle::Live);
    let api_format = |id: &str| settings.post_process_provider(id).unwrap().api_format;
    assert_eq!(api_format("custom"), PostProcessApiFormat::OpenAi);
    assert_eq!(api_format("anthropic"), PostProcessApiFormat::Anthropic);
    // Deterministic under test: the OS's languages are left out of the guess.
    assert_eq!(settings.spoken_languages, vec!["en".to_string()]);
    assert!(crate::remote_transcription::streaming_enabled(&settings));
}

#[test]
fn owner_store_is_rewritten_only_once() {
    let (settings, _) = load(&stored());
    let first = serde_json::to_value(&settings).unwrap();

    let (again, updated) = load(&first);
    assert!(!updated, "a second launch must not rewrite the store");
    assert_eq!(serde_json::to_value(&again).unwrap(), first);
}
