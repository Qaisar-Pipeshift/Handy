import React, { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { ToggleSwitch } from "../ui/ToggleSwitch";
import { SettingContainer } from "../ui/SettingContainer";
import { Button } from "../ui/Button";
import { BaseUrlField } from "./PostProcessingSettingsApi/BaseUrlField";
import { ApiKeyField } from "./PostProcessingSettingsApi/ApiKeyField";
import { useSettings } from "../../hooks/useSettings";
import { commands, type RemoteCheck } from "@/bindings";

interface RemoteSettingProps {
  descriptionMode?: "inline" | "tooltip";
  grouped?: boolean;
}

/**
 * Fork: the self-hosted dictation server (engine part). Reads and writes the same settings store the app and the
 * tray's "Live dictation" item use, so values set elsewhere show up here; changes apply from the next recording.
 */
export const RemoteEngineSettings: React.FC<RemoteSettingProps> = React.memo(
  ({ descriptionMode = "tooltip", grouped = false }) => {
    const { t } = useTranslation();
    const { getSetting, updateSetting, isUpdating } = useSettings();
    const [check, setCheck] = useState<RemoteCheck | null>(null);
    const [testing, setTesting] = useState(false);
    const [urlError, setUrlError] = useState("");
    // Bumped on every URL/key edit, so a test that finishes after an edit is not shown against the new values.
    const editGeneration = useRef(0);

    const enabled = getSetting("remote_enabled") ?? false;
    const baseUrl = getSetting("remote_base_url") ?? "";
    const apiKey = getSetting("remote_api_key") ?? "";

    const runTest = async () => {
      const generation = editGeneration.current;
      setTesting(true);
      try {
        const result = await commands.testRemoteConnection();
        if (generation !== editGeneration.current) return;
        setCheck(
          result.status === "ok"
            ? result.data
            : { status: "unreachable", latency_ms: 0, detail: result.error },
        );
      } catch (error) {
        if (generation !== editGeneration.current) return;
        setCheck({ status: "unreachable", latency_ms: 0, detail: String(error) });
      } finally {
        setTesting(false);
      }
    };

    const fieldEdited = () => {
      editGeneration.current += 1;
      setCheck(null);
    };

    const checkMessage = check
      ? t(`settings.remote.test.${check.status}`, {
          ms: check.latency_ms,
          detail: check.detail,
        })
      : "";

    return (
      <>
        <ToggleSwitch
          checked={enabled}
          onChange={(value) => updateSetting("remote_enabled", value)}
          isUpdating={isUpdating("remote_enabled")}
          label={t("settings.remote.enabled.label")}
          description={t("settings.remote.enabled.description")}
          descriptionMode={descriptionMode}
          grouped={grouped}
        />
        <SettingContainer
          title={t("settings.remote.baseUrl.label")}
          description={t("settings.remote.baseUrl.description")}
          descriptionMode={descriptionMode}
          grouped={grouped}
          layout="horizontal"
        >
          <div className="flex flex-col items-end gap-1">
            <BaseUrlField
              value={baseUrl}
              onBlur={(value) => {
                const url = value.trim();
                if (url && !/^https?:\/\//i.test(url)) {
                  setUrlError(t("settings.remote.baseUrl.invalid"));
                  return;
                }
                setUrlError("");
                if (url !== baseUrl) {
                  fieldEdited();
                  void updateSetting("remote_base_url", url);
                }
              }}
              disabled={isUpdating("remote_base_url")}
              placeholder="https://"
            />
            {urlError && <span className="text-xs text-red-400">{urlError}</span>}
          </div>
        </SettingContainer>
        <SettingContainer
          title={t("settings.remote.apiKey.label")}
          description={t("settings.remote.apiKey.description")}
          descriptionMode={descriptionMode}
          grouped={grouped}
          layout="horizontal"
        >
          <ApiKeyField
            value={apiKey}
            onBlur={(value) => {
              if (value.trim() !== apiKey) {
                fieldEdited();
                void updateSetting("remote_api_key", value.trim());
              }
            }}
            disabled={isUpdating("remote_api_key")}
          />
        </SettingContainer>
        <SettingContainer
          title={t("settings.remote.test.label")}
          description={t("settings.remote.test.description")}
          descriptionMode={descriptionMode}
          grouped={grouped}
          layout="horizontal"
        >
          <div className="flex items-center gap-3">
            {checkMessage && (
              <span
                className={`text-sm ${check?.status === "ok" ? "text-green-500" : "text-red-400"}`}
                title={check?.detail}
              >
                {checkMessage}
              </span>
            )}
            <Button
              onClick={() => void runTest()}
              disabled={testing}
              variant="secondary"
              size="sm"
            >
              {testing
                ? t("settings.remote.test.testing")
                : t("settings.remote.test.button")}
            </Button>
          </div>
        </SettingContainer>
      </>
    );
  },
);

RemoteEngineSettings.displayName = "RemoteEngineSettings";

/** Fork: live (streamed) vs one-shot remote dictation. Also switchable from the tray menu. */
export const RemoteStreamingSetting: React.FC<RemoteSettingProps> = React.memo(
  ({ descriptionMode = "tooltip", grouped = false }) => {
    const { t } = useTranslation();
    const { getSetting, updateSetting, isUpdating, refreshSettings } =
      useSettings();

    // The tray can flip this while the window is open; re-read on focus so the switch never lies.
    useEffect(() => {
      const onFocus = () => {
        void refreshSettings();
      };
      window.addEventListener("focus", onFocus);
      return () => window.removeEventListener("focus", onFocus);
    }, [refreshSettings]);

    return (
      <ToggleSwitch
        checked={getSetting("remote_streaming") ?? true}
        onChange={(value) => updateSetting("remote_streaming", value)}
        isUpdating={isUpdating("remote_streaming")}
        disabled={!(getSetting("remote_enabled") ?? false)}
        label={t("settings.remote.streaming.label")}
        description={t("settings.remote.streaming.description")}
        descriptionMode={descriptionMode}
        grouped={grouped}
      />
    );
  },
);

RemoteStreamingSetting.displayName = "RemoteStreamingSetting";

/** Fork, opt-in: clean the text so far at every pause (live + post-process shortcut) so release pastes at once. */
export const RemoteSpeculativeCleanupSetting: React.FC<RemoteSettingProps> =
  React.memo(({ descriptionMode = "tooltip", grouped = false }) => {
    const { t } = useTranslation();
    const { getSetting, updateSetting, isUpdating } = useSettings();
    const available =
      (getSetting("remote_enabled") ?? false) &&
      (getSetting("remote_streaming") ?? true) &&
      (getSetting("post_process_enabled") ?? false);

    return (
      <ToggleSwitch
        checked={getSetting("remote_speculative_cleanup") ?? false}
        onChange={(value) => updateSetting("remote_speculative_cleanup", value)}
        isUpdating={isUpdating("remote_speculative_cleanup")}
        disabled={!available}
        label={t("settings.remote.speculative.label")}
        description={t("settings.remote.speculative.description")}
        descriptionMode={descriptionMode}
        grouped={grouped}
      />
    );
  });

RemoteSpeculativeCleanupSetting.displayName = "RemoteSpeculativeCleanupSetting";
