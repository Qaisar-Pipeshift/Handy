import React from "react";
import { useTranslation } from "react-i18next";
import { SettingsGroup } from "../../ui/SettingsGroup";
import {
  RemoteEngineSettings,
  RemoteSpeculativeCleanupSetting,
  RemoteStreamingSetting,
} from "../RemoteServerSettings";
import { PostProcessingToggle } from "../PostProcessingToggle";
import { ShortcutInput } from "../ShortcutInput";
import { useSettings } from "../../../hooks/useSettings";

/**
 * Fork: one page for every per-dictation choice — remote or local engine, live or one-shot, raw or cleaned —
 * next to the existing settings it works with (shortcuts, post-processing). Provider/URL/prompt for cleanup stay on
 * the Post Process page.
 */
export const RemoteSettings: React.FC = () => {
  const { t } = useTranslation();
  const { getSetting } = useSettings();
  const postProcessEnabled = getSetting("post_process_enabled") ?? false;

  return (
    <div className="max-w-3xl w-full mx-auto space-y-6">
      <SettingsGroup title={t("settings.remote.groups.engine")}>
        <RemoteEngineSettings descriptionMode="tooltip" grouped={true} />
      </SettingsGroup>

      <SettingsGroup title={t("settings.remote.groups.mode")}>
        <RemoteStreamingSetting descriptionMode="tooltip" grouped={true} />
      </SettingsGroup>

      <SettingsGroup
        title={t("settings.remote.groups.cleanup")}
        description={t("settings.remote.cleanup.description")}
      >
        <ShortcutInput
          shortcutId="transcribe"
          descriptionMode="tooltip"
          grouped={true}
        />
        <PostProcessingToggle descriptionMode="tooltip" grouped={true} />
        {postProcessEnabled && (
          <ShortcutInput
            shortcutId="transcribe_with_post_process"
            descriptionMode="tooltip"
            grouped={true}
          />
        )}
        {postProcessEnabled && (
          <RemoteSpeculativeCleanupSetting
            descriptionMode="tooltip"
            grouped={true}
          />
        )}
      </SettingsGroup>
    </div>
  );
};
