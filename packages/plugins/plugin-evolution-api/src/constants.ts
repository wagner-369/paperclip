export const PLUGIN_ID = "paperclip-evolution-api";
export const PLUGIN_VERSION = "0.1.0";

export const JOB_KEYS = {
  sqsPoll: "sqs-poll",
} as const;

export const WEBHOOK_KEYS = {
  ingest: "evolution-ingest",
} as const;

export const TOOL_NAMES = {
  send: "evolution-send",
  history: "evolution-history",
} as const;

export const SLOT_IDS = {
  settingsPage: "evolution-settings",
} as const;

export const EXPORT_NAMES = {
  settingsPage: "EvolutionSettingsPage",
} as const;

export const SOURCE_TAG = "<!-- source: plugin-evolution-api -->";
