import type { PaperclipPluginManifestV1 } from "@paperclipai/plugin-sdk";
import { PLUGIN_ID, PLUGIN_VERSION, JOB_KEYS, WEBHOOK_KEYS, TOOL_NAMES, SLOT_IDS, EXPORT_NAMES } from "./constants.js";

const manifest: PaperclipPluginManifestV1 = {
  id: PLUGIN_ID,
  apiVersion: 1,
  version: PLUGIN_VERSION,
  displayName: "Evolution API",
  description:
    "Bridges Evolution API channels (WhatsApp, Telegram, etc.) into Paperclip issues. " +
    "Inbound messages create or update issues; agent comments are forwarded back to the contact instantly.",
  author: "Paperclip Community",
  categories: ["connector", "automation"],

  capabilities: [
    "companies.read",
    "projects.read",
    "issues.read",
    "issues.create",
    "issues.update",
    "issue.comments.create",
    "agents.read",
    "agents.invoke",
    "events.subscribe",
    "plugin.state.read",
    "plugin.state.write",
    "http.outbound",
    "secrets.read-ref",
    "jobs.schedule",
    "webhooks.receive",
    "agent.tools.register",
    "instance.settings.register",
    "activity.log.write",
  ],

  entrypoints: {
    worker: "./dist/worker.js",
    ui: "./dist/ui",
  },

  instanceConfigSchema: {
    type: "object",
    properties: {
      evolutionApiUrl: {
        type: "string",
        title: "Evolution API URL",
        description: "Base URL of the Evolution API instance (e.g. https://evolution.example.com)",
      },
      evolutionApiKey: {
        type: "string",
        title: "Evolution API Key",
      },
      evolutionInstance: {
        type: "string",
        title: "Evolution Instance Name",
        description: "Name of the Evolution API instance to use",
      },
      sqsQueueUrl: {
        type: "string",
        title: "SQS Queue URL",
        description: "AWS SQS queue URL for receiving Evolution API events",
      },
      awsRegion: {
        type: "string",
        title: "AWS Region",
        default: "us-east-2",
      },
      awsAccessKeyId: {
        type: "string",
        title: "AWS Access Key ID",
      },
      awsSecretAccessKey: {
        type: "string",
        title: "AWS Secret Access Key",
      },
      companyId: {
        type: "string",
        title: "Company ID",
        description: "Paperclip company UUID to create issues under",
      },
      projectId: {
        type: "string",
        title: "Project ID",
        description: "Paperclip project UUID to create issues under",
      },
      debounceMs: {
        type: "number",
        title: "Debounce (ms)",
        description: "Buffer inbound messages for this many milliseconds before creating/updating issues",
        default: 30000,
      },
      historyDepth: {
        type: "number",
        title: "History Depth",
        description: "Number of recent messages to include when creating a new issue",
        default: 50,
      },
      routing: {
        type: "object",
        title: "Agent Routing",
        properties: {
          agents: {
            type: "array",
            title: "Agents",
            items: {
              type: "object",
              properties: {
                name: { type: "string", title: "Name (mention keyword)" },
                id: { type: "string", title: "Agent UUID" },
                title: { type: "string", title: "Role title" },
              },
              required: ["name", "id"],
            },
          },
          defaultAgent: {
            type: "string",
            title: "Default Agent Name",
            description: "Agent name to route to when no explicit mention is found",
          },
          internalContacts: {
            type: "array",
            title: "Internal Contact Phones",
            description: "Phone numbers that should route to the internal default agent",
            items: { type: "string" },
          },
          internalDefaultAgent: {
            type: "string",
            title: "Internal Default Agent Name",
            description: "Agent name for internal contacts",
          },
        },
      },
    },
    required: ["evolutionApiUrl", "evolutionApiKey", "evolutionInstance", "companyId", "projectId"],
  },

  jobs: [
    {
      jobKey: JOB_KEYS.sqsPoll,
      displayName: "SQS Poll & Buffer Flush",
      description: "Polls AWS SQS for Evolution API events, buffers messages, and flushes to Paperclip issues.",
      schedule: "*/1 * * * *",
    },
  ],

  webhooks: [
    {
      endpointKey: WEBHOOK_KEYS.ingest,
      displayName: "Evolution API Webhook",
      description: "Receives Evolution API events directly (alternative to SQS).",
    },
  ],

  tools: [
    {
      name: TOOL_NAMES.send,
      displayName: "Send Message (Evolution API)",
      description: "Send a text message to a phone number via Evolution API (WhatsApp, Telegram, etc.).",
      parametersSchema: {
        type: "object",
        properties: {
          phone: {
            type: "string",
            description: "Phone number with country code (e.g. 5548999999999)",
          },
          text: {
            type: "string",
            description: "Message text to send",
          },
        },
        required: ["phone", "text"],
      },
    },
    {
      name: TOOL_NAMES.history,
      displayName: "Conversation History (Evolution API)",
      description: "Retrieve recent conversation history for a contact.",
      parametersSchema: {
        type: "object",
        properties: {
          phone: {
            type: "string",
            description: "Phone number to look up",
          },
          limit: {
            type: "number",
            description: "Maximum messages to return (default 20)",
            default: 20,
          },
        },
        required: ["phone"],
      },
    },
  ],

  ui: {
    slots: [
      {
        type: "settingsPage",
        id: SLOT_IDS.settingsPage,
        displayName: "Evolution API Settings",
        exportName: EXPORT_NAMES.settingsPage,
      },
    ],
  },
};

export default manifest;
