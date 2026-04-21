/**
 * End-to-end tests for the Evolution API plugin.
 *
 * Validates the full lifecycle: inbound messages (SQS, webhook, direct poll),
 * outbound forwarding, media handling, conversation lifecycle, agent routing,
 * config validation, and tools.
 *
 * DB and external HTTP calls are mocked via vitest. The plugin SDK test harness
 * provides an in-memory Paperclip host.
 */

import { describe, it, expect, vi, beforeEach, afterEach, type Mock } from "vitest";
import { createTestHarness, type TestHarness } from "@paperclipai/plugin-sdk/testing";
import manifest from "../src/manifest.js";
import { SOURCE_TAG, JOB_KEYS, TOOL_NAMES, WEBHOOK_KEYS } from "../src/constants.js";

// ─── Mocks ───────────────────────────────────────────────────────────────

// DB mock — all functions return controllable values
vi.mock("../src/lib/db.js", () => ({
  upsertConversation: vi.fn(),
  getConversationByIssueId: vi.fn(),
  getConversationByPhone: vi.fn(),
  getActiveConversations: vi.fn(),
  insertMessage: vi.fn(),
  getHistory: vi.fn(),
  isMessageSeen: vi.fn(),
  updateConversationIssue: vi.fn(),
  updateConversationStatus: vi.fn(),
}));

// Evolution HTTP client
vi.mock("../src/lib/evolution-client.js", () => ({
  sendTextMessage: vi.fn(),
  getMediaBase64: vi.fn(),
  toDataUri: vi.fn((b64: string, mime: string) => `data:${mime};base64,${b64}`),
}));

// SQS
vi.mock("../src/lib/sqs.js", () => ({
  pollSqs: vi.fn(),
  deleteSqsMessage: vi.fn(),
}));

// Evolution direct poll
vi.mock("../src/lib/evolution-poll.js", () => ({
  pollEvolutionMessages: vi.fn(),
  convertPollRecord: vi.fn((r: Record<string, unknown>) => ({
    key: r.key,
    pushName: r.pushName,
    message: r.message,
    messageTimestamp: r.messageTimestamp,
  })),
}));

import {
  upsertConversation,
  getConversationByIssueId,
  getConversationByPhone,
  getActiveConversations,
  insertMessage,
  getHistory,
  isMessageSeen,
  updateConversationIssue,
  updateConversationStatus,
} from "../src/lib/db.js";

import {
  sendTextMessage,
  getMediaBase64,
} from "../src/lib/evolution-client.js";

import { pollSqs, deleteSqsMessage } from "../src/lib/sqs.js";
import { pollEvolutionMessages } from "../src/lib/evolution-poll.js";

// ─── Fixtures ────────────────────────────────────────────────────────────

const COMPANY_ID = "c0000000-0000-0000-0000-000000000001";
const PROJECT_ID = "p0000000-0000-0000-0000-000000000001";
const AGENT_ID = "a0000000-0000-0000-0000-000000000001";
const AGENT_NAME = "suporte";
const CONV_ID = "conv0000-0000-0000-0000-000000000001";
const ISSUE_ID = "iss00000-0000-0000-0000-000000000001";
const PHONE = "5548999999999@s.whatsapp.net";
const PHONE_CLEAN = "5548999999999";

const BASE_CONFIG: Record<string, unknown> = {
  evolutionApiUrl: "https://evo.test",
  evolutionApiKey: "test-key",
  evolutionInstance: "test-instance",
  companyId: COMPANY_ID,
  projectId: PROJECT_ID,
  debounceMs: 0, // flush immediately in tests
  routing: {
    agents: [{ name: AGENT_NAME, id: AGENT_ID, title: "Suporte" }],
    defaultAgent: AGENT_NAME,
    internalContacts: [],
    internalDefaultAgent: "",
  },
};

function makeConversation(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    id: CONV_ID,
    companyId: COMPANY_ID,
    instanceKey: "test-instance",
    phone: PHONE,
    contactName: "João",
    channel: "whatsapp",
    issueId: null,
    assignedAgentId: null,
    status: "active",
    lastMessageAt: new Date(),
    createdAt: new Date(),
    ...overrides,
  };
}

function makeSqsMessageBody(messages: Record<string, unknown>[]) {
  return JSON.stringify({
    event: "messages.upsert",
    data: messages,
  });
}

function makeRawTextMessage(text: string, id = "msg-001") {
  return {
    key: { id, remoteJid: PHONE, fromMe: false },
    pushName: "João",
    message: { conversation: text },
    messageTimestamp: Date.now() / 1000,
  };
}

function makeRawImageMessage(caption: string | null = null, id = "msg-img-001") {
  return {
    key: { id, remoteJid: PHONE, fromMe: false },
    pushName: "João",
    message: {
      imageMessage: {
        url: "https://evo.test/media/img.jpg",
        mimetype: "image/jpeg",
        ...(caption ? { caption } : {}),
      },
    },
    messageTimestamp: Date.now() / 1000,
  };
}

function makeRawVideoMessage(id = "msg-vid-001") {
  return {
    key: { id, remoteJid: PHONE, fromMe: false },
    pushName: "João",
    message: {
      videoMessage: {
        url: "https://evo.test/media/vid.mp4",
        mimetype: "video/mp4",
        caption: "olha isso",
      },
    },
    messageTimestamp: Date.now() / 1000,
  };
}

function makeRawAudioMessage(id = "msg-aud-001") {
  return {
    key: { id, remoteJid: PHONE, fromMe: false },
    pushName: "João",
    message: {
      audioMessage: {
        url: "https://evo.test/media/aud.ogg",
        mimetype: "audio/ogg",
      },
    },
    messageTimestamp: Date.now() / 1000,
  };
}

function makeRawDocumentMessage(id = "msg-doc-001") {
  return {
    key: { id, remoteJid: PHONE, fromMe: false },
    pushName: "João",
    message: {
      documentMessage: {
        url: "https://evo.test/media/doc.pdf",
        mimetype: "application/pdf",
        fileName: "contrato.pdf",
      },
    },
    messageTimestamp: Date.now() / 1000,
  };
}

function makeRawStickerMessage(id = "msg-stk-001") {
  return {
    key: { id, remoteJid: PHONE, fromMe: false },
    pushName: "João",
    message: {
      stickerMessage: {
        url: "https://evo.test/media/stk.webp",
        mimetype: "image/webp",
      },
    },
    messageTimestamp: Date.now() / 1000,
  };
}

function makeRawLocationMessage(id = "msg-loc-001") {
  return {
    key: { id, remoteJid: PHONE, fromMe: false },
    pushName: "João",
    message: {
      locationMessage: {
        degreesLatitude: -27.5954,
        degreesLongitude: -48.548,
      },
    },
    messageTimestamp: Date.now() / 1000,
  };
}

function makeRawContactMessage(id = "msg-contact-001") {
  return {
    key: { id, remoteJid: PHONE, fromMe: false },
    pushName: "João",
    message: {
      contactMessage: {
        displayName: "Maria Silva",
        vcard: "BEGIN:VCARD\nFN:Maria Silva\nEND:VCARD",
      },
    },
    messageTimestamp: Date.now() / 1000,
  };
}

// ─── Helpers ─────────────────────────────────────────────────────────────

let harness: TestHarness;
let plugin: typeof import("../src/worker.js").default;

async function setupPlugin(configOverrides: Record<string, unknown> = {}) {
  // Dynamic import to reset module state between tests
  const workerModule = await import("../src/worker.js");
  plugin = workerModule.default;

  harness = createTestHarness({
    manifest,
    capabilities: [...manifest.capabilities, "events.emit"],
    config: { ...BASE_CONFIG, ...configOverrides },
  });

  harness.seed({
    companies: [{
      id: COMPANY_ID,
      name: "Test Co",
      slug: "test-co",
      iconUrl: null,
      brandColor: null,
      feedbackDataSharingEnabled: false,
      feedbackDataSharingConsentAt: null,
      feedbackDataSharingConsentByUserId: null,
      feedbackDataSharingTermsVersion: null,
      createdAt: new Date(),
      updatedAt: new Date(),
    }],
    projects: [{
      id: PROJECT_ID,
      companyId: COMPANY_ID,
      name: "Test Project",
      slug: "test-project",
      key: "TP",
      description: null,
      repoUrl: null,
      defaultBranch: null,
      color: null,
      archived: false,
      env: null,
      issueCounter: 0,
      createdAt: new Date(),
      updatedAt: new Date(),
    }],
    agents: [{
      id: AGENT_ID,
      companyId: COMPANY_ID,
      name: AGENT_NAME,
      model: "claude-sonnet-4-20250514",
      status: "idle",
      systemPrompt: null,
      configHash: null,
      icon: null,
      createdAt: new Date(),
      updatedAt: new Date(),
    }],
  });

  await plugin.definition.setup(harness.ctx);
}

function resetMocks() {
  vi.clearAllMocks();

  // Default DB mock behaviors
  (isMessageSeen as Mock).mockResolvedValue(false);
  (upsertConversation as Mock).mockResolvedValue(makeConversation());
  (getConversationByPhone as Mock).mockResolvedValue(makeConversation());
  (getConversationByIssueId as Mock).mockResolvedValue(null);
  (getActiveConversations as Mock).mockResolvedValue([]);
  (insertMessage as Mock).mockResolvedValue("msg-id");
  (getHistory as Mock).mockResolvedValue([]);
  (updateConversationIssue as Mock).mockResolvedValue(undefined);
  (updateConversationStatus as Mock).mockResolvedValue(undefined);

  // Default HTTP mock behaviors
  (sendTextMessage as Mock).mockResolvedValue(true);
  (getMediaBase64 as Mock).mockResolvedValue(null);
  (pollSqs as Mock).mockResolvedValue([]);
  (deleteSqsMessage as Mock).mockResolvedValue(undefined);
  (pollEvolutionMessages as Mock).mockResolvedValue([]);
}

// ─── Test suites ─────────────────────────────────────────────────────────

describe("Evolution API Plugin E2E", () => {
  beforeEach(async () => {
    resetMocks();
    await setupPlugin();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // ═══════════════════════════════════════════════════════════════════════
  // CONFIG VALIDATION
  // ═══════════════════════════════════════════════════════════════════════

  describe("config validation", () => {
    it("skips job gracefully when required config fields are missing", async () => {
      harness.setConfig({});

      await harness.runJob(JOB_KEYS.sqsPoll);

      const warns = harness.logs.filter((l) => l.level === "warn");
      expect(warns.some((w) => w.message.includes("missing required config"))).toBe(true);
    });

    it("skips issue.updated event when config is missing", async () => {
      harness.setConfig({});

      await harness.emit("issue.updated", {
        id: ISSUE_ID,
        status: "done",
      }, { entityId: ISSUE_ID });

      expect(getConversationByIssueId).not.toHaveBeenCalled();
    });
  });

  // ═══════════════════════════════════════════════════════════════════════
  // INBOUND: SQS POLLING
  // ═══════════════════════════════════════════════════════════════════════

  describe("inbound via SQS polling", () => {
    it("processes text messages from SQS and creates issues", async () => {
      harness.setConfig({
        ...BASE_CONFIG,
        sqsQueueUrl: "https://sqs.us-east-2.amazonaws.com/123/queue",
        awsAccessKeyId: "AKIA-TEST",
        awsSecretAccessKey: "secret-test",
      });

      const rawMsg = makeRawTextMessage("Olá, preciso de ajuda");
      (pollSqs as Mock).mockResolvedValue([{
        body: makeSqsMessageBody([rawMsg]),
        receiptHandle: "receipt-1",
      }]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      // Verify SQS message was deleted
      expect(deleteSqsMessage).toHaveBeenCalled();

      // Verify conversation upsert
      expect(upsertConversation).toHaveBeenCalledWith(
        expect.any(String),
        COMPANY_ID,
        "test-instance",
        PHONE,
        "João",
      );

      // Verify message stored
      expect(insertMessage).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        "inbound",
        "text",
        "Olá, preciso de ajuda",
        "msg-001",
        expect.anything(), // raw payload
        null, // mediaUrl (text has none)
        null, // mediaMimeType
      );
    });

    it("deduplicates already-seen messages", async () => {
      harness.setConfig({
        ...BASE_CONFIG,
        sqsQueueUrl: "https://sqs.us-east-2.amazonaws.com/123/queue",
        awsAccessKeyId: "AKIA-TEST",
        awsSecretAccessKey: "secret-test",
      });

      (isMessageSeen as Mock).mockResolvedValue(true);

      const rawMsg = makeRawTextMessage("duplicate");
      (pollSqs as Mock).mockResolvedValue([{
        body: makeSqsMessageBody([rawMsg]),
        receiptHandle: "receipt-1",
      }]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      // Should NOT store or buffer
      expect(upsertConversation).not.toHaveBeenCalled();
    });

    it("skips group messages", async () => {
      harness.setConfig({
        ...BASE_CONFIG,
        sqsQueueUrl: "https://sqs.us-east-2.amazonaws.com/123/queue",
        awsAccessKeyId: "AKIA-TEST",
        awsSecretAccessKey: "secret-test",
      });

      const groupMsg = {
        key: { id: "msg-group", remoteJid: "120363000000@g.us", fromMe: false },
        pushName: "GroupUser",
        message: { conversation: "group message" },
      };

      (pollSqs as Mock).mockResolvedValue([{
        body: makeSqsMessageBody([groupMsg]),
        receiptHandle: "receipt-1",
      }]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      expect(upsertConversation).not.toHaveBeenCalled();
    });

    it("skips outbound (fromMe) messages", async () => {
      harness.setConfig({
        ...BASE_CONFIG,
        sqsQueueUrl: "https://sqs.us-east-2.amazonaws.com/123/queue",
        awsAccessKeyId: "AKIA-TEST",
        awsSecretAccessKey: "secret-test",
      });

      const outboundMsg = {
        key: { id: "msg-out", remoteJid: PHONE, fromMe: true },
        pushName: "Bot",
        message: { conversation: "auto-reply" },
      };

      (pollSqs as Mock).mockResolvedValue([{
        body: makeSqsMessageBody([outboundMsg]),
        receiptHandle: "receipt-1",
      }]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      expect(upsertConversation).not.toHaveBeenCalled();
    });
  });

  // ═══════════════════════════════════════════════════════════════════════
  // INBOUND: DIRECT POLL
  // ═══════════════════════════════════════════════════════════════════════

  describe("inbound via direct Evolution API poll", () => {
    it("processes messages from Evolution API direct poll", async () => {
      const rawMsg = makeRawTextMessage("msg via poll", "msg-poll-001");
      (pollEvolutionMessages as Mock).mockResolvedValue([rawMsg]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      expect(upsertConversation).toHaveBeenCalled();
      expect(insertMessage).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        "inbound",
        "text",
        "msg via poll",
        "msg-poll-001",
        expect.anything(),
        null,
        null,
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════════════
  // INBOUND: WEBHOOK
  // ═══════════════════════════════════════════════════════════════════════

  describe("inbound via webhook", () => {
    it("processes webhook payload and creates issue", async () => {
      const rawMsg = makeRawTextMessage("webhook msg", "msg-wh-001");
      const body = makeSqsMessageBody([rawMsg]);

      await plugin.definition.onWebhook!({
        endpointKey: WEBHOOK_KEYS.ingest,
        headers: { "content-type": "application/json" },
        rawBody: body,
        parsedBody: JSON.parse(body),
        requestId: "req-001",
      });

      expect(upsertConversation).toHaveBeenCalled();
      expect(insertMessage).toHaveBeenCalled();
    });

    it("ignores webhook with wrong endpoint key", async () => {
      await plugin.definition.onWebhook!({
        endpointKey: "wrong-endpoint",
        headers: {},
        rawBody: "{}",
        requestId: "req-002",
      });

      expect(upsertConversation).not.toHaveBeenCalled();
    });
  });

  // ═══════════════════════════════════════════════════════════════════════
  // OUTBOUND: NO COMMENT FORWARDING (tool-only mode)
  // ═══════════════════════════════════════════════════════════════════════

  describe("outbound: comments are NOT forwarded to WhatsApp", () => {
    it("does not forward any agent comments to WhatsApp (tool-only mode)", async () => {
      (getConversationByIssueId as Mock).mockResolvedValue(makeConversation({ issueId: ISSUE_ID }));

      await harness.emit("issue.comment.created", {
        issueId: ISSUE_ID,
        body: "Olá, posso ajudar?",
        authorAgentId: AGENT_ID,
      }, { entityId: ISSUE_ID });

      // No event handler for issue.comment.created — nothing should happen
      expect(sendTextMessage).not.toHaveBeenCalled();
      expect(insertMessage).not.toHaveBeenCalled();
    });
  });

  // ═══════════════════════════════════════════════════════════════════════
  // STALE CONVERSATION SYNC (Phase 1c)
  // ═══════════════════════════════════════════════════════════════════════

  describe("stale conversation sync during job poll", () => {
    it("resolves conversations whose linked issue is done", async () => {
      const doneIssueId = "iss-stale-done";
      const conv = makeConversation({ issueId: doneIssueId, status: "active" });
      (getActiveConversations as Mock).mockResolvedValue([conv]);

      harness.seed({
        issues: [{
          id: doneIssueId,
          companyId: COMPANY_ID,
          projectId: PROJECT_ID,
          projectWorkspaceId: null,
          goalId: null,
          parentId: null,
          title: "Done issue",
          description: null,
          status: "done",
          priority: "medium",
          assigneeAgentId: AGENT_ID,
          assigneeUserId: null,
          checkoutRunId: null,
          executionRunId: null,
          executionAgentNameKey: null,
          executionLockedAt: null,
          createdByAgentId: null,
          createdByUserId: null,
          issueNumber: null,
          identifier: null,
          requestDepth: 0,
          billingCode: null,
          assigneeAdapterOverrides: null,
          executionWorkspaceId: null,
          executionWorkspacePreference: null,
          executionWorkspaceSettings: null,
          startedAt: null,
          completedAt: new Date(),
          cancelledAt: null,
          hiddenAt: null,
          createdAt: new Date(),
          updatedAt: new Date(),
        }],
      });

      await harness.runJob(JOB_KEYS.sqsPoll);

      expect(updateConversationStatus).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        "resolved",
      );

      // Should NOT try to forward comments for resolved conversations
      expect(sendTextMessage).not.toHaveBeenCalled();
    });
  });

  // ═══════════════════════════════════════════════════════════════════════
  // CONVERSATION LIFECYCLE (issue.updated event)
  // ═══════════════════════════════════════════════════════════════════════

  describe("conversation lifecycle (issue.updated)", () => {
    it("marks conversation as resolved when issue is done", async () => {
      (getConversationByIssueId as Mock).mockResolvedValue(makeConversation({ issueId: ISSUE_ID, status: "active" }));

      await harness.emit("issue.updated", {
        id: ISSUE_ID,
        status: "done",
      }, { entityId: ISSUE_ID });

      expect(updateConversationStatus).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        "resolved",
      );
    });

    it("marks conversation as resolved when issue is cancelled", async () => {
      (getConversationByIssueId as Mock).mockResolvedValue(makeConversation({ issueId: ISSUE_ID, status: "active" }));

      await harness.emit("issue.updated", {
        id: ISSUE_ID,
        status: "cancelled",
      }, { entityId: ISSUE_ID });

      expect(updateConversationStatus).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        "resolved",
      );
    });

    it("reactivates conversation when resolved issue is reopened", async () => {
      (getConversationByIssueId as Mock).mockResolvedValue(makeConversation({ issueId: ISSUE_ID, status: "resolved" }));

      await harness.emit("issue.updated", {
        id: ISSUE_ID,
        status: "todo",
      }, { entityId: ISSUE_ID });

      expect(updateConversationStatus).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        "active",
      );
    });

    it("does not reactivate already-active conversation", async () => {
      (getConversationByIssueId as Mock).mockResolvedValue(makeConversation({ issueId: ISSUE_ID, status: "active" }));

      await harness.emit("issue.updated", {
        id: ISSUE_ID,
        status: "in_progress",
      }, { entityId: ISSUE_ID });

      expect(updateConversationStatus).not.toHaveBeenCalled();
    });

    it("ignores issues without linked conversation", async () => {
      (getConversationByIssueId as Mock).mockResolvedValue(null);

      await harness.emit("issue.updated", {
        id: "random-issue",
        status: "done",
      }, { entityId: "random-issue" });

      expect(updateConversationStatus).not.toHaveBeenCalled();
    });
  });

  // ═══════════════════════════════════════════════════════════════════════
  // ISSUE CREATION & COMMENT LOGIC
  // ═══════════════════════════════════════════════════════════════════════

  describe("issue creation and comment routing", () => {
    it("creates a new issue when no existing conversation issue", async () => {
      (getConversationByPhone as Mock).mockResolvedValue(makeConversation({ issueId: null }));

      const rawMsg = makeRawTextMessage("Preciso de ajuda", "msg-new-001");
      (pollEvolutionMessages as Mock).mockResolvedValue([rawMsg]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      // Issue should have been created via harness
      // Check updateConversationIssue was called (linking issue to conversation)
      expect(updateConversationIssue).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        expect.any(String), // new issue ID
        AGENT_ID,
      );
    });

    it("creates new issue when existing issue is done", async () => {
      const existingIssueId = "iss-done-001";
      (getConversationByPhone as Mock).mockResolvedValue(makeConversation({ issueId: existingIssueId }));

      // Seed the existing issue as "done"
      harness.seed({
        issues: [{
          id: existingIssueId,
          companyId: COMPANY_ID,
          projectId: PROJECT_ID,
          projectWorkspaceId: null,
          goalId: null,
          parentId: null,
          title: "Old issue",
          description: null,
          status: "done",
          priority: "medium",
          assigneeAgentId: AGENT_ID,
          assigneeUserId: null,
          checkoutRunId: null,
          executionRunId: null,
          executionAgentNameKey: null,
          executionLockedAt: null,
          createdByAgentId: null,
          createdByUserId: null,
          issueNumber: null,
          identifier: null,
          requestDepth: 0,
          billingCode: null,
          assigneeAdapterOverrides: null,
          executionWorkspaceId: null,
          executionWorkspacePreference: null,
          executionWorkspaceSettings: null,
          startedAt: null,
          completedAt: new Date(),
          cancelledAt: null,
          hiddenAt: null,
          createdAt: new Date(),
          updatedAt: new Date(),
        }],
      });

      const rawMsg = makeRawTextMessage("Nova pergunta", "msg-newq-001");
      (pollEvolutionMessages as Mock).mockResolvedValue([rawMsg]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      // Should create a NEW issue, not comment on the done one
      expect(updateConversationIssue).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        expect.not.stringContaining(existingIssueId),
        AGENT_ID,
      );
    });

    it("adds comment when existing issue is active with same agent", async () => {
      const existingIssueId = "iss-active-001";
      (getConversationByPhone as Mock).mockResolvedValue(makeConversation({ issueId: existingIssueId, assignedAgentId: AGENT_ID }));

      harness.seed({
        issues: [{
          id: existingIssueId,
          companyId: COMPANY_ID,
          projectId: PROJECT_ID,
          projectWorkspaceId: null,
          goalId: null,
          parentId: null,
          title: "Active issue",
          description: null,
          status: "in_progress",
          priority: "medium",
          assigneeAgentId: AGENT_ID,
          assigneeUserId: null,
          checkoutRunId: null,
          executionRunId: null,
          executionAgentNameKey: null,
          executionLockedAt: null,
          createdByAgentId: null,
          createdByUserId: null,
          issueNumber: null,
          identifier: null,
          requestDepth: 0,
          billingCode: null,
          assigneeAdapterOverrides: null,
          executionWorkspaceId: null,
          executionWorkspacePreference: null,
          executionWorkspaceSettings: null,
          startedAt: null,
          completedAt: null,
          cancelledAt: null,
          hiddenAt: null,
          createdAt: new Date(),
          updatedAt: new Date(),
        }],
      });

      const rawMsg = makeRawTextMessage("Follow-up question", "msg-followup-001");
      (pollEvolutionMessages as Mock).mockResolvedValue([rawMsg]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      // Should NOT create new issue — should add comment
      expect(updateConversationIssue).not.toHaveBeenCalled();
      // Verify a comment was created with SOURCE_TAG
      const logMessages = harness.logs.map((l) => l.message);
      expect(logMessages.some((m) => m.includes("Comment on issue"))).toBe(true);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════
  // MEDIA MESSAGES
  // ═══════════════════════════════════════════════════════════════════════

  describe("media message handling", () => {
    it("processes image messages with URL", async () => {
      const rawMsg = makeRawImageMessage("foto do produto", "msg-img-001");
      (pollEvolutionMessages as Mock).mockResolvedValue([rawMsg]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      expect(insertMessage).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        "inbound",
        "image",
        "foto do produto",
        "msg-img-001",
        expect.anything(),
        "https://evo.test/media/img.jpg", // media URL preserved
        "image/jpeg",
      );
    });

    it("processes image without caption", async () => {
      const rawMsg = makeRawImageMessage(null, "msg-img-nocap");
      (pollEvolutionMessages as Mock).mockResolvedValue([rawMsg]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      expect(insertMessage).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        "inbound",
        "image",
        null,
        "msg-img-nocap",
        expect.anything(),
        "https://evo.test/media/img.jpg",
        "image/jpeg",
      );
    });

    it("processes video messages", async () => {
      const rawMsg = makeRawVideoMessage("msg-vid-001");
      (pollEvolutionMessages as Mock).mockResolvedValue([rawMsg]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      expect(insertMessage).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        "inbound",
        "video",
        "olha isso",
        "msg-vid-001",
        expect.anything(),
        "https://evo.test/media/vid.mp4",
        "video/mp4",
      );
    });

    it("processes audio messages", async () => {
      const rawMsg = makeRawAudioMessage("msg-aud-001");
      (pollEvolutionMessages as Mock).mockResolvedValue([rawMsg]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      expect(insertMessage).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        "inbound",
        "audio",
        null,
        "msg-aud-001",
        expect.anything(),
        "https://evo.test/media/aud.ogg",
        "audio/ogg",
      );
    });

    it("processes document messages", async () => {
      const rawMsg = makeRawDocumentMessage("msg-doc-001");
      (pollEvolutionMessages as Mock).mockResolvedValue([rawMsg]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      expect(insertMessage).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        "inbound",
        "document",
        "contrato.pdf",
        "msg-doc-001",
        expect.anything(),
        "https://evo.test/media/doc.pdf",
        "application/pdf",
      );
    });

    it("processes sticker messages", async () => {
      const rawMsg = makeRawStickerMessage("msg-stk-001");
      (pollEvolutionMessages as Mock).mockResolvedValue([rawMsg]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      expect(insertMessage).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        "inbound",
        "sticker",
        null,
        "msg-stk-001",
        expect.anything(),
        "https://evo.test/media/stk.webp",
        "image/webp",
      );
    });

    it("processes location messages", async () => {
      const rawMsg = makeRawLocationMessage("msg-loc-001");
      (pollEvolutionMessages as Mock).mockResolvedValue([rawMsg]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      expect(insertMessage).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        "inbound",
        "location",
        "-27.5954,-48.548",
        "msg-loc-001",
        expect.anything(),
        null, // location has no media URL
        null,
      );
    });

    it("processes contact messages", async () => {
      const rawMsg = makeRawContactMessage("msg-contact-001");
      (pollEvolutionMessages as Mock).mockResolvedValue([rawMsg]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      expect(insertMessage).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        "inbound",
        "contact",
        "Maria Silva",
        "msg-contact-001",
        expect.anything(),
        null, // contact has no media URL
        null,
      );
    });

    it("falls back to base64 download when media URL is missing", async () => {
      const rawMsg = {
        key: { id: "msg-img-nurl", remoteJid: PHONE, fromMe: false },
        pushName: "João",
        message: {
          imageMessage: {
            // no url or directPath
            mimetype: "image/png",
            caption: "sem url",
          },
        },
        messageTimestamp: Date.now() / 1000,
      };

      (pollEvolutionMessages as Mock).mockResolvedValue([rawMsg]);
      (getMediaBase64 as Mock).mockResolvedValue({
        base64: "iVBOR...",
        mimetype: "image/png",
      });

      await harness.runJob(JOB_KEYS.sqsPoll);

      // Should have tried to fetch base64
      expect(getMediaBase64).toHaveBeenCalledWith(
        "https://evo.test",
        "test-key",
        "test-instance",
        "msg-img-nurl",
        PHONE,
      );

      // Should store the data URI
      expect(insertMessage).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        "inbound",
        "image",
        "sem url",
        "msg-img-nurl",
        expect.anything(),
        "data:image/png;base64,iVBOR...",
        "image/png",
      );
    });
  });

  // Comment poll bridge was removed — outbound is tool-only now.

  // ═══════════════════════════════════════════════════════════════════════
  // TOOLS
  // ═══════════════════════════════════════════════════════════════════════

  describe("tool: evolution-send", () => {
    it("sends a message and stores it", async () => {
      const result = await harness.executeTool(TOOL_NAMES.send, {
        phone: PHONE_CLEAN,
        text: "Olá!",
      }, { agentId: AGENT_ID });

      expect(sendTextMessage).toHaveBeenCalledWith(
        expect.anything(),
        "https://evo.test",
        "test-key",
        "test-instance",
        `${PHONE_CLEAN}@s.whatsapp.net`,
        `*${AGENT_NAME}:*\nOlá!`,
      );

      expect(result).toEqual({ content: `Message sent to ${PHONE_CLEAN}` });
      expect(upsertConversation).toHaveBeenCalled();
      expect(insertMessage).toHaveBeenCalled();
    });

    it("handles send failure", async () => {
      (sendTextMessage as Mock).mockResolvedValue(false);

      const result = await harness.executeTool(TOOL_NAMES.send, {
        phone: PHONE_CLEAN,
        text: "fail test",
      }, { agentId: AGENT_ID });

      expect(result).toEqual({ content: `Failed to send message to ${PHONE_CLEAN}` });
      // Should NOT store message on failure
      expect(insertMessage).not.toHaveBeenCalled();
    });

    it("preserves phone with existing JID suffix", async () => {
      await harness.executeTool(TOOL_NAMES.send, {
        phone: PHONE, // already has @s.whatsapp.net
        text: "test",
      }, { agentId: AGENT_ID });

      expect(sendTextMessage).toHaveBeenCalledWith(
        expect.anything(),
        expect.anything(),
        expect.anything(),
        expect.anything(),
        PHONE, // should not double-suffix
        expect.anything(),
      );
    });
  });

  describe("tool: evolution-history", () => {
    it("returns formatted conversation history", async () => {
      const now = new Date();
      (getConversationByPhone as Mock).mockResolvedValue(makeConversation());
      (getHistory as Mock).mockResolvedValue([
        {
          id: "h1",
          conversationId: CONV_ID,
          direction: "inbound",
          messageType: "text",
          content: "Oi",
          mediaUrl: null,
          mediaMimeType: null,
          evolutionMsgId: null,
          issueId: null,
          createdAt: now,
        },
        {
          id: "h2",
          conversationId: CONV_ID,
          direction: "outbound",
          messageType: "text",
          content: "Oi, como posso ajudar?",
          mediaUrl: null,
          mediaMimeType: null,
          evolutionMsgId: null,
          issueId: null,
          createdAt: now,
        },
      ]);

      const result = await harness.executeTool(TOOL_NAMES.history, {
        phone: PHONE_CLEAN,
        limit: 10,
      });

      expect(result.content).toContain("→ Oi");
      expect(result.content).toContain("← Oi, como posso ajudar?");
    });

    it("returns message when no conversation found", async () => {
      (getConversationByPhone as Mock).mockResolvedValue(null);

      const result = await harness.executeTool(TOOL_NAMES.history, {
        phone: "5500000000000",
      });

      expect(result.content).toContain("No conversation found");
    });

    it("includes media type tag in history output", async () => {
      (getConversationByPhone as Mock).mockResolvedValue(makeConversation());
      (getHistory as Mock).mockResolvedValue([
        {
          id: "h-img",
          conversationId: CONV_ID,
          direction: "inbound",
          messageType: "image",
          content: "foto",
          mediaUrl: "https://example.com/img.jpg",
          mediaMimeType: "image/jpeg",
          evolutionMsgId: null,
          issueId: null,
          createdAt: new Date(),
        },
      ]);

      const result = await harness.executeTool(TOOL_NAMES.history, { phone: PHONE_CLEAN });

      expect(result.content).toContain("[media: image/jpeg]");
    });
  });

  // ═══════════════════════════════════════════════════════════════════════
  // AGENT ROUTING
  // ═══════════════════════════════════════════════════════════════════════

  describe("agent routing", () => {
    it("routes to mentioned agent in message text", async () => {
      harness.setConfig({
        ...BASE_CONFIG,
        routing: {
          agents: [
            { name: "suporte", id: "agent-suporte", title: "Suporte" },
            { name: "vendas", id: "agent-vendas", title: "Vendas" },
          ],
          defaultAgent: "suporte",
          internalContacts: [],
          internalDefaultAgent: "",
        },
      });

      (getConversationByPhone as Mock).mockResolvedValue(makeConversation({ issueId: null }));

      const rawMsg = makeRawTextMessage("preciso falar com vendas", "msg-route-001");
      (pollEvolutionMessages as Mock).mockResolvedValue([rawMsg]);

      await harness.runJob(JOB_KEYS.sqsPoll);

      expect(updateConversationIssue).toHaveBeenCalledWith(
        expect.any(String),
        CONV_ID,
        expect.any(String),
        "agent-vendas", // should route to vendas, not suporte
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════════════
  // HEALTH CHECK
  // ═══════════════════════════════════════════════════════════════════════

  describe("health check", () => {
    it("returns ok status", async () => {
      const health = await plugin.definition.onHealth!();
      expect(health).toEqual({ status: "ok", message: "Evolution API plugin running" });
    });
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// UNIT TESTS: normalise.ts
// ═══════════════════════════════════════════════════════════════════════════

describe("normalise", () => {
  // Direct import — these are pure functions, no mocking needed
  let normalisePhone: typeof import("../src/lib/normalise.js").normalisePhone;
  let toWhatsAppJid: typeof import("../src/lib/normalise.js").toWhatsAppJid;
  let normaliseMessage: typeof import("../src/lib/normalise.js").normaliseMessage;
  let extractMessagesFromSqsBody: typeof import("../src/lib/normalise.js").extractMessagesFromSqsBody;

  beforeEach(async () => {
    const mod = await import("../src/lib/normalise.js");
    normalisePhone = mod.normalisePhone;
    toWhatsAppJid = mod.toWhatsAppJid;
    normaliseMessage = mod.normaliseMessage;
    extractMessagesFromSqsBody = mod.extractMessagesFromSqsBody;
  });

  describe("normalisePhone", () => {
    it("strips @s.whatsapp.net", () => {
      expect(normalisePhone("5548999999999@s.whatsapp.net")).toBe("5548999999999");
    });

    it("strips @lid", () => {
      expect(normalisePhone("12345@lid")).toBe("12345");
    });

    it("strips @g.us", () => {
      expect(normalisePhone("120363000000@g.us")).toBe("120363000000");
    });

    it("returns clean number unchanged", () => {
      expect(normalisePhone("5548999999999")).toBe("5548999999999");
    });
  });

  describe("toWhatsAppJid", () => {
    it("adds @s.whatsapp.net to clean number", () => {
      expect(toWhatsAppJid("5548999999999")).toBe("5548999999999@s.whatsapp.net");
    });

    it("preserves existing JID suffix", () => {
      expect(toWhatsAppJid("5548999999999@s.whatsapp.net")).toBe("5548999999999@s.whatsapp.net");
    });

    it("preserves @lid suffix", () => {
      expect(toWhatsAppJid("12345@lid")).toBe("12345@lid");
    });
  });

  describe("normaliseMessage", () => {
    it("parses text conversation", () => {
      const result = normaliseMessage({
        key: { id: "m1", remoteJid: "5548999@s.whatsapp.net", fromMe: false },
        pushName: "Test",
        message: { conversation: "hello" },
      });

      expect(result).toMatchObject({
        phone: "5548999@s.whatsapp.net",
        contactName: "Test",
        direction: "inbound",
        type: "text",
        content: "hello",
        evolutionMsgId: "m1",
      });
    });

    it("parses extendedTextMessage", () => {
      const result = normaliseMessage({
        key: { id: "m2", remoteJid: "5548@s.whatsapp.net", fromMe: false },
        message: { extendedTextMessage: { text: "quoted reply" } },
      });

      expect(result?.type).toBe("text");
      expect(result?.content).toBe("quoted reply");
    });

    it("parses imageMessage with caption", () => {
      const result = normaliseMessage({
        key: { id: "m3", remoteJid: "55@s.whatsapp.net", fromMe: false },
        message: {
          imageMessage: {
            url: "https://cdn.test/img.jpg",
            mimetype: "image/jpeg",
            caption: "look at this",
          },
        },
      });

      expect(result).toMatchObject({
        type: "image",
        content: "look at this",
        mediaUrl: "https://cdn.test/img.jpg",
        mediaMimeType: "image/jpeg",
      });
    });

    it("parses videoMessage", () => {
      const result = normaliseMessage({
        key: { id: "m4", remoteJid: "55@s.whatsapp.net", fromMe: false },
        message: {
          videoMessage: {
            url: "https://cdn.test/vid.mp4",
            mimetype: "video/mp4",
          },
        },
      });

      expect(result?.type).toBe("video");
      expect(result?.mediaUrl).toBe("https://cdn.test/vid.mp4");
    });

    it("parses audioMessage", () => {
      const result = normaliseMessage({
        key: { id: "m5", remoteJid: "55@s.whatsapp.net", fromMe: false },
        message: {
          audioMessage: {
            url: "https://cdn.test/aud.ogg",
            mimetype: "audio/ogg",
          },
        },
      });

      expect(result?.type).toBe("audio");
      expect(result?.content).toBeNull();
      expect(result?.mediaUrl).toBe("https://cdn.test/aud.ogg");
    });

    it("parses documentMessage", () => {
      const result = normaliseMessage({
        key: { id: "m6", remoteJid: "55@s.whatsapp.net", fromMe: false },
        message: {
          documentMessage: {
            url: "https://cdn.test/doc.pdf",
            mimetype: "application/pdf",
            fileName: "report.pdf",
          },
        },
      });

      expect(result?.type).toBe("document");
      expect(result?.content).toBe("report.pdf");
    });

    it("parses stickerMessage", () => {
      const result = normaliseMessage({
        key: { id: "m7", remoteJid: "55@s.whatsapp.net", fromMe: false },
        message: {
          stickerMessage: {
            url: "https://cdn.test/stk.webp",
            mimetype: "image/webp",
          },
        },
      });

      expect(result?.type).toBe("sticker");
    });

    it("parses locationMessage", () => {
      const result = normaliseMessage({
        key: { id: "m8", remoteJid: "55@s.whatsapp.net", fromMe: false },
        message: {
          locationMessage: {
            degreesLatitude: -23.5505,
            degreesLongitude: -46.6333,
          },
        },
      });

      expect(result?.type).toBe("location");
      expect(result?.content).toBe("-23.5505,-46.6333");
    });

    it("parses contactMessage", () => {
      const result = normaliseMessage({
        key: { id: "m9", remoteJid: "55@s.whatsapp.net", fromMe: false },
        message: {
          contactMessage: {
            displayName: "Ana",
            vcard: "BEGIN:VCARD...",
          },
        },
      });

      expect(result?.type).toBe("contact");
      expect(result?.content).toBe("Ana");
    });

    it("returns null for messages without key", () => {
      expect(normaliseMessage({})).toBeNull();
    });

    it("returns null for group messages", () => {
      const result = normaliseMessage({
        key: { id: "g1", remoteJid: "120363@g.us", fromMe: false },
        message: { conversation: "group msg" },
      });
      expect(result).toBeNull();
    });

    it("returns null for fromMe when allowFromMe=false", () => {
      const result = normaliseMessage({
        key: { id: "fm1", remoteJid: "55@s.whatsapp.net", fromMe: true },
        message: { conversation: "outbound" },
      });
      expect(result).toBeNull();
    });

    it("returns outbound when allowFromMe=true", () => {
      const result = normaliseMessage({
        key: { id: "fm2", remoteJid: "55@s.whatsapp.net", fromMe: true },
        message: { conversation: "outbound" },
      }, true);

      expect(result?.direction).toBe("outbound");
    });
  });

  describe("extractMessagesFromSqsBody", () => {
    it("extracts messages from messages.upsert event", () => {
      const body = JSON.stringify({
        event: "messages.upsert",
        data: [{ key: { id: "1" } }],
      });

      expect(extractMessagesFromSqsBody(body)).toHaveLength(1);
    });

    it("extracts from data.messages array", () => {
      const body = JSON.stringify({
        event: "messages.upsert",
        data: { messages: [{ key: { id: "1" } }] },
      });

      expect(extractMessagesFromSqsBody(body)).toHaveLength(1);
    });

    it("wraps single data object", () => {
      const body = JSON.stringify({
        event: "messages.upsert",
        data: { key: { id: "1" } },
      });

      expect(extractMessagesFromSqsBody(body)).toHaveLength(1);
    });

    it("returns empty for non-upsert events", () => {
      const body = JSON.stringify({ event: "messages.delete", data: [] });
      expect(extractMessagesFromSqsBody(body)).toHaveLength(0);
    });

    it("returns empty for invalid JSON", () => {
      expect(extractMessagesFromSqsBody("not json")).toHaveLength(0);
    });
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// UNIT TESTS: routing.ts
// ═══════════════════════════════════════════════════════════════════════════

describe("routing", () => {
  let detectAgent: typeof import("../src/lib/routing.js").detectAgent;
  let getAgentName: typeof import("../src/lib/routing.js").getAgentName;

  beforeEach(async () => {
    const mod = await import("../src/lib/routing.js");
    detectAgent = mod.detectAgent;
    getAgentName = mod.getAgentName;
  });

  const routingConfig = {
    agents: [
      { name: "suporte", id: "id-suporte", title: "Suporte" },
      { name: "vendas", id: "id-vendas", title: "Vendas" },
    ],
    defaultAgent: "suporte",
    internalContacts: ["5548888"],
    internalDefaultAgent: "vendas",
  };

  it("detects agent by keyword in text", () => {
    const result = detectAgent("preciso falar com vendas por favor", "55@s.whatsapp.net", routingConfig);
    expect(result?.id).toBe("id-vendas");
  });

  it("falls back to default agent when no keyword match", () => {
    const result = detectAgent("olá, preciso de ajuda", "55@s.whatsapp.net", routingConfig);
    expect(result?.id).toBe("id-suporte");
  });

  it("routes internal contacts to internal default agent", () => {
    const result = detectAgent("olá", "5548888777666@s.whatsapp.net", routingConfig);
    expect(result?.id).toBe("id-vendas");
  });

  it("returns null when no agents configured", () => {
    expect(detectAgent("hello", "55@s.whatsapp.net")).toBeNull();
  });

  it("falls back to first agent when default is not found", () => {
    const config = { ...routingConfig, defaultAgent: "nonexistent" };
    const result = detectAgent("olá", "55@s.whatsapp.net", config);
    expect(result?.id).toBe("id-suporte"); // first agent
  });

  it("getAgentName returns name for known agent", () => {
    expect(getAgentName("id-suporte", routingConfig)).toBe("suporte");
  });

  it("getAgentName returns fallback for unknown agent", () => {
    expect(getAgentName("unknown-id", routingConfig)).toBe("Assistente");
  });

  it("getAgentName returns fallback without config", () => {
    expect(getAgentName("any-id")).toBe("Assistente");
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// UNIT TESTS: buffer.ts
// ═══════════════════════════════════════════════════════════════════════════

describe("buffer", () => {
  let addToBuffer: typeof import("../src/lib/buffer.js").addToBuffer;
  let getFlushablePhones: typeof import("../src/lib/buffer.js").getFlushablePhones;
  let flushBuffer: typeof import("../src/lib/buffer.js").flushBuffer;

  beforeEach(async () => {
    const mod = await import("../src/lib/buffer.js");
    addToBuffer = mod.addToBuffer;
    getFlushablePhones = mod.getFlushablePhones;
    flushBuffer = mod.flushBuffer;

    // Flush any leftover state from prior tests
    for (const phone of mod.getFlushablePhones(0)) {
      mod.flushBuffer(phone);
    }
  });

  const msg = (content: string) => ({
    type: "text" as const,
    content,
    contactName: "Test",
    evolutionMsgId: null,
    conversationId: "conv-1",
    mediaUrl: null,
    mediaMimeType: null,
  });

  it("buffers messages per phone", () => {
    addToBuffer("phone-A", msg("hello"));
    addToBuffer("phone-A", msg("world"));
    addToBuffer("phone-B", msg("other"));

    const flushedA = flushBuffer("phone-A");
    const flushedB = flushBuffer("phone-B");

    expect(flushedA).toHaveLength(2);
    expect(flushedA[0].content).toBe("hello");
    expect(flushedA[1].content).toBe("world");
    expect(flushedB).toHaveLength(1);
  });

  it("returns empty for unknown phone", () => {
    expect(flushBuffer("unknown")).toEqual([]);
  });

  it("getFlushablePhones respects debounce window", () => {
    addToBuffer("phone-1", msg("test"));

    // With 1 hour debounce, nothing should be ready
    expect(getFlushablePhones(3600000)).toEqual([]);

    // With 0ms debounce, should be ready immediately
    expect(getFlushablePhones(0)).toContain("phone-1");
  });

  it("clears buffer after flush", () => {
    addToBuffer("phone-X", msg("once"));
    flushBuffer("phone-X");

    expect(flushBuffer("phone-X")).toEqual([]);
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// UNIT TESTS: templates.ts
// ═══════════════════════════════════════════════════════════════════════════

describe("templates", () => {
  let buildIssueTitle: typeof import("../src/lib/templates.js").buildIssueTitle;
  let buildIssueBody: typeof import("../src/lib/templates.js").buildIssueBody;
  let buildCommentBody: typeof import("../src/lib/templates.js").buildCommentBody;

  beforeEach(async () => {
    const mod = await import("../src/lib/templates.js");
    buildIssueTitle = mod.buildIssueTitle;
    buildIssueBody = mod.buildIssueBody;
    buildCommentBody = mod.buildCommentBody;
  });

  it("builds issue title with contact and agent names", () => {
    const title = buildIssueTitle("João", PHONE, { id: "a1", name: "suporte", title: "Suporte" });
    expect(title).toBe("João → suporte");
  });

  it("builds issue title with phone fallback", () => {
    const title = buildIssueTitle(null, PHONE, null);
    expect(title).toBe("5548999999999 → Evolution");
  });

  it("builds issue body with metadata and messages", () => {
    const body = buildIssueBody("João", PHONE, [], [
      { type: "text", content: "Olá!" },
    ]);

    expect(body).toContain("evolution-meta");
    expect(body).toContain("**Contato:** João");
    expect(body).toContain("→ Olá!");
  });

  it("builds issue body with media messages", () => {
    const body = buildIssueBody("João", PHONE, [], [
      { type: "image", content: "foto", mediaUrl: "https://cdn.test/img.jpg" },
      { type: "audio", content: null, mediaUrl: "https://cdn.test/aud.ogg" },
      { type: "document", content: "report.pdf", mediaUrl: "https://cdn.test/doc.pdf" },
      { type: "location", content: "-23.55,-46.63" },
    ]);

    expect(body).toContain("[imagem](https://cdn.test/img.jpg)");
    expect(body).toContain("[áudio](https://cdn.test/aud.ogg)");
    expect(body).toContain("[report.pdf](https://cdn.test/doc.pdf)");
    expect(body).toContain("maps.google.com");
  });

  it("builds comment body with SOURCE_TAG", () => {
    const comment = buildCommentBody([
      { type: "text", content: "nova mensagem" },
    ]);

    expect(comment).toContain(SOURCE_TAG);
    expect(comment).toContain("→ nova mensagem");
  });

  it("builds comment body with media", () => {
    const comment = buildCommentBody([
      { type: "image", content: null, mediaUrl: "https://cdn.test/img.jpg" },
    ]);

    expect(comment).toContain("[imagem](https://cdn.test/img.jpg)");
  });
});

// isInternalComment tests removed — comment forwarding was replaced by tool-only mode.

// ═══════════════════════════════════════════════════════════════════════════
// UNIT TESTS: broadcast filtering in normalise
// ═══════════════════════════════════════════════════════════════════════════

describe("normalise — broadcast filtering", () => {
  let normaliseMessage: typeof import("../src/lib/normalise.js").normaliseMessage;

  beforeEach(async () => {
    const mod = await import("../src/lib/normalise.js");
    normaliseMessage = mod.normaliseMessage;
  });

  it("returns null for status@broadcast messages", () => {
    const result = normaliseMessage({
      key: { id: "b1", remoteJid: "status@broadcast", fromMe: false },
      pushName: "Someone",
      message: { conversation: "status update" },
    });
    expect(result).toBeNull();
  });

  it("returns null for any @broadcast suffix", () => {
    const result = normaliseMessage({
      key: { id: "b2", remoteJid: "list123@broadcast", fromMe: false },
      pushName: "Someone",
      message: { conversation: "broadcast list msg" },
    });
    expect(result).toBeNull();
  });
});
