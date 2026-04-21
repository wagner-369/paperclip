/**
 * Evolution API plugin worker.
 *
 * Handles:
 * - SQS polling (scheduled job) for inbound messages
 * - Webhook endpoint for direct Evolution API events
 * - issue.updated event for conversation lifecycle
 * - Agent tools: evolution-send (outbound), evolution-history
 * - Media download and attachment for images, videos, audio, documents, stickers
 *
 * Outbound messages are sent exclusively via the evolution-send tool.
 * Agent comments on issues are internal notes and are NOT forwarded to WhatsApp.
 */

import { definePlugin, runWorker, type PluginContext, type ToolRunContext, type PluginWebhookInput } from "@paperclipai/plugin-sdk";
import { JOB_KEYS, WEBHOOK_KEYS, TOOL_NAMES } from "./constants.js";
import { sendTextMessage, getMediaBase64, toDataUri } from "./lib/evolution-client.js";
import { normaliseMessage, extractMessagesFromSqsBody, normalisePhone, toWhatsAppJid, type NormalisedMessage } from "./lib/normalise.js";
import { detectAgent, getAgentName, type RoutingConfig } from "./lib/routing.js";
import { addToBuffer, getFlushablePhones, flushBuffer, type BufferedMessage } from "./lib/buffer.js";
import { buildIssueTitle, buildIssueBody, buildCommentBody } from "./lib/templates.js";
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
} from "./lib/db.js";
import { pollSqs, deleteSqsMessage, type SqsConfig } from "./lib/sqs.js";
import { pollEvolutionMessages, convertPollRecord } from "./lib/evolution-poll.js";

// --- Config type ---

interface EvolutionConfig {
  evolutionApiUrl: string;
  evolutionApiKey: string;
  evolutionInstance: string;
  sqsQueueUrl?: string;
  awsRegion?: string;
  awsAccessKeyId?: string;
  awsSecretAccessKey?: string;
  databaseUrl: string;
  companyId: string;
  projectId: string;
  debounceMs?: number;
  historyDepth?: number;
  routing?: RoutingConfig;
}

class ConfigValidationError extends Error {
  constructor(missing: string[]) {
    super(`Evolution API plugin: missing required config fields: ${missing.join(", ")}`);
    this.name = "ConfigValidationError";
  }
}

function resolveDbUrl(): string {
  if (process.env.DATABASE_URL) return process.env.DATABASE_URL;
  const port = process.env.PAPERCLIP_EMBEDDED_POSTGRES_PORT ?? "54329";
  return `postgresql://paperclip:paperclip@localhost:${port}/paperclip`;
}

async function resolveConfig(ctx: PluginContext): Promise<EvolutionConfig> {
  const raw = (await ctx.config.get()) as Record<string, unknown>;

  const config: EvolutionConfig = {
    evolutionApiUrl: (raw.evolutionApiUrl as string) ?? "",
    evolutionApiKey: (raw.evolutionApiKey as string) ?? "",
    evolutionInstance: (raw.evolutionInstance as string) ?? "",
    sqsQueueUrl: (raw.sqsQueueUrl as string) ?? undefined,
    awsRegion: (raw.awsRegion as string) ?? "us-east-2",
    awsAccessKeyId: (raw.awsAccessKeyId as string) ?? "",
    awsSecretAccessKey: (raw.awsSecretAccessKey as string) ?? "",
    databaseUrl: resolveDbUrl(),
    companyId: (raw.companyId as string) ?? "",
    projectId: (raw.projectId as string) ?? "",
    debounceMs: (raw.debounceMs as number) ?? 30000,
    historyDepth: (raw.historyDepth as number) ?? 50,
    routing: (raw.routing as RoutingConfig) ?? undefined,
  };

  // Validate required fields
  const missing: string[] = [];
  if (!config.evolutionApiUrl) missing.push("evolutionApiUrl");
  if (!config.evolutionApiKey) missing.push("evolutionApiKey");
  if (!config.evolutionInstance) missing.push("evolutionInstance");
  if (!config.companyId) missing.push("companyId");
  if (!config.projectId) missing.push("projectId");

  if (missing.length > 0) {
    throw new ConfigValidationError(missing);
  }

  return config;
}

// --- Media helper ---

async function resolveMediaUrl(
  config: EvolutionConfig,
  normalised: NormalisedMessage,
): Promise<string | null> {
  // If the normalised message already has a direct URL, use it
  if (normalised.mediaUrl && normalised.mediaUrl.startsWith("http")) {
    return normalised.mediaUrl;
  }

  // Otherwise try to fetch base64 from Evolution API
  if (normalised.evolutionMsgId && normalised.phone) {
    const media = await getMediaBase64(
      config.evolutionApiUrl,
      config.evolutionApiKey,
      config.evolutionInstance,
      normalised.evolutionMsgId,
      normalised.phone,
    );
    if (media) {
      return toDataUri(media.base64, media.mimetype);
    }
  }

  return null;
}

// --- Inbound message processing helper ---

async function processInboundMessage(
  ctx: PluginContext,
  config: EvolutionConfig,
  normalised: NormalisedMessage,
): Promise<void> {
  // Deduplication
  if (normalised.evolutionMsgId && await isMessageSeen(config.databaseUrl, normalised.evolutionMsgId)) {
    return;
  }

  // Resolve media URL for non-text messages
  let mediaUrl = normalised.mediaUrl;
  if (normalised.type !== "text" && normalised.type !== "location" && normalised.type !== "contact") {
    try {
      mediaUrl = await resolveMediaUrl(config, normalised);
    } catch (err) {
      ctx.logger.warn(`Media resolve failed for ${normalised.evolutionMsgId}: ${err}`);
    }
  }

  // Upsert conversation
  const conv = await upsertConversation(
    config.databaseUrl,
    config.companyId,
    config.evolutionInstance,
    normalised.phone,
    normalised.contactName,
  );

  // Store message
  await insertMessage(
    config.databaseUrl,
    conv.id,
    "inbound",
    normalised.type,
    normalised.content,
    normalised.evolutionMsgId,
    normalised.raw,
    mediaUrl,
    normalised.mediaMimeType,
  );

  // Add to buffer
  addToBuffer(normalised.phone, {
    type: normalised.type,
    content: normalised.content,
    contactName: normalised.contactName,
    evolutionMsgId: normalised.evolutionMsgId,
    conversationId: conv.id,
    mediaUrl,
    mediaMimeType: normalised.mediaMimeType,
  });
}

// --- Plugin definition ---

let pluginCtx: PluginContext | null = null;

const plugin = definePlugin({
  async setup(ctx) {
    pluginCtx = ctx;
    // ---------------------------------------------------------------
    // EVENT: issue.updated → update conversation status lifecycle
    // ---------------------------------------------------------------
    ctx.events.on("issue.updated", async (event) => {
      try {
        const config = await resolveConfig(ctx);

        const payload = event.payload as Record<string, unknown>;
        const issueId = (payload.id ?? event.entityId) as string;
        if (!issueId) return;

        const conv = await getConversationByIssueId(config.databaseUrl, issueId);
        if (!conv) return;

        const newStatus = payload.status as string | undefined;
        if (!newStatus) return;

        // Map issue status to conversation status
        if (newStatus === "done" || newStatus === "cancelled") {
          await updateConversationStatus(config.databaseUrl, conv.id, "resolved");
          ctx.logger.info(`Conversation ${conv.id} marked resolved (issue ${newStatus})`);
        } else if (newStatus === "todo" || newStatus === "in_progress") {
          // Re-activate if issue is reopened
          if (conv.status === "resolved") {
            await updateConversationStatus(config.databaseUrl, conv.id, "active");
            ctx.logger.info(`Conversation ${conv.id} reactivated (issue ${newStatus})`);
          }
        }
      } catch (err) {
        if (err instanceof ConfigValidationError) return;
        ctx.logger.error(`Issue updated event handler error: ${err}`);
      }
    });

    // ---------------------------------------------------------------
    // JOB: sqs-poll → poll SQS + flush buffers
    // ---------------------------------------------------------------
    ctx.jobs.register(JOB_KEYS.sqsPoll, async (_job) => {
      try {
      const config = await resolveConfig(ctx);
      ctx.logger.info(`SQS poll job started. sqsUrl=${config.sqsQueueUrl ?? "EMPTY"}`);

      // --- Phase 1: Poll SQS for new messages ---
      if (config.sqsQueueUrl && config.awsAccessKeyId && config.awsSecretAccessKey) {
        const sqsConfig: SqsConfig = {
          queueUrl: config.sqsQueueUrl,
          region: config.awsRegion ?? "us-east-2",
          accessKeyId: config.awsAccessKeyId,
          secretAccessKey: config.awsSecretAccessKey,
        };

        const sqsMessages = await pollSqs(ctx, sqsConfig, 2);
        ctx.logger.info(`SQS poll: ${sqsMessages.length} message(s) received`);

        for (const sqsMsg of sqsMessages) {
          const rawMessages = extractMessagesFromSqsBody(sqsMsg.body);

          for (const raw of rawMessages) {
            const normalised = normaliseMessage(raw as Record<string, unknown>);
            if (!normalised || normalised.direction !== "inbound") continue;

            await processInboundMessage(ctx, config, normalised);
          }

          // Delete from SQS
          try {
            await deleteSqsMessage(sqsConfig, sqsMsg.receiptHandle);
          } catch (err) {
            ctx.logger.error(`SQS delete error: ${err}`);
          }
        }
      }

      // --- Phase 1b: Poll Evolution API directly (fallback / primary) ---
      if (config.evolutionApiUrl && config.evolutionApiKey && config.evolutionInstance) {
        const pollRecords = await pollEvolutionMessages(
          config.evolutionApiUrl,
          config.evolutionApiKey,
          config.evolutionInstance,
          20,
        );

        ctx.logger.info(`Evolution direct poll: ${pollRecords.length} record(s) fetched`);

        let newCount = 0;
        for (const record of pollRecords) {
          const raw = convertPollRecord(record);
          const normalised = normaliseMessage(raw as Record<string, unknown>);
          if (!normalised || normalised.direction !== "inbound") continue;

          // Dedup check before full processing
          if (normalised.evolutionMsgId && await isMessageSeen(config.databaseUrl, normalised.evolutionMsgId)) {
            continue;
          }

          await processInboundMessage(ctx, config, normalised);
          newCount++;
        }

        if (newCount > 0) {
          ctx.logger.info(`Evolution poll: ${newCount} new message(s) from ${pollRecords.length} total`);
        }
      }

      // --- Phase 1c: Sync stale conversations ---
      try {
        const activeConvs = await getActiveConversations(config.databaseUrl, config.companyId);
        for (const conv of activeConvs) {
          if (!conv.issueId) continue;
          try {
            const issue = await ctx.issues.get(conv.issueId, config.companyId);
            if (issue) {
              const issueStatus = (issue as unknown as Record<string, unknown>).status as string;
              if (issueStatus === "done" || issueStatus === "cancelled") {
                await updateConversationStatus(config.databaseUrl, conv.id, "resolved");
                ctx.logger.info(`Stale conversation ${conv.id} resolved (issue ${issueStatus})`);
              }
            }
          } catch {
            await updateConversationStatus(config.databaseUrl, conv.id, "resolved");
          }
        }
      } catch (err) {
        ctx.logger.warn(`Stale conversation sync error: ${err}`);
      }

      // --- Phase 2: Flush ready buffers → create/update issues ---
      const debounceMs = config.debounceMs ?? 30000;
      const readyPhones = getFlushablePhones(debounceMs);

      for (const phone of readyPhones) {
        const messages = flushBuffer(phone);
        if (messages.length === 0) continue;

        try {
          await processMessagePack(ctx, config, phone, messages);
        } catch (err) {
          ctx.logger.error(`Error processing pack for ${phone}: ${err}`);
        }
      }
      } catch (jobErr) {
        if (jobErr instanceof ConfigValidationError) {
          ctx.logger.warn(`SQS poll skipped: ${jobErr.message}`);
          return;
        }
        ctx.logger.error(`SQS poll job FATAL error: ${jobErr}`);
      }
    });

    // ---------------------------------------------------------------
    // TOOL: evolution-send
    // ---------------------------------------------------------------
    ctx.tools.register(
      TOOL_NAMES.send,
      {
        displayName: "Send Message (Evolution API)",
        description: "Send a text message to a phone number via Evolution API.",
        parametersSchema: {
          type: "object" as const,
          properties: {
            phone: { type: "string" as const, description: "Phone number with country code" },
            text: { type: "string" as const, description: "Message text to send" },
          },
          required: ["phone", "text"],
        },
      },
      async (rawParams: unknown, runCtx: ToolRunContext) => {
        const params = rawParams as { phone: string; text: string };
        const config = await resolveConfig(ctx);

        const phone = toWhatsAppJid(params.phone);
        const agentName = getAgentName(runCtx.agentId, config.routing);
        const formattedText = `*${agentName}:*\n${params.text}`;

        const ok = await sendTextMessage(
          ctx, config.evolutionApiUrl, config.evolutionApiKey, config.evolutionInstance,
          phone, formattedText,
        );

        if (ok) {
          const conv = await upsertConversation(config.databaseUrl, config.companyId, config.evolutionInstance, phone);
          await insertMessage(config.databaseUrl, conv.id, "outbound", "text", formattedText, null);
        }

        return { content: ok ? `Message sent to ${params.phone}` : `Failed to send message to ${params.phone}` };
      },
    );

    // ---------------------------------------------------------------
    // TOOL: evolution-history
    // ---------------------------------------------------------------
    ctx.tools.register(
      TOOL_NAMES.history,
      {
        displayName: "Conversation History (Evolution API)",
        description: "Retrieve recent conversation history for a contact.",
        parametersSchema: {
          type: "object" as const,
          properties: {
            phone: { type: "string" as const, description: "Phone number to look up" },
            limit: { type: "number" as const, description: "Max messages (default 20)" },
          },
          required: ["phone"],
        },
      },
      async (rawParams: unknown, _runCtx: ToolRunContext) => {
        const params = rawParams as { phone: string; limit?: number };
        const config = await resolveConfig(ctx);

        const phone = toWhatsAppJid(params.phone);
        const conv = await getConversationByPhone(config.databaseUrl, config.companyId, config.evolutionInstance, phone);
        if (!conv) return { content: `No conversation found for ${params.phone}` };

        const limit = params.limit ?? 20;
        const history = await getHistory(config.databaseUrl, conv.id, limit);

        const formatted = history
          .map((msg) => {
            const arrow = msg.direction === "inbound" ? "→" : "←";
            const ts = msg.createdAt instanceof Date
              ? msg.createdAt.toLocaleString("pt-BR")
              : String(msg.createdAt);
            const mediaTag = msg.mediaUrl ? ` [media: ${msg.mediaMimeType ?? msg.messageType}]` : "";
            return `[${ts}] ${arrow} ${msg.content ?? `[${msg.messageType}]`}${mediaTag}`;
          })
          .join("\n");

        return { content: formatted || "No messages found", data: history };
      },
    );
  },

  async onHealth() {
    return { status: "ok", message: "Evolution API plugin running" };
  },

  async onWebhook(input: PluginWebhookInput) {
    const ctx = pluginCtx;
    if (!ctx) return;

    if (input.endpointKey !== WEBHOOK_KEYS.ingest) return;

    try {
      const config = await resolveConfig(ctx);

      const body = input.rawBody ?? (typeof input.parsedBody === "string" ? input.parsedBody : JSON.stringify(input.parsedBody));
      const rawMessages = extractMessagesFromSqsBody(body);

      ctx.logger.info(`Webhook received: ${rawMessages.length} message(s)`);

      let newCount = 0;
      for (const raw of rawMessages) {
        const normalised = normaliseMessage(raw as Record<string, unknown>);
        if (!normalised || normalised.direction !== "inbound") continue;

        await processInboundMessage(ctx, config, normalised);
        newCount++;
      }

      if (newCount > 0) {
        ctx.logger.info(`Webhook: ${newCount} new inbound message(s) processed`);
      }

      // Immediately flush buffers for webhook-driven messages (lower latency)
      const debounceMs = config.debounceMs ?? 30000;
      const readyPhones = getFlushablePhones(debounceMs);
      for (const phone of readyPhones) {
        const messages = flushBuffer(phone);
        if (messages.length === 0) continue;
        try {
          await processMessagePack(ctx, config, phone, messages);
        } catch (err) {
          ctx.logger.error(`Webhook flush error for ${phone}: ${err}`);
        }
      }
    } catch (err) {
      if (err instanceof ConfigValidationError) return;
      ctx.logger.error(`Webhook handler error: ${err}`);
    }
  },
});

// --- Helper: process a message pack into an issue ---

async function processMessagePack(
  ctx: PluginContext,
  config: EvolutionConfig,
  phone: string,
  messages: BufferedMessage[],
) {
  const conv = await getConversationByPhone(config.databaseUrl, config.companyId, config.evolutionInstance, phone);
  if (!conv) return;

  const contactName = conv.contactName ?? messages[0]?.contactName ?? phone;
  const firstContent = messages.find((m) => m.content)?.content ?? "";
  const targetAgent = detectAgent(firstContent, phone, config.routing);

  // Decide: create new issue or add comment?
  let action: "create" | "comment" = "create";
  const existingIssueId = conv.issueId;

  if (existingIssueId) {
    try {
      const issue = await ctx.issues.get(existingIssueId, config.companyId);
      if (issue) {
        const status = (issue as unknown as Record<string, unknown>).status as string;
        const currentAgent = (issue as unknown as Record<string, unknown>).assigneeAgentId as string;

        if (status === "done" || status === "cancelled") {
          action = "create";
        } else if (targetAgent && currentAgent === targetAgent.id) {
          action = "comment";
        } else {
          action = "create";
        }
      }
    } catch {
      action = "create";
    }
  }

  const newMsgs = messages.map((m) => ({
    type: m.type,
    content: m.content,
    mediaUrl: m.mediaUrl,
  }));

  if (action === "create") {
    const history = await getHistory(config.databaseUrl, conv.id, config.historyDepth ?? 50);
    const title = buildIssueTitle(contactName, phone, targetAgent);
    const body = buildIssueBody(contactName, phone, history, newMsgs);

    const issue = await ctx.issues.create({
      companyId: config.companyId,
      title,
      description: body,
      projectId: config.projectId,
      assigneeAgentId: targetAgent?.id,
    });

    const issueId = (issue as unknown as Record<string, unknown>).id as string;

    // Set status to "todo" so the agent wakeup doesn't skip it
    try {
      await ctx.issues.update(issueId, { status: "todo" }, config.companyId);
    } catch (err) {
      ctx.logger.warn(`Failed to set issue ${issueId} to todo: ${err}`);
    }

    await updateConversationIssue(config.databaseUrl, conv.id, issueId, targetAgent?.id);
    ctx.logger.info(`Issue created: ${issueId} → ${targetAgent?.name ?? "?"} for ${phone}`);

    // Wake up the assigned agent
    if (targetAgent?.id) {
      try {
        await ctx.agents.invoke(targetAgent.id, config.companyId, {
          reason: "issue_assigned",
          prompt: `New WhatsApp message from ${contactName} (${normalisePhone(phone)}). Issue ${issueId} has been assigned to you.`,
        });
        ctx.logger.info(`Agent wakeup: ${targetAgent.name} for issue ${issueId}`);
      } catch (err) {
        ctx.logger.warn(`Agent wakeup failed for ${targetAgent.name}: ${err}`);
      }
    }
  } else if (existingIssueId) {
    const commentBody = buildCommentBody(newMsgs);
    await ctx.issues.createComment(existingIssueId, commentBody, config.companyId);
    ctx.logger.info(`Comment on issue ${existingIssueId} for ${phone}`);

    // Wake up the agent for the new comment
    if (targetAgent?.id) {
      try {
        await ctx.agents.invoke(targetAgent.id, config.companyId, {
          reason: "issue_assigned",
          prompt: `New WhatsApp message from ${contactName} (${normalisePhone(phone)}) on issue ${existingIssueId}.`,
        });
      } catch (err) {
        ctx.logger.warn(`Agent wakeup failed for comment on ${existingIssueId}: ${err}`);
      }
    }
  }
}

export default plugin;
runWorker(plugin, import.meta.url);
