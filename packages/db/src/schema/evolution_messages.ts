import { pgTable, uuid, text, timestamp, jsonb, boolean, index, uniqueIndex } from "drizzle-orm/pg-core";
import { evolutionConversations } from "./evolution_conversations.js";

export const evolutionMessages = pgTable(
  "evolution_messages",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    conversationId: uuid("conversation_id").notNull().references(() => evolutionConversations.id),
    direction: text("direction").notNull(),
    messageType: text("message_type").notNull(),
    content: text("content"),
    mediaUrl: text("media_url"),
    mediaMimeType: text("media_mime_type"),
    evolutionMsgId: text("evolution_msg_id"),
    issueId: uuid("issue_id"),
    issueCommentId: uuid("issue_comment_id"),
    rawPayload: jsonb("raw_payload"),
    transcribed: boolean("transcribed").notNull().default(false),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => ({
    convCreatedAtIdx: index("evo_msg_conv_created_at_idx").on(
      table.conversationId,
      table.createdAt,
    ),
    evolutionMsgIdIdx: uniqueIndex("evo_msg_evolution_msg_id_idx").on(table.evolutionMsgId),
  }),
);
