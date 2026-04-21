import { pgTable, uuid, text, timestamp, index, uniqueIndex } from "drizzle-orm/pg-core";
import { companies } from "./companies.js";
import { issues } from "./issues.js";
import { agents } from "./agents.js";

export const evolutionConversations = pgTable(
  "evolution_conversations",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    companyId: uuid("company_id").notNull().references(() => companies.id),
    instanceKey: text("instance_key").notNull(),
    phone: text("phone").notNull(),
    contactName: text("contact_name"),
    channel: text("channel").notNull().default("whatsapp"),
    issueId: uuid("issue_id").references(() => issues.id, { onDelete: "set null" }),
    assignedAgentId: uuid("assigned_agent_id").references(() => agents.id, { onDelete: "set null" }),
    status: text("status").notNull().default("active"),
    lastMessageAt: timestamp("last_message_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => ({
    companyIdx: index("evo_conv_company_idx").on(table.companyId),
    companyPhoneIdx: uniqueIndex("evo_conv_company_instance_phone_idx").on(
      table.companyId,
      table.instanceKey,
      table.phone,
    ),
    issueIdx: index("evo_conv_issue_idx").on(table.issueId),
    statusIdx: index("evo_conv_status_idx").on(table.companyId, table.status),
  }),
);
