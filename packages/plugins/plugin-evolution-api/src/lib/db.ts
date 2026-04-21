/**
 * Direct PostgreSQL access for evolution_conversations and evolution_messages tables.
 * Uses the `postgres` driver for lightweight queries.
 */

import postgres from "postgres";

let sql: ReturnType<typeof postgres> | null = null;

export function getDb(databaseUrl: string) {
  if (!sql) {
    sql = postgres(databaseUrl, { max: 3 });
  }
  return sql;
}

// --- Conversations ---

export interface Conversation {
  id: string;
  companyId: string;
  instanceKey: string;
  phone: string;
  contactName: string | null;
  channel: string;
  issueId: string | null;
  assignedAgentId: string | null;
  status: string;
  lastMessageAt: Date | null;
  createdAt: Date;
}

export async function upsertConversation(
  databaseUrl: string,
  companyId: string,
  instanceKey: string,
  phone: string,
  contactName?: string | null,
): Promise<Conversation> {
  const db = getDb(databaseUrl);
  const [row] = await db`
    INSERT INTO evolution_conversations (company_id, instance_key, phone, contact_name, last_message_at)
    VALUES (${companyId}, ${instanceKey}, ${phone}, ${contactName ?? null}, now())
    ON CONFLICT (company_id, instance_key, phone)
    DO UPDATE SET
      contact_name = COALESCE(EXCLUDED.contact_name, evolution_conversations.contact_name),
      last_message_at = now(),
      updated_at = now()
    RETURNING *
  `;
  return mapConversation(row);
}

export async function getConversationByIssueId(
  databaseUrl: string,
  issueId: string,
): Promise<Conversation | null> {
  const db = getDb(databaseUrl);
  const rows = await db`
    SELECT * FROM evolution_conversations WHERE issue_id = ${issueId} LIMIT 1
  `;
  return rows.length > 0 ? mapConversation(rows[0]) : null;
}

export async function getConversationByPhone(
  databaseUrl: string,
  companyId: string,
  instanceKey: string,
  phone: string,
): Promise<Conversation | null> {
  const db = getDb(databaseUrl);
  const rows = await db`
    SELECT * FROM evolution_conversations
    WHERE company_id = ${companyId} AND instance_key = ${instanceKey} AND phone = ${phone}
    LIMIT 1
  `;
  return rows.length > 0 ? mapConversation(rows[0]) : null;
}

export async function getActiveConversations(
  databaseUrl: string,
  companyId: string,
): Promise<Conversation[]> {
  const db = getDb(databaseUrl);
  const rows = await db`
    SELECT * FROM evolution_conversations
    WHERE company_id = ${companyId} AND status = 'active' AND issue_id IS NOT NULL
    ORDER BY last_message_at DESC
  `;
  return rows.map(mapConversation);
}

export async function updateConversationIssue(
  databaseUrl: string,
  conversationId: string,
  issueId: string,
  agentId?: string,
): Promise<void> {
  const db = getDb(databaseUrl);
  await db`
    UPDATE evolution_conversations
    SET issue_id = ${issueId},
        assigned_agent_id = ${agentId ?? null},
        status = 'active',
        updated_at = now()
    WHERE id = ${conversationId}
  `;
}

export async function updateConversationStatus(
  databaseUrl: string,
  conversationId: string,
  status: string,
): Promise<void> {
  const db = getDb(databaseUrl);
  await db`
    UPDATE evolution_conversations
    SET status = ${status},
        updated_at = now()
    WHERE id = ${conversationId}
  `;
}

function mapConversation(row: Record<string, unknown>): Conversation {
  return {
    id: row.id as string,
    companyId: row.company_id as string,
    instanceKey: row.instance_key as string,
    phone: row.phone as string,
    contactName: row.contact_name as string | null,
    channel: row.channel as string,
    issueId: row.issue_id as string | null,
    assignedAgentId: row.assigned_agent_id as string | null,
    status: row.status as string,
    lastMessageAt: row.last_message_at as Date | null,
    createdAt: row.created_at as Date,
  };
}

// --- Messages ---

export interface Message {
  id: string;
  conversationId: string;
  direction: string;
  messageType: string;
  content: string | null;
  mediaUrl: string | null;
  mediaMimeType: string | null;
  evolutionMsgId: string | null;
  issueId: string | null;
  createdAt: Date;
}

export async function insertMessage(
  databaseUrl: string,
  conversationId: string,
  direction: string,
  messageType: string,
  content: string | null,
  evolutionMsgId: string | null,
  rawPayload?: unknown,
  mediaUrl?: string | null,
  mediaMimeType?: string | null,
): Promise<string | null> {
  const db = getDb(databaseUrl);
  try {
    const [row] = await db`
      INSERT INTO evolution_messages
        (conversation_id, direction, message_type, content, evolution_msg_id, raw_payload, media_url, media_mime_type)
      VALUES
        (${conversationId}, ${direction}, ${messageType}, ${content}, ${evolutionMsgId}, ${rawPayload ? JSON.stringify(rawPayload) : null}::jsonb, ${mediaUrl ?? null}, ${mediaMimeType ?? null})
      ON CONFLICT (evolution_msg_id) DO NOTHING
      RETURNING id
    `;
    return row?.id ?? null;
  } catch {
    return null;
  }
}

export async function getHistory(
  databaseUrl: string,
  conversationId: string,
  limit = 50,
): Promise<Message[]> {
  const db = getDb(databaseUrl);
  const rows = await db`
    SELECT id, conversation_id, direction, message_type, content, media_url, media_mime_type, evolution_msg_id, issue_id, created_at
    FROM evolution_messages
    WHERE conversation_id = ${conversationId}
    ORDER BY created_at DESC
    LIMIT ${limit}
  `;
  return rows.reverse().map((r) => ({
    id: r.id as string,
    conversationId: r.conversation_id as string,
    direction: r.direction as string,
    messageType: r.message_type as string,
    content: r.content as string | null,
    mediaUrl: r.media_url as string | null,
    mediaMimeType: r.media_mime_type as string | null,
    evolutionMsgId: r.evolution_msg_id as string | null,
    issueId: r.issue_id as string | null,
    createdAt: r.created_at as Date,
  }));
}

export async function isMessageSeen(
  databaseUrl: string,
  evolutionMsgId: string,
): Promise<boolean> {
  const db = getDb(databaseUrl);
  const rows = await db`
    SELECT 1 FROM evolution_messages WHERE evolution_msg_id = ${evolutionMsgId} LIMIT 1
  `;
  return rows.length > 0;
}
