/**
 * Issue body and comment templates for Evolution API conversations.
 */

import { SOURCE_TAG } from "../constants.js";
import type { Message } from "./db.js";
import type { AgentRoute } from "./routing.js";
import { normalisePhone } from "./normalise.js";

const MEDIA_LABELS: Record<string, string> = {
  image: "imagem",
  video: "vídeo",
  audio: "áudio",
  document: "documento",
  sticker: "sticker",
  location: "localização",
  contact: "contato",
};

function formatMediaContent(type: string, content: string | null, mediaUrl: string | null): string {
  const label = MEDIA_LABELS[type] ?? type;

  if (type === "image" && mediaUrl) {
    const caption = content ? ` — ${content}` : "";
    return `📷 [${label}](${mediaUrl})${caption}`;
  }
  if (type === "video" && mediaUrl) {
    const caption = content ? ` — ${content}` : "";
    return `🎥 [${label}](${mediaUrl})${caption}`;
  }
  if (type === "audio" && mediaUrl) {
    return `🎤 [${label}](${mediaUrl})`;
  }
  if (type === "document" && mediaUrl) {
    const fileName = content ?? label;
    return `📎 [${fileName}](${mediaUrl})`;
  }
  if (type === "sticker" && mediaUrl) {
    return `🏷️ [${label}](${mediaUrl})`;
  }
  if (type === "location") {
    if (content) {
      const [lat, lng] = content.split(",");
      return `📍 [${label}](https://maps.google.com/?q=${lat},${lng})`;
    }
    return `📍 [${label}]`;
  }
  if (type === "contact") {
    return `👤 ${content ?? label}`;
  }

  // Text or fallback
  return content ?? `[${label}]`;
}

export function buildIssueTitle(
  contactName: string | null,
  phone: string,
  agent: AgentRoute | null,
): string {
  const name = contactName ?? normalisePhone(phone);
  const agentLabel = agent?.name ?? "Evolution";
  return `${name} → ${agentLabel}`;
}

export function buildIssueBody(
  contactName: string | null,
  phone: string,
  history: Message[],
  newMessages: Array<{ type: string; content: string | null; mediaUrl?: string | null }>,
  channel = "whatsapp",
): string {
  const name = contactName ?? normalisePhone(phone);

  let body = `<!-- evolution-meta
phone: ${phone}
contact_name: ${name}
channel: ${channel}
-->

**Contato:** ${name} (${phone})
**Canal:** ${channel}

`;

  // History
  if (history.length > 0) {
    body += "─── Histórico ────────────────\n";
    for (const msg of history) {
      const ts = msg.createdAt instanceof Date
        ? msg.createdAt.toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" })
        : "??:??";
      const arrow = msg.direction === "inbound" ? "→" : "←";
      const text = formatMediaContent(msg.messageType, msg.content, msg.mediaUrl);
      body += `  [${ts}] ${arrow} ${text}\n`;
    }
    body += "────────────────────────────\n\n";
  }

  // New messages
  body += "─── Mensagens novas ────────────\n";
  for (const msg of newMessages) {
    const text = formatMediaContent(msg.type, msg.content, msg.mediaUrl ?? null);
    body += `  → ${text}\n`;
  }
  body += "────────────────────────────\n";

  return body;
}

export function buildCommentBody(
  newMessages: Array<{ type: string; content: string | null; mediaUrl?: string | null }>,
): string {
  const now = new Date().toLocaleString("pt-BR", {
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });

  const lines = [
    SOURCE_TAG,
    `**Nova(s) mensagem(ns) — ${now}:**\n`,
  ];

  for (const msg of newMessages) {
    const text = formatMediaContent(msg.type, msg.content, msg.mediaUrl ?? null);
    lines.push(`→ ${text}`);
  }

  return lines.join("\n");
}
