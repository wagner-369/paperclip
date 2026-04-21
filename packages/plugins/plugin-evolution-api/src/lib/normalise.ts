/**
 * Normalise Evolution API MESSAGES_UPSERT payloads into a consistent shape.
 */

export type MessageType = "text" | "audio" | "image" | "video" | "document" | "sticker" | "location" | "contact";

export interface NormalisedMessage {
  phone: string;
  contactName: string | null;
  direction: "inbound" | "outbound";
  type: MessageType;
  content: string | null;
  mediaUrl: string | null;
  mediaMimeType: string | null;
  evolutionMsgId: string | null;
  messageTimestamp: number | null;
  raw: unknown;
}

/**
 * Normalise a phone identifier by stripping WhatsApp suffixes.
 * Returns just the numeric phone (e.g. "5548999999999").
 */
export function normalisePhone(phone: string): string {
  return phone
    .replace("@s.whatsapp.net", "")
    .replace("@lid", "")
    .replace("@g.us", "");
}

/**
 * Ensure a phone number has the WhatsApp JID suffix for API calls.
 * Preserves @lid suffix if already present.
 */
export function toWhatsAppJid(phone: string): string {
  if (phone.includes("@")) return phone;
  return `${phone}@s.whatsapp.net`;
}

export function normaliseMessage(raw: Record<string, unknown>, allowFromMe = false): NormalisedMessage | null {
  const key = raw.key as Record<string, unknown> | undefined;
  if (!key) return null;

  const fromMe = Boolean(key.fromMe);
  if (fromMe && !allowFromMe) return null;

  const remoteJid = (key.remoteJid as string) ?? "";

  // Skip groups and broadcast lists (WhatsApp stories/status)
  if (remoteJid.endsWith("@g.us")) return null;
  if (remoteJid === "status@broadcast" || remoteJid.endsWith("@broadcast")) return null;

  const phone = remoteJid;
  const pushName = (raw.pushName as string) ?? null;
  const message = (raw.message as Record<string, unknown>) ?? {};

  // Detect message type, content, and media info
  let type: MessageType = "text";
  let content: string | null = null;
  let mediaUrl: string | null = null;
  let mediaMimeType: string | null = null;

  if (typeof message.conversation === "string") {
    type = "text";
    content = message.conversation;
  } else if (message.extendedTextMessage) {
    type = "text";
    content = (message.extendedTextMessage as Record<string, unknown>).text as string ?? null;
  } else if (message.imageMessage) {
    type = "image";
    const img = message.imageMessage as Record<string, unknown>;
    const caption = img.caption as string | undefined;
    mediaUrl = (img.url as string) ?? (img.directPath as string) ?? null;
    mediaMimeType = (img.mimetype as string) ?? "image/jpeg";
    content = caption ?? null;
  } else if (message.videoMessage) {
    type = "video";
    const vid = message.videoMessage as Record<string, unknown>;
    const caption = vid.caption as string | undefined;
    mediaUrl = (vid.url as string) ?? (vid.directPath as string) ?? null;
    mediaMimeType = (vid.mimetype as string) ?? "video/mp4";
    content = caption ?? null;
  } else if (message.audioMessage) {
    type = "audio";
    const aud = message.audioMessage as Record<string, unknown>;
    mediaUrl = (aud.url as string) ?? (aud.directPath as string) ?? null;
    mediaMimeType = (aud.mimetype as string) ?? "audio/ogg";
    content = null;
  } else if (message.documentMessage) {
    type = "document";
    const doc = message.documentMessage as Record<string, unknown>;
    const fileName = doc.fileName as string | undefined;
    mediaUrl = (doc.url as string) ?? (doc.directPath as string) ?? null;
    mediaMimeType = (doc.mimetype as string) ?? "application/octet-stream";
    content = fileName ?? null;
  } else if (message.stickerMessage) {
    type = "sticker";
    const stk = message.stickerMessage as Record<string, unknown>;
    mediaUrl = (stk.url as string) ?? (stk.directPath as string) ?? null;
    mediaMimeType = (stk.mimetype as string) ?? "image/webp";
    content = null;
  } else if (message.locationMessage) {
    type = "location";
    const loc = message.locationMessage as Record<string, unknown>;
    const lat = loc.degreesLatitude as number | undefined;
    const lng = loc.degreesLongitude as number | undefined;
    content = lat != null && lng != null ? `${lat},${lng}` : null;
  } else if (message.contactMessage) {
    type = "contact";
    const ct = message.contactMessage as Record<string, unknown>;
    content = (ct.displayName as string) ?? (ct.vcard as string) ?? null;
  } else {
    // Unknown message type — try to extract any text
    const txt = (message as Record<string, unknown>).text;
    if (typeof txt === "string") {
      content = txt;
    }
  }

  const msgId = (key.id as string) ?? null;
  const ts = (raw.messageTimestamp as number) ?? null;

  return {
    phone,
    contactName: pushName,
    direction: fromMe ? "outbound" : "inbound",
    type,
    content,
    mediaUrl,
    mediaMimeType,
    evolutionMsgId: msgId,
    messageTimestamp: ts,
    raw,
  };
}

/**
 * Extract messages from an SQS body (Evolution API MESSAGES_UPSERT event).
 */
export function extractMessagesFromSqsBody(body: string): Record<string, unknown>[] {
  try {
    const parsed = JSON.parse(body);

    // Evolution API wraps messages in data.messages or data array
    if (parsed.event === "messages.upsert") {
      const data = parsed.data;
      if (Array.isArray(data)) return data;
      if (data && Array.isArray(data.messages)) return data.messages;
      if (data && typeof data === "object") return [data];
    }

    return [];
  } catch {
    return [];
  }
}
