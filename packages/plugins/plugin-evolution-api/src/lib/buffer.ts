/**
 * In-memory message buffer with debounce.
 * Buffers messages per phone number and flushes when the debounce window expires.
 */

export interface BufferedMessage {
  type: string;
  content: string | null;
  contactName: string | null;
  evolutionMsgId: string | null;
  conversationId: string;
  mediaUrl: string | null;
  mediaMimeType: string | null;
}

interface BufferEntry {
  firstMessageAt: number;
  messages: BufferedMessage[];
}

const buffers = new Map<string, BufferEntry>();

export function addToBuffer(phone: string, msg: BufferedMessage): void {
  const existing = buffers.get(phone);
  if (existing) {
    existing.messages.push(msg);
  } else {
    buffers.set(phone, {
      firstMessageAt: Date.now(),
      messages: [msg],
    });
  }
}

export function getFlushablePhones(debounceMs: number): string[] {
  const now = Date.now();
  const ready: string[] = [];
  for (const [phone, entry] of buffers) {
    if (now - entry.firstMessageAt >= debounceMs) {
      ready.push(phone);
    }
  }
  return ready;
}

export function flushBuffer(phone: string): BufferedMessage[] {
  const entry = buffers.get(phone);
  if (!entry) return [];
  buffers.delete(phone);
  return entry.messages;
}
