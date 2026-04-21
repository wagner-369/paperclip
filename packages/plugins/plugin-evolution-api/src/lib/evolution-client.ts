/**
 * Evolution API HTTP client — send messages and download media via any configured instance.
 */

import type { PluginContext } from "@paperclipai/plugin-sdk";

export async function sendTextMessage(
  ctx: PluginContext,
  apiUrl: string,
  apiKey: string,
  instance: string,
  phone: string,
  text: string,
): Promise<boolean> {
  // Keep @lid suffix for LID-based numbers; only strip @s.whatsapp.net
  const cleanPhone = phone.includes("@lid") ? phone : phone.replace("@s.whatsapp.net", "");
  const url = `${apiUrl}/message/sendText/${encodeURIComponent(instance)}`;

  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: {
        apikey: apiKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ number: cleanPhone, text }),
    });

    if (resp.ok) {
      return true;
    }
    ctx.logger.error(`Evolution API sendText failed: HTTP ${resp.status}`);
    return false;
  } catch (err) {
    ctx.logger.error(`Evolution API sendText error: ${err}`);
    return false;
  }
}

/**
 * Download media from Evolution API using the message ID.
 * Returns the base64-encoded data and mime type, or null on failure.
 *
 * Evolution API v2 endpoint: GET /chat/getBase64FromMediaMessage/{instance}
 * Body: { message: { key: { id, remoteJid } } }
 */
export async function getMediaBase64(
  apiUrl: string,
  apiKey: string,
  instance: string,
  messageId: string,
  remoteJid: string,
): Promise<{ base64: string; mimetype: string } | null> {
  const url = `${apiUrl}/chat/getBase64FromMediaMessage/${encodeURIComponent(instance)}`;

  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: {
        apikey: apiKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        message: {
          key: {
            id: messageId,
            remoteJid,
          },
        },
      }),
    });

    if (!resp.ok) {
      return null;
    }

    const data = await resp.json() as Record<string, unknown>;
    const base64 = data.base64 as string | undefined;
    const mimetype = data.mimetype as string | undefined;

    if (!base64) return null;

    return { base64, mimetype: mimetype ?? "application/octet-stream" };
  } catch {
    return null;
  }
}

/**
 * Build a data URI from base64 media content.
 */
export function toDataUri(base64: string, mimetype: string): string {
  return `data:${mimetype};base64,${base64}`;
}
