/**
 * Poll Evolution API directly for new messages (fallback when SQS is unavailable).
 * Uses native Node fetch to avoid plugin SSRF restrictions on custom headers.
 */

export async function pollEvolutionMessages(
  apiUrl: string,
  apiKey: string,
  instance: string,
  limit = 20,
): Promise<Record<string, unknown>[]> {
  const url = `${apiUrl}/chat/findMessages/${encodeURIComponent(instance)}`;

  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: {
        apikey: apiKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        where: { key: { fromMe: false } },
        limit,
      }),
    });

    if (!resp.ok) {
      return [];
    }

    const data = await resp.json() as Record<string, unknown>;
    const messages = data.messages as Record<string, unknown> | undefined;
    const records = messages?.records;
    if (Array.isArray(records)) return records;
    if (Array.isArray(data)) return data;

    return [];
  } catch (err) {
    console.error(`[evolution-poll] fetch error: ${err}`);
    return [];
  }
}

/**
 * Convert Evolution API findMessages record to the same shape as SQS/webhook payload.
 */
export function convertPollRecord(record: Record<string, unknown>): Record<string, unknown> {
  // findMessages returns { key, pushName, message, messageType, messageTimestamp, ... }
  // normalise.ts expects the same shape
  return {
    key: record.key,
    pushName: record.pushName,
    message: record.message,
    messageTimestamp: record.messageTimestamp,
  };
}
