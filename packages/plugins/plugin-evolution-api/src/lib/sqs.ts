/**
 * AWS SQS polling for Evolution API events.
 */

import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } from "@aws-sdk/client-sqs";
import type { PluginContext } from "@paperclipai/plugin-sdk";

export interface SqsConfig {
  queueUrl: string;
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
}

export async function pollSqs(
  ctx: PluginContext,
  config: SqsConfig,
  maxRounds = 2,
): Promise<Array<{ body: string; receiptHandle: string }>> {
  const client = new SQSClient({
    region: config.region,
    credentials: {
      accessKeyId: config.accessKeyId,
      secretAccessKey: config.secretAccessKey,
    },
  });

  const allMessages: Array<{ body: string; receiptHandle: string }> = [];

  for (let round = 0; round < maxRounds; round++) {
    try {
      const resp = await client.send(
        new ReceiveMessageCommand({
          QueueUrl: config.queueUrl,
          WaitTimeSeconds: 10,
          MaxNumberOfMessages: 10,
        }),
      );

      const messages = resp.Messages ?? [];
      if (messages.length === 0) break; // No more messages

      for (const msg of messages) {
        if (msg.Body && msg.ReceiptHandle) {
          allMessages.push({ body: msg.Body, receiptHandle: msg.ReceiptHandle });
        }
      }
    } catch (err) {
      ctx.logger.error(`SQS poll error: ${err}`);
      break;
    }
  }

  return allMessages;
}

export async function deleteSqsMessage(
  config: SqsConfig,
  receiptHandle: string,
): Promise<void> {
  const client = new SQSClient({
    region: config.region,
    credentials: {
      accessKeyId: config.accessKeyId,
      secretAccessKey: config.secretAccessKey,
    },
  });

  await client.send(
    new DeleteMessageCommand({
      QueueUrl: config.queueUrl,
      ReceiptHandle: receiptHandle,
    }),
  );
}
