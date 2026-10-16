'use strict';

/**
 * CreateOrder - consumes order messages from the orders SQS queue (step 6).
 *
 *  1. Idempotent create (step 8): the orderId is the idempotency key. A conditional
 *     PutItem (attribute_not_exists) claims the order; only ONE concurrent caller wins.
 *  2. Publishes an OrderCreated event through the EventBridge GLOBAL ENDPOINT (step 9),
 *     or straight to the regional bus when GLOBAL_ENDPOINT_ID is empty (step 1).
 *  3. Marks the order COMPLETED. Duplicates of a COMPLETED order return the stored
 *     result and trigger no side effects.
 *
 * Order record states in DynamoDB:
 *   PROCESSING  claimed, with a lease (lockExpiresAt). If the worker dies, the lease expires
 *               and a later SQS retry takes the order over (optimistic, conditional update).
 *   COMPLETED   order stored AND event published.
 *
 * Partial batch responses: only the failed messages go back to the queue; after 3 failed
 * receives SQS moves them to the dead-letter queue (step 7, redrive policy in template.yaml).
 */

const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand, GetCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { EventBridgeClient, PutEventsCommand } = require('@aws-sdk/client-eventbridge');

const { TABLE_NAME, EVENT_BUS_NAME, GLOBAL_ENDPOINT_ID } = process.env;
const LOCK_MS = Number(process.env.LOCK_SECONDS || 30) * 1000;

// PutEvents through a global endpoint is signed with SigV4A, which the AWS SDK for JavaScript v3
// implements with the AWS Common Runtime. Loading it here fails fast (cold start) if it is missing.
if (GLOBAL_ENDPOINT_ID) require('@aws-sdk/signature-v4-crt');

// Clients are created once per execution environment, outside the handler.
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true },
});
const eventBridge = new EventBridgeClient({});

class RetryableError extends Error {}

const isConditionalFailure = (err) => err && err.name === 'ConditionalCheckFailedException';
const round2 = (n) => Math.round(n * 100) / 100;
const log = (msg, extra = {}) => console.log(JSON.stringify({ msg, ...extra }));

function validate(order) {
  if (!order || typeof order !== 'object') throw new Error('message body must be a JSON object');
  if (typeof order.orderId !== 'string' || !order.orderId) throw new Error('orderId missing');
  if (typeof order.customerId !== 'string' || !order.customerId) throw new Error('customerId missing');
  if (!Array.isArray(order.items) || order.items.length === 0) throw new Error('items missing');
}

async function publishOrderCreated(order) {
  const result = await eventBridge.send(
    new PutEventsCommand({
      ...(GLOBAL_ENDPOINT_ID ? { EndpointId: GLOBAL_ENDPOINT_ID } : {}),
      Entries: [
        {
          EventBusName: EVENT_BUS_NAME, // must exist with the same name in both Regions
          Source: 'orders.service',
          DetailType: 'OrderCreated',
          Detail: JSON.stringify({
            orderId: order.orderId,
            idempotencyKey: order.orderId, // consumers de-duplicate on this
            customerId: order.customerId,
            items: order.items,
            total: order.total,
            createdAt: order.createdAt,
            sourceRegion: process.env.AWS_REGION,
          }),
        },
      ],
    }),
  );
  if (result.FailedEntryCount) {
    const e = result.Entries && result.Entries[0];
    throw new Error(`PutEvents failed: ${e && e.ErrorCode} ${e && e.ErrorMessage}`);
  }
  return result.Entries[0].EventId;
}

// Publishes the event, then flips PROCESSING -> COMPLETED.
async function completeOrder(order) {
  const eventId = await publishOrderCreated(order);
  const now = new Date().toISOString();
  try {
    await ddb.send(
      new UpdateCommand({
        TableName: TABLE_NAME,
        Key: { orderId: order.orderId },
        UpdateExpression: 'SET #s = :completed, completedAt = :now, eventId = :eventId REMOVE lockExpiresAt',
        ConditionExpression: '#s = :processing',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: { ':completed': 'COMPLETED', ':processing': 'PROCESSING', ':now': now, ':eventId': eventId },
      }),
    );
  } catch (err) {
    if (!isConditionalFailure(err)) throw err; // somebody else already completed it: fine
  }
  return { duplicate: false, order: { ...order, status: 'COMPLETED', completedAt: now, eventId } };
}

async function processOrder(input) {
  validate(input);
  const now = Date.now();
  const total = round2(input.items.reduce((sum, i) => sum + Number(i.quantity) * Number(i.price), 0));
  const record = {
    orderId: input.orderId,
    customerId: input.customerId,
    items: input.items,
    total,
    status: 'PROCESSING',
    createdAt: new Date(now).toISOString(),
    lockExpiresAt: now + LOCK_MS,
    attempts: 1,
  };

  // Claim the orderId. The condition makes this race-free: of N concurrent processors, exactly one succeeds.
  try {
    await ddb.send(
      new PutCommand({ TableName: TABLE_NAME, Item: record, ConditionExpression: 'attribute_not_exists(orderId)' }),
    );
    return await completeOrder(record);
  } catch (err) {
    if (!isConditionalFailure(err)) throw err;
  }

  // The orderId already exists -> this is a duplicate (client retry or SQS redelivery).
  const { Item: existing } = await ddb.send(
    new GetCommand({ TableName: TABLE_NAME, Key: { orderId: input.orderId }, ConsistentRead: true }),
  );
  if (!existing) throw new RetryableError('order vanished between conditional write and read');

  if (existing.status === 'COMPLETED') {
    log('duplicate order, returning stored result', { orderId: existing.orderId });
    return { duplicate: true, order: existing };
  }
  if (existing.lockExpiresAt > now) {
    // Another worker is processing this order right now. Do nothing; SQS will redeliver later.
    throw new RetryableError(`order ${existing.orderId} is being processed by another worker`);
  }

  // PROCESSING with an expired lease: the previous worker died. Take over, but only if nobody else did
  // (the condition compares the exact lease value we read).
  const newLock = now + LOCK_MS;
  try {
    await ddb.send(
      new UpdateCommand({
        TableName: TABLE_NAME,
        Key: { orderId: existing.orderId },
        UpdateExpression: 'SET lockExpiresAt = :newLock, attempts = attempts + :one',
        ConditionExpression: '#s = :processing AND lockExpiresAt = :seen',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: { ':newLock': newLock, ':one': 1, ':processing': 'PROCESSING', ':seen': existing.lockExpiresAt },
      }),
    );
  } catch (err) {
    if (isConditionalFailure(err)) throw new RetryableError(`order ${existing.orderId} was taken over by another worker`);
    throw err;
  }
  log('taking over expired order lease', { orderId: existing.orderId });
  return completeOrder({ ...existing, lockExpiresAt: newLock });
}

async function handleRecord(record) {
  const order = JSON.parse(record.body);
  const outcome = await processOrder(order);
  log(outcome.duplicate ? 'order already processed' : 'order created', {
    orderId: outcome.order.orderId,
    status: outcome.order.status,
  });
}

exports.handler = async (event) => {
  const batchItemFailures = [];
  for (const record of event.Records) {
    try {
      await handleRecord(record);
    } catch (err) {
      console.error(JSON.stringify({ msg: 'order processing failed', messageId: record.messageId, error: err.message }));
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }
  return { batchItemFailures };
};

exports._test = { processOrder, RetryableError };
