'use strict';

/**
 * FulfilOrder - consumes OrderCreated events that EventBridge routed into the fulfilment SQS queue (step 2).
 * Each SQS message body is the complete EventBridge event. The result is stored in the Fulfilment table.
 *
 * Idempotent: events can be delivered more than once (SQS is at-least-once, and with global-endpoint
 * replication an event can also arrive from the other Region), so the write is conditional on the orderId.
 */

const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand } = require('@aws-sdk/lib-dynamodb');

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}), { marshallOptions: { removeUndefinedValues: true } });
const TABLE_NAME = process.env.FULFILMENT_TABLE;

async function fulfil(event) {
  const detail = event && event.detail;
  if (!detail || typeof detail.orderId !== 'string') throw new Error('not an OrderCreated event');

  try {
    await ddb.send(
      new PutCommand({
        TableName: TABLE_NAME,
        Item: {
          orderId: detail.orderId,
          customerId: detail.customerId,
          total: detail.total,
          itemCount: Array.isArray(detail.items) ? detail.items.length : 0,
          status: 'FULFILLED',
          fulfilledAt: new Date().toISOString(),
          eventId: event.id,
          orderRegion: detail.sourceRegion,
          fulfilledInRegion: process.env.AWS_REGION,
        },
        ConditionExpression: 'attribute_not_exists(orderId)',
      }),
    );
    console.log(JSON.stringify({ msg: 'order fulfilled', orderId: detail.orderId }));
  } catch (err) {
    if (err.name !== 'ConditionalCheckFailedException') throw err;
    console.log(JSON.stringify({ msg: 'duplicate event ignored', orderId: detail.orderId }));
  }
}

exports.handler = async (event) => {
  const batchItemFailures = [];
  for (const record of event.Records) {
    try {
      await fulfil(JSON.parse(record.body));
    } catch (err) {
      console.error(JSON.stringify({ msg: 'fulfilment failed', messageId: record.messageId, error: err.message }));
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }
  return { batchItemFailures };
};

exports._test = { fulfil };
