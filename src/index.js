'use strict';

/**
 * Orders Lambda function.
 *   POST /orders                               -> create an order (one DynamoDB row per item)
 *   GET  /orders/{orderId}/items/{itemId}      -> read one item (through DAX when DAX_ENDPOINT is set)
 *
 * Step 6 optimisation: every client is created ONCE at module load (cold start),
 * outside the handler, so warm invocations reuse the connections.
 * The AWS SDK v3 is provided by the Node.js runtime and is kept out of the bundle.
 */

const { randomUUID } = require('node:crypto');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, TransactWriteCommand, GetCommand } = require('@aws-sdk/lib-dynamodb');

// ---------------------------------------------------------------------------
// Initialisation (outside the handler)
// ---------------------------------------------------------------------------
const TABLE_NAME = process.env.TABLE_NAME;
const DAX_ENDPOINT = process.env.DAX_ENDPOINT;

// Writes always go straight to DynamoDB (DAX is used for reads only).
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true },
});

// Reads: DAX when configured. The DAX JavaScript client is built on AWS SDK v2,
// so it is only required (lazily bundled by esbuild) when DAX_ENDPOINT is set.
let daxDocClient = null;
if (DAX_ENDPOINT) {
  const AmazonDaxClient = require('amazon-dax-client');
  const DynamoDB = require('aws-sdk/clients/dynamodb');
  const dax = new AmazonDaxClient({ endpoints: [DAX_ENDPOINT], region: process.env.AWS_REGION });
  daxDocClient = new DynamoDB.DocumentClient({ service: dax });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
const MAX_ITEMS = 25;

const response = (statusCode, body) => ({
  statusCode,
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(body),
});

const isNonEmptyString = (v, max) => typeof v === 'string' && v.trim().length > 0 && v.length <= max;

function parseBody(event) {
  if (!event.body) return null;
  const raw = event.isBase64Encoded ? Buffer.from(event.body, 'base64').toString('utf8') : event.body;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function validateOrder(order) {
  if (!order || typeof order !== 'object') return 'Body must be a JSON object';
  if (!isNonEmptyString(order.customerId, 100)) return 'customerId is required (max 100 chars)';
  if (!Array.isArray(order.items) || order.items.length < 1 || order.items.length > MAX_ITEMS) {
    return `items must be an array with 1 to ${MAX_ITEMS} entries`;
  }
  const seen = new Set();
  for (const item of order.items) {
    if (!item || !isNonEmptyString(item.itemId, 100)) return 'every item needs an itemId (max 100 chars)';
    if (seen.has(item.itemId)) return `duplicate itemId: ${item.itemId}`;
    seen.add(item.itemId);
    if (!Number.isInteger(item.quantity) || item.quantity < 1 || item.quantity > 1000) {
      return `item ${item.itemId}: quantity must be an integer between 1 and 1000`;
    }
    if (typeof item.price !== 'number' || !Number.isFinite(item.price) || item.price < 0) {
      return `item ${item.itemId}: price must be a number >= 0`;
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------
async function createOrder(event) {
  const order = parseBody(event);
  const error = validateOrder(order);
  if (error) return response(400, { message: error });

  const orderId = randomUUID();
  const createdAt = new Date().toISOString();

  // One atomic transaction: either every item of the order is stored, or none.
  await ddb.send(
    new TransactWriteCommand({
      TransactItems: order.items.map((item) => ({
        Put: {
          TableName: TABLE_NAME,
          Item: {
            orderId,
            itemId: item.itemId,
            customerId: order.customerId,
            quantity: item.quantity,
            price: item.price,
            createdAt,
          },
          ConditionExpression: 'attribute_not_exists(orderId)',
        },
      })),
    }),
  );

  return response(201, { orderId, itemCount: order.items.length, createdAt });
}

async function getOrderItem(event) {
  const { orderId, itemId } = event.pathParameters || {};
  if (!isNonEmptyString(orderId, 100) || !isNonEmptyString(itemId, 100)) {
    return response(400, { message: 'orderId and itemId are required' });
  }

  const key = { orderId: decodeURIComponent(orderId), itemId: decodeURIComponent(itemId) };
  const result = daxDocClient
    ? await daxDocClient.get({ TableName: TABLE_NAME, Key: key }).promise()
    : await ddb.send(new GetCommand({ TableName: TABLE_NAME, Key: key }));

  if (!result.Item) return response(404, { message: 'Order item not found' });
  return response(200, { ...result.Item, source: daxDocClient ? 'dax' : 'dynamodb' });
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------
exports.handler = async (event) => {
  try {
    switch (`${event.httpMethod} ${event.resource}`) {
      case 'POST /orders':
        return await createOrder(event);
      case 'GET /orders/{orderId}/items/{itemId}':
        return await getOrderItem(event);
      default:
        return response(404, { message: 'Not found' });
    }
  } catch (err) {
    console.error('Unhandled error', err);
    return response(500, { message: 'Internal server error' });
  }
};
