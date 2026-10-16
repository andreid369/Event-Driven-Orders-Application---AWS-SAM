'use strict';

/**
 * GetOrder - GET /orders/{orderId}
 * Because order creation is asynchronous (the API answers 202), this is how a client checks progress:
 *   404                 not processed yet (or unknown)
 *   status PROCESSING   claimed by CreateOrder
 *   status COMPLETED    stored + OrderCreated event published; "fulfilment" is filled once FulfilOrder ran.
 */

const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand } = require('@aws-sdk/lib-dynamodb');

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const { TABLE_NAME, FULFILMENT_TABLE } = process.env;

const response = (statusCode, body) => ({
  statusCode,
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(body),
});

exports.handler = async (event) => {
  const orderId = event.pathParameters && event.pathParameters.orderId;
  if (!orderId || !/^[A-Za-z0-9-]{8,64}$/.test(orderId)) return response(400, { message: 'invalid orderId' });

  try {
    const [order, fulfilment] = await Promise.all([
      ddb.send(new GetCommand({ TableName: TABLE_NAME, Key: { orderId } })),
      ddb.send(new GetCommand({ TableName: FULFILMENT_TABLE, Key: { orderId } })),
    ]);
    if (!order.Item) return response(404, { message: 'order not found (not processed yet?)' });

    const { lockExpiresAt, ...visible } = order.Item;
    return response(200, { ...visible, fulfilment: fulfilment.Item || null });
  } catch (err) {
    console.error('GetOrder failed', err);
    return response(500, { message: 'internal error' });
  }
};
