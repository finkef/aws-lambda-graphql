"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DynamoDBRangeSubscriptionManager = void 0;
const assert_1 = __importDefault(require("assert"));
const client_dynamodb_1 = require("@aws-sdk/client-dynamodb");
const lib_dynamodb_1 = require("@aws-sdk/lib-dynamodb");
const helpers_1 = require("./helpers");
const DEFAULT_TTL = 7200;
// polyfill Symbol.asyncIterator
if (Symbol.asyncIterator === undefined) {
    Symbol.asyncIterator = Symbol.for('asyncIterator');
}
/**
 * DynamoDBSubscriptionManager
 *
 * Stores all subsrciptions in Subscriptions and SubscriptionOperations tables (both can be overridden)
 *
 * DynamoDB table structures
 *
 * Subscriptions:
 *  event: primary key (HASH)
 *  subscriptionId: range key (RANGE) - connectionId:operationId (this is always unique per client)
 *
 * SubscriptionOperations:
 *  subscriptionId: primary key (HASH) - connectionId:operationId (this is always unique per client)
 *  event: range key (RANGE)

 */
/** In order to use this implementation you need to use RANGE key for event in serverless.yml */
class DynamoDBRangeSubscriptionManager {
    constructor({ dynamoDbClient, subscriptionsTableName = 'Subscriptions', subscriptionOperationsTableName = 'SubscriptionOperations', ttl = DEFAULT_TTL, getSubscriptionNameFromEvent = (event) => event.event, } = {}) {
        this.subscribersByEvent = (event) => {
            let ExclusiveStartKey;
            let done = false;
            const name = this.getSubscriptionNameFromEvent(event);
            return {
                next: async () => {
                    if (done) {
                        return { value: [], done: true };
                    }
                    const time = Math.round(Date.now() / 1000);
                    const result = await this.db.send(new lib_dynamodb_1.QueryCommand({
                        ExclusiveStartKey,
                        TableName: this.subscriptionsTableName,
                        Limit: 50,
                        KeyConditionExpression: 'event = :event',
                        FilterExpression: '#ttl > :time OR attribute_not_exists(#ttl)',
                        ExpressionAttributeValues: {
                            ':event': name,
                            ':time': time,
                        },
                        ExpressionAttributeNames: {
                            '#ttl': 'ttl',
                        },
                    }));
                    ExclusiveStartKey = result.LastEvaluatedKey;
                    if (ExclusiveStartKey == null) {
                        done = true;
                    }
                    // we store connectionData on subscription too so we don't
                    // need to load data from connections table
                    const value = (result.Items || []);
                    return { value, done: value.length === 0 };
                },
                [Symbol.asyncIterator]() {
                    return this;
                },
            };
        };
        this.subscribe = async (names, connection, operation) => {
            const subscriptionId = this.generateSubscriptionId(connection.id, operation.operationId);
            const ttlField = this.ttl === false || this.ttl == null
                ? {}
                : { ttl: (0, helpers_1.computeTTL)(this.ttl) };
            await this.db.send(new lib_dynamodb_1.BatchWriteCommand({
                RequestItems: {
                    [this.subscriptionsTableName]: names.map((name) => ({
                        PutRequest: {
                            Item: {
                                connection,
                                operation,
                                event: name,
                                subscriptionId,
                                operationId: operation.operationId,
                                ...ttlField,
                            },
                        },
                    })),
                    [this.subscriptionOperationsTableName]: names.map((name) => ({
                        PutRequest: {
                            Item: {
                                subscriptionId,
                                event: name,
                                ...ttlField,
                            },
                        },
                    })),
                },
            }));
        };
        this.unsubscribe = async (subscriber) => {
            const subscriptionId = this.generateSubscriptionId(subscriber.connection.id, subscriber.operationId);
            await this.db.send(new lib_dynamodb_1.TransactWriteCommand({
                TransactItems: [
                    {
                        Delete: {
                            TableName: this.subscriptionsTableName,
                            Key: {
                                event: subscriber.event,
                                subscriptionId,
                            },
                        },
                    },
                    {
                        Delete: {
                            TableName: this.subscriptionOperationsTableName,
                            Key: {
                                subscriptionId,
                                event: subscriber.event,
                            },
                        },
                    },
                ],
            }));
        };
        this.unsubscribeOperation = async (connectionId, operationId) => {
            const operation = await this.db.send(new lib_dynamodb_1.QueryCommand({
                TableName: this.subscriptionOperationsTableName,
                KeyConditionExpression: 'subscriptionId = :id',
                ExpressionAttributeValues: {
                    ':id': this.generateSubscriptionId(connectionId, operationId),
                },
            }));
            if (operation.Items) {
                await this.db.send(new lib_dynamodb_1.BatchWriteCommand({
                    RequestItems: {
                        [this.subscriptionsTableName]: operation.Items.map((item) => ({
                            DeleteRequest: {
                                Key: { event: item.event, subscriptionId: item.subscriptionId },
                            },
                        })),
                        [this.subscriptionOperationsTableName]: operation.Items.map((item) => ({
                            DeleteRequest: {
                                Key: {
                                    subscriptionId: item.subscriptionId,
                                    event: item.event,
                                },
                            },
                        })),
                    },
                }));
            }
        };
        this.unsubscribeAllByConnectionId = async (connectionId) => {
            let cursor;
            do {
                const result = await this.db.send(new lib_dynamodb_1.ScanCommand({
                    TableName: this.subscriptionsTableName,
                    ExclusiveStartKey: cursor,
                    FilterExpression: 'begins_with(subscriptionId, :connection_id)',
                    ExpressionAttributeValues: {
                        ':connection_id': connectionId,
                    },
                    Limit: 12, // Maximum of 25 request items sent to DynamoDB a time
                }));
                const { Items, LastEvaluatedKey } = result;
                if (Items == null || !Items.length) {
                    return;
                }
                await this.db.send(new lib_dynamodb_1.BatchWriteCommand({
                    RequestItems: {
                        [this.subscriptionsTableName]: Items.map((item) => ({
                            DeleteRequest: {
                                Key: { event: item.event, subscriptionId: item.subscriptionId },
                            },
                        })),
                        [this.subscriptionOperationsTableName]: Items.map((item) => ({
                            DeleteRequest: {
                                Key: { subscriptionId: item.subscriptionId, event: item.event },
                            },
                        })),
                    },
                }));
                cursor = LastEvaluatedKey;
            } while (cursor);
        };
        this.generateSubscriptionId = (connectionId, operationId) => {
            return `${connectionId}:${operationId}`;
        };
        assert_1.default.ok(typeof subscriptionOperationsTableName === 'string', 'Please provide subscriptionOperationsTableName as a string');
        assert_1.default.ok(typeof subscriptionsTableName === 'string', 'Please provide subscriptionsTableName as a string');
        assert_1.default.ok(ttl === false || (typeof ttl === 'number' && ttl > 0), 'Please provide ttl as a number greater than 0 or false to turn it off');
        assert_1.default.ok(dynamoDbClient == null || typeof dynamoDbClient === 'object', 'Please provide dynamoDbClient as an instance of DynamoDBDocumentClient or DynamoDBClient');
        this.subscriptionsTableName = subscriptionsTableName;
        this.subscriptionOperationsTableName = subscriptionOperationsTableName;
        // Handle both DynamoDBDocumentClient and DynamoDBClient
        // Always ensure proper marshalling options are applied
        if (dynamoDbClient) {
            if (dynamoDbClient instanceof lib_dynamodb_1.DynamoDBDocumentClient) {
                // If a DynamoDBDocumentClient is passed, warn the user about potential marshalling issues
                console.warn('[aws-lambda-graphql] Warning: DynamoDBDocumentClient passed directly. Please ensure it was created with marshallOptions: { convertClassInstanceToMap: true, removeUndefinedValues: true } to avoid marshalling errors.');
                this.db = dynamoDbClient;
            }
            else {
                this.db = lib_dynamodb_1.DynamoDBDocumentClient.from(dynamoDbClient, {
                    marshallOptions: {
                        convertClassInstanceToMap: true,
                        removeUndefinedValues: true,
                    },
                });
            }
        }
        else {
            this.db = lib_dynamodb_1.DynamoDBDocumentClient.from(new client_dynamodb_1.DynamoDBClient({}), {
                marshallOptions: {
                    convertClassInstanceToMap: true,
                    removeUndefinedValues: true,
                },
            });
        }
        this.ttl = ttl;
        this.getSubscriptionNameFromEvent = getSubscriptionNameFromEvent;
    }
}
exports.DynamoDBRangeSubscriptionManager = DynamoDBRangeSubscriptionManager;
//# sourceMappingURL=DynamoDBRangeSubscriptionManager.js.map