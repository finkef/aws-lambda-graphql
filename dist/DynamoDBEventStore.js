"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DynamoDBEventStore = void 0;
const assert_1 = __importDefault(require("assert"));
const client_dynamodb_1 = require("@aws-sdk/client-dynamodb");
const lib_dynamodb_1 = require("@aws-sdk/lib-dynamodb");
const ulid_1 = require("ulid");
const helpers_1 = require("./helpers");
const DEFAULT_TTL = 7200;
/**
 * DynamoDB event store
 *
 * This event store stores published events in DynamoDB table
 *
 * The server needs to expose DynamoDBEventProcessor handler in order to process these events
 */
class DynamoDBEventStore {
    constructor({ dynamoDbClient, eventsTable = 'Events', ttl = DEFAULT_TTL, } = {}) {
        this.publish = async (event) => {
            await this.db.send(new lib_dynamodb_1.PutCommand({
                TableName: this.tableName,
                Item: {
                    id: (0, ulid_1.ulid)(),
                    ...event,
                    ...(this.ttl === false || this.ttl == null
                        ? {}
                        : { ttl: (0, helpers_1.computeTTL)(this.ttl) }),
                },
            }));
        };
        assert_1.default.ok(ttl === false || (typeof ttl === 'number' && ttl > 0), 'Please provide ttl as a number greater than 0 or false to turn it off');
        assert_1.default.ok(dynamoDbClient == null || typeof dynamoDbClient === 'object', 'Please provide dynamoDbClient as an instance of DynamoDBDocumentClient or DynamoDBClient');
        assert_1.default.ok(typeof eventsTable === 'string', 'Please provide eventsTable as a string');
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
        this.tableName = eventsTable;
        this.ttl = ttl;
    }
}
exports.DynamoDBEventStore = DynamoDBEventStore;
//# sourceMappingURL=DynamoDBEventStore.js.map