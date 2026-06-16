"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DynamoDBEventProcessor = void 0;
const util_dynamodb_1 = require("@aws-sdk/util-dynamodb");
const iterall_1 = require("iterall");
const ArrayPubSub_1 = require("./ArrayPubSub");
const formatMessage_1 = require("./formatMessage");
const execute_1 = require("./execute");
const protocol_1 = require("./protocol");
const isTTLExpired_1 = require("./helpers/isTTLExpired");
/**
 * DynamoDBEventProcessor
 *
 * Processes DynamoDB stream event in order to send events to subscribed clients
 */
class DynamoDBEventProcessor {
    constructor(options = {}) {
        this.log = options.log || console.log;
        this.onError = options.onError || ((err) => this.log(err));
        this.debug = options.debug || false;
    }
    createHandler(server) {
        return async (lambdaEvent, lambdaContext) => {
            const connectionManager = server.getConnectionManager();
            const subscriptionManager = server.getSubscriptionManager();
            const { Records } = lambdaEvent;
            for (const record of Records) {
                // process only INSERT events
                if (record.eventName !== 'INSERT') {
                    continue;
                }
                // now construct event from dynamodb image
                const event = (0, util_dynamodb_1.unmarshall)(record.dynamodb.NewImage);
                // skip if event is expired
                if ((0, isTTLExpired_1.isTTLExpired)(event.ttl)) {
                    if (this.debug)
                        this.log('Discarded event : TTL expired', event);
                    continue;
                }
                if (this.debug)
                    this.log('Processing event', event);
                // iterate over subscribers that listen to this event
                // and for each connection:
                //  - create a schema (so we have subscribers registered in PubSub)
                //  - execute operation from event againt schema
                //  - if iterator returns a result, send it to client
                //  - clean up subscriptions and follow with next page of subscriptions
                //  - if they are no more subscriptions, process next event
                // make sure that you won't throw any errors otherwise dynamo will call
                // handler with same events again
                for await (const subscribers of subscriptionManager.subscribersByEvent(event)) {
                    const promises = subscribers
                        .map(async (subscriber) => {
                        // create PubSub for this subscriber
                        const pubSub = new ArrayPubSub_1.ArrayPubSub([event]);
                        const options = await server.createGraphQLServerOptions(lambdaEvent, lambdaContext, {
                            // this allows createGraphQLServerOptions() to append more extra data
                            // to context from connection.data.context
                            connection: subscriber.connection,
                            operation: subscriber.operation,
                            pubSub,
                        });
                        // execute operation by executing it and then publishing the event
                        const iterable = await (0, execute_1.execute)({
                            connectionManager,
                            subscriptionManager,
                            schema: options.schema,
                            event: lambdaEvent,
                            lambdaContext,
                            context: options.context,
                            connection: subscriber.connection,
                            operation: subscriber.operation,
                            pubSub,
                            registerSubscriptions: false,
                        });
                        if (!(0, iterall_1.isAsyncIterable)(iterable)) {
                            // something went wrong, probably there is an error
                            this.log('Execution result: non iterable', event);
                            return Promise.resolve();
                        }
                        const iterator = (0, iterall_1.getAsyncIterator)(iterable);
                        const result = await iterator.next();
                        if (result.value != null) {
                            if (this.debug)
                                this.log('Send event ', result);
                            return connectionManager.sendToConnection(subscriber.connection, (0, formatMessage_1.formatMessage)({
                                id: subscriber.operationId,
                                payload: result.value,
                                type: protocol_1.SERVER_EVENT_TYPES.GQL_DATA,
                            }));
                        }
                        return Promise.resolve();
                    })
                        .map((promise) => promise.catch(this.onError));
                    await Promise.all(promises);
                }
            }
        };
    }
}
exports.DynamoDBEventProcessor = DynamoDBEventProcessor;
//# sourceMappingURL=DynamoDBEventProcessor.js.map