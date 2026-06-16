"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MemoryEventProcessor = void 0;
const iterall_1 = require("iterall");
const ArrayPubSub_1 = require("./ArrayPubSub");
const formatMessage_1 = require("./formatMessage");
const execute_1 = require("./execute");
const protocol_1 = require("./protocol");
// polyfill Symbol.asyncIterator
if (Symbol.asyncIterator === undefined) {
    Symbol.asyncIterator = Symbol.for('asyncIterator');
}
class MemoryEventProcessor {
    createHandler(server) {
        return async function processEvents(events, lambdaContext = {}) {
            const options = await server.createGraphQLServerOptions(events, lambdaContext);
            const { connectionManager, subscriptionManager } = options.$$internal;
            for (const event of events) {
                // iterate over subscribers that listen to this event
                // and for each connection:
                //  - create a schema (so we have subscribers registered in PubSub)
                //  - execute operation from event againt schema
                //  - if iterator returns a result, send it to client
                //  - clean up subscriptions and follow with next page of subscriptions
                //  - if the are no more subscriptions, process next event
                // make sure that you won't throw any errors otherwise dynamo will call
                // handler with same events again
                for await (const subscribers of subscriptionManager.subscribersByEvent(event)) {
                    const promises = subscribers
                        .map(async (subscriber) => {
                        // create PubSub for this subscriber
                        const pubSub = new ArrayPubSub_1.ArrayPubSub([event]);
                        // execute operation by executing it and then publishing the event
                        const iterable = await (0, execute_1.execute)({
                            connectionManager,
                            subscriptionManager,
                            schema: options.schema,
                            event: {},
                            lambdaContext: lambdaContext,
                            context: options.context,
                            connection: subscriber.connection,
                            operation: subscriber.operation,
                            pubSub,
                            registerSubscriptions: false,
                        });
                        if (!(0, iterall_1.isAsyncIterable)(iterable)) {
                            // something went wrong, probably there is an error
                            return Promise.resolve();
                        }
                        const iterator = (0, iterall_1.getAsyncIterator)(iterable);
                        const result = await iterator.next();
                        if (result.value != null) {
                            return connectionManager.sendToConnection(subscriber.connection, (0, formatMessage_1.formatMessage)({
                                id: subscriber.operationId,
                                payload: result.value,
                                type: protocol_1.SERVER_EVENT_TYPES.GQL_DATA,
                            }));
                        }
                        return Promise.resolve();
                    })
                        .map((promise) => promise.catch((e) => console.log(e)));
                    await Promise.all(promises);
                }
            }
        };
    }
}
exports.MemoryEventProcessor = MemoryEventProcessor;
//# sourceMappingURL=MemoryEventProcessor.js.map