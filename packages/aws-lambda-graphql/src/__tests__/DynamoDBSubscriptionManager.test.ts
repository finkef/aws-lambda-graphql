import {
  batchWriteMock,
  queryMock,
  resetAllMocks,
  transactWriteMock,
} from '../__mocks__/aws-sdk-v3';
import { DynamoDBSubscriptionManager } from '../DynamoDBSubscriptionManager';

describe('DynamoDBSubscriptionManager', () => {
  beforeEach(() => {
    resetAllMocks();
  });

  describe('subscribersByEvent', () => {
    it('works correctly for emty query result', async () => {
      const subscriptionManager = new DynamoDBSubscriptionManager();

      queryMock.mockResolvedValueOnce({ Items: [] });

      let pages = 0;

      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      for await (const page of subscriptionManager.subscribersByEvent({
        event: 'test',
        payload: {},
      })) {
        pages++;
      }

      expect(pages).toBe(0);
      expect(queryMock).toHaveBeenCalledTimes(1);
    });

    it('works correctly for non emty query result', async () => {
      const subscriptionManager = new DynamoDBSubscriptionManager();

      queryMock.mockResolvedValueOnce({
        Items: [{}],
        LastEvaluatedKey: {},
      });
      queryMock.mockResolvedValueOnce({
        Items: [{}],
        LastEvaluatedKey: {},
      });
      queryMock.mockResolvedValueOnce({
        Items: [{}],
        LastEvaluatedKey: {},
      });
      queryMock.mockResolvedValueOnce({ Items: [] });

      let pages = 0;

      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      for await (const page of subscriptionManager.subscribersByEvent({
        event: 'test',
        payload: {},
      })) {
        pages++;
      }

      expect(pages).toBe(3);
      expect(queryMock).toHaveBeenCalledTimes(4);
    });
  });

  describe('subscribe', () => {
    it('subscribes correctly', async () => {
      const subscriptionManager = new DynamoDBSubscriptionManager();

      batchWriteMock.mockResolvedValueOnce({});

      await expect(
        subscriptionManager.subscribe(
          ['name1'],
          { id: '1' } as any,
          { operationId: '1' } as any,
        ),
      ).resolves.toBeUndefined();

      expect(batchWriteMock).toHaveBeenCalledTimes(1);
      expect(batchWriteMock).toHaveBeenCalledWith(
        expect.objectContaining({
          RequestItems: {
            SubscriptionOperations: [
              {
                PutRequest: {
                  Item: {
                    event: 'name1',
                    subscriptionId: '1:1',
                    ttl: expect.any(Number),
                  },
                },
              },
            ],
            Subscriptions: [
              {
                PutRequest: {
                  Item: {
                    connection: { id: '1' },
                    event: 'name1',
                    operation: { operationId: '1' },
                    operationId: '1',
                    subscriptionId: '1:1',
                    ttl: expect.any(Number),
                  },
                },
              },
            ],
          },
        }),
      );
    });

    it('supports turning off ttl', async () => {
      const subscriptionManager = new DynamoDBSubscriptionManager({
        ttl: false,
      });

      batchWriteMock.mockResolvedValueOnce({});

      await expect(
        subscriptionManager.subscribe(
          ['name1'],
          { id: '1' } as any,
          { operationId: '1' } as any,
        ),
      ).resolves.toBeUndefined();

      expect(batchWriteMock).toHaveBeenCalledTimes(1);
      expect(batchWriteMock).not.toHaveBeenCalledWith(
        expect.objectContaining({
          RequestItems: {
            SubscriptionOperations: [
              {
                PutRequest: {
                  Item: {
                    ttl: expect.any(Number),
                  },
                },
              },
            ],
            Subscriptions: [
              {
                PutRequest: {
                  Item: {
                    ttl: expect.any(Number),
                  },
                },
              },
            ],
          },
        }),
      );
    });
  });

  describe('unsubscribe', () => {
    it('unsubscribes correctly', async () => {
      const subscriptionManager = new DynamoDBSubscriptionManager();

      transactWriteMock.mockResolvedValueOnce({});

      await expect(
        subscriptionManager.unsubscribe({
          connection: { id: '1' } as any,
          event: 'test',
          operation: {} as any,
          operationId: '1',
        }),
      ).resolves.toBeUndefined();

      expect(transactWriteMock).toHaveBeenCalledTimes(1);
      expect((transactWriteMock as jest.Mock).mock.calls[0][0])
        .toMatchInlineSnapshot(`
        {
          "TransactItems": [
            {
              "Delete": {
                "Key": {
                  "event": "test",
                  "subscriptionId": "1:1",
                },
                "TableName": "Subscriptions",
              },
            },
            {
              "Delete": {
                "Key": {
                  "subscriptionId": "1:1",
                },
                "TableName": "SubscriptionOperations",
              },
            },
          ],
        }
      `);
    });
  });
});
