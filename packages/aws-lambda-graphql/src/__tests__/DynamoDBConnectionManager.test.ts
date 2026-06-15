import {
  deleteConnectionMock,
  deleteMock,
  getMock,
  postToConnectionMock,
  putMock,
  resetAllMocks,
  updateMock,
} from '../__mocks__/aws-sdk-v3';
import { DynamoDBConnectionManager } from '../DynamoDBConnectionManager';
import { ConnectionNotFoundError } from '../errors';
import { computeTTL } from '../helpers';

const subscriptionManager: any = {
  unsubscribeAllByConnectionId: jest.fn(),
};

describe('DynamoDBConnectionManager', () => {
  beforeEach(() => {
    resetAllMocks();
    subscriptionManager.unsubscribeAllByConnectionId.mockReset();
  });

  describe('registerConnection', () => {
    it('registers connection by its connectionId and returns a Connection', async () => {
      const manager = new DynamoDBConnectionManager({
        subscriptions: subscriptionManager,
      });

      await expect(
        manager.registerConnection({ connectionId: 'id', endpoint: '' }),
      ).resolves.toEqual({
        id: 'id',
        data: {
          endpoint: '',
          context: {},
          isInitialized: false,
        },
      });

      expect(putMock as jest.Mock).toHaveBeenCalledTimes(1);
      expect(putMock).toHaveBeenCalledWith(
        expect.objectContaining({
          Item: {
            createdAt: expect.any(String),
            data: {
              context: {},
              endpoint: '',
              isInitialized: false,
            },
            id: 'id',
            ttl: expect.any(Number),
          },
          TableName: 'Connections',
        }),
      );
    });

    it('supports turning off ttl', async () => {
      const manager = new DynamoDBConnectionManager({
        subscriptions: subscriptionManager,
        ttl: false,
      });

      await expect(
        manager.registerConnection({ connectionId: 'id', endpoint: '' }),
      ).resolves.toEqual({
        id: 'id',
        data: {
          endpoint: '',
          context: {},
          isInitialized: false,
        },
      });

      expect(putMock as jest.Mock).toHaveBeenCalledTimes(1);
      expect(putMock).not.toHaveBeenCalledWith(
        expect.objectContaining({
          Item: {
            ttl: expect.any(Number),
          },
          TableName: 'Connections',
        }),
      );
    });
  });

  describe('hydrateConnection', () => {
    const manager = new DynamoDBConnectionManager({
      subscriptions: subscriptionManager,
    });

    it('throws ConnectionNotFoundError if connection is not registered', async () => {
      getMock.mockResolvedValueOnce({ Item: null });

      await expect(manager.hydrateConnection('id')).rejects.toThrowError(
        ConnectionNotFoundError,
      );

      expect(getMock as jest.Mock).toHaveBeenCalledTimes(1);
    });

    it('throws ConnectionNotFoundError if connection is expired', async () => {
      getMock.mockResolvedValueOnce({
        Item: { ttl: computeTTL(-10) },
      });

      await expect(manager.hydrateConnection('id')).rejects.toThrowError(
        ConnectionNotFoundError,
      );

      expect(getMock as jest.Mock).toHaveBeenCalledTimes(1);
    });

    it('hydrates connection', async () => {
      getMock.mockResolvedValueOnce({
        Item: { id: 'id', data: { endpoint: '' } },
      });

      await expect(manager.hydrateConnection('id')).resolves.toEqual({
        id: 'id',
        data: {
          endpoint: '',
        },
      });

      expect(getMock as jest.Mock).toHaveBeenCalledTimes(1);
    });

    it('hydrates connection with retry', async () => {
      getMock
        .mockResolvedValueOnce({})
        .mockResolvedValueOnce({
          Item: { id: 'id', data: { endpoint: '' } },
        });

      await expect(
        manager.hydrateConnection('id', { retryCount: 1 }),
      ).resolves.toEqual({
        id: 'id',
        data: {
          endpoint: '',
        },
      });

      expect(getMock as jest.Mock).toHaveBeenCalledTimes(2);
    });
  });

  describe('setConnectionData', () => {
    const manager = new DynamoDBConnectionManager({
      subscriptions: subscriptionManager,
    });

    it('updates connection data', async () => {
      await expect(
        manager.setConnectionData(
          { context: {}, isInitialized: false },
          { id: 'id', data: { context: {}, isInitialized: false } },
        ),
      ).resolves.toBeUndefined();
      expect(updateMock).toHaveBeenCalledTimes(1);
    });
  });

  describe('sendToConnection', () => {
    const manager = new DynamoDBConnectionManager({
      subscriptions: subscriptionManager,
    });

    it('unregisters connection and all subscriptions if it is stale', async () => {
      const err = new Error();
      (err as any).$metadata = { httpStatusCode: 410 };

      postToConnectionMock.mockRejectedValueOnce(err);

      await expect(
        manager.sendToConnection(
          {
            id: 'id',
            data: { endpoint: '', context: {}, isInitialized: false },
          },
          'stringified data',
        ),
      ).resolves.toBeUndefined();

      expect(postToConnectionMock).toHaveBeenCalledTimes(1);
      expect(deleteMock).toHaveBeenCalledTimes(1);
      expect(
        subscriptionManager.unsubscribeAllByConnectionId,
      ).toHaveBeenCalledTimes(1);
      expect(
        subscriptionManager.unsubscribeAllByConnectionId,
      ).toHaveBeenCalledWith('id');
    });

    it('throws error if unknown error happens', async () => {
      const err = new Error('Unknown error');

      postToConnectionMock.mockRejectedValueOnce(err);

      await expect(
        manager.sendToConnection(
          {
            id: 'id',
            data: { endpoint: '', context: {}, isInitialized: false },
          },
          'stringified data',
        ),
      ).rejects.toThrowError(err);

      expect(postToConnectionMock).toHaveBeenCalledTimes(1);
      expect(deleteMock).not.toHaveBeenCalled();
    });

    it('sends data to connection', async () => {
      postToConnectionMock.mockResolvedValueOnce({});

      await expect(
        manager.sendToConnection(
          {
            id: 'id',
            data: { endpoint: '', context: {}, isInitialized: false },
          },
          'stringified data',
        ),
      ).resolves.toBeUndefined();

      expect(postToConnectionMock).toHaveBeenCalledTimes(1);
      expect(deleteMock).not.toHaveBeenCalled();
    });
  });

  describe('unregisterConnection', () => {
    const manager = new DynamoDBConnectionManager({
      subscriptions: subscriptionManager,
    });

    it('deletes connection', async () => {
      deleteMock.mockResolvedValueOnce({
        Item: { id: 'id', data: { context: {}, isInitialized: false } },
      });

      await expect(
        manager.unregisterConnection({
          id: 'id',
          data: { context: {}, isInitialized: false },
        }),
      ).resolves.toBeUndefined();

      expect(deleteMock).toHaveBeenCalledTimes(1);
    });
  });

  describe('closeConnection', () => {
    const manager = new DynamoDBConnectionManager({
      subscriptions: subscriptionManager,
    });
    it('closes connection', async () => {
      await expect(
        manager.closeConnection({
          id: 'id',
          data: { context: {}, isInitialized: false },
        }),
      ).resolves.toBeUndefined();
      expect(deleteConnectionMock).toHaveBeenCalledTimes(1);
    });
  });
});
