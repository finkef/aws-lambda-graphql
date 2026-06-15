import { putMock, resetAllMocks } from '../__mocks__/aws-sdk-v3';
import { DynamoDBEventStore } from '../DynamoDBEventStore';

describe('DynamoDBEventStore', () => {
  beforeEach(() => {
    resetAllMocks();
  });

  it('works correctly', async () => {
    const eventStore = new DynamoDBEventStore();

    await expect(
      eventStore.publish({
        event: 'test',
        payload: {
          custom: true,
        },
      }),
    ).resolves.toBeUndefined();

    expect(putMock).toHaveBeenCalledTimes(1);
    expect(putMock).toHaveBeenCalledWith(
      expect.objectContaining({
        TableName: 'Events',
        Item: expect.objectContaining({
          id: expect.stringMatching(/^[A-Z0-9]{26}$/),
          event: 'test',
          payload: {
            custom: true,
          },
          ttl: expect.any(Number),
        }),
      }),
    );
  });

  it('supports turning off the TTL', async () => {
    const eventStore = new DynamoDBEventStore({
      ttl: false,
    });

    await expect(
      eventStore.publish({
        event: 'test',
        payload: {
          custom: true,
        },
      }),
    ).resolves.toBeUndefined();

    expect(putMock).toHaveBeenCalledTimes(1);
    expect(putMock).not.toHaveBeenCalledWith(
      expect.objectContaining({
        Item: expect.objectContaining({
          ttl: expect.any(Number),
        }),
      }),
    );
  });
});
