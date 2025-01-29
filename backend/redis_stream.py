import redis
import asyncio

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
STREAM_NAME = "spreadsheet_updates"

redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


async def publish_update(row, column, value):
    try:
        print(f"Publishing update to Redis: row={row}, column={column}, value={value}")
        # Publish the message to the Redis Stream
        message_id = redis_client.xadd(STREAM_NAME, {"row": row, "column": column, "value": value})
        print(f"Message published with ID: {message_id}")
    except Exception as e:
        print(f"Error publishing to Redis: {e}")


async def listen_to_stream():
    print("Starting to listen to Redis Stream...")
    last_id = "$"  # Start listening from the latest message
    while True:
        try:
            print(f"Listening for new messages from ID: {last_id}")
            # Use xread with a blocking call to wait for new messages
            messages = redis_client.xread({STREAM_NAME: last_id}, count=1, block=1000)
            if messages:
                print(f"Received messages: {messages}")
                for stream, updates in messages:
                    for update_id, update in updates:
                        print(f"Processing update: {update}")
                        yield update
                        last_id = update_id  # Update the last processed message ID
            else:
                print("No new messages received")
        except Exception as e:
            print(f"Error listening to Redis Stream: {e}")
            await asyncio.sleep(1)  # Wait before retrying
