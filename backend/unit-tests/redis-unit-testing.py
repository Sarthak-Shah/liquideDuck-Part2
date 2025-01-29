import unittest
import asyncio
import os, sys

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from redis_stream import publish_update, listen_to_stream

class TestRedisStream(unittest.TestCase):
    def test_publish_and_listen(self):
        async def test():
            # Publish a test message
            await publish_update(1, 1, "Test")

            # Listen for the message
            async for update in listen_to_stream():
                print(f"Received update: {update}")
                self.assertEqual(update, {"row": "1", "column": "1", "value": "Test"})
                break  # Exit after receiving the first message

        # Run the async test
        asyncio.run(test())


if __name__ == "__main__":
    unittest.main()
