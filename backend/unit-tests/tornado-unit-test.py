import asyncio
import unittest
from tornado.ioloop import IOLoop
import redis.asyncio as aioredis
import tornado.testing
import tornado.websocket
import json

import os
import sys

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server import make_app, SpreadsheetWebSocketHandler, stream_to_clients


class TestWebSocket(tornado.testing.AsyncHTTPTestCase):
    def get_app(self):
        """Creates and returns the Tornado application."""
        app = make_app()
        IOLoop.current().spawn_callback(stream_to_clients)   # Start Redis Stream Listener
        return app

    async def wait_for_redis_update(self, redis_client, expected_id):
        """Waits for a message to appear in Redis Stream."""
        for _ in range(10):  # Retry for 5 seconds (10 * 0.5s)
            response = await redis_client.xrange("spreadsheet_updates", "-", "+")
            if response and response[-1][0] != expected_id:  # Ensure new message ID
                print(f"✅ New message found in Redis: {response[-1]}")
                return response[-1]
            await asyncio.sleep(0.5)  # Wait before checking again
        raise AssertionError("❌ Redis did not receive the expected message!")

    @tornado.testing.gen_test
    async def test_websocket(self):
        """Tests WebSocket message sending & receiving."""
        ws_url = f"ws://localhost:{self.get_http_port()}/websocket"
        ws = await tornado.websocket.websocket_connect(ws_url)
        print("✅ WebSocket connected")

        # Create a Redis client using from_url
        redis_client = await aioredis.from_url("redis://localhost:6379", decode_responses=True)

        test_message = {"row": 1, "column": 1, "value": "Test"}
        await ws.write_message(json.dumps(test_message))
        print("📤 Message sent:", test_message)

        # Wait for WebSocket response
        response = await ws.read_message()
        assert response is not None, "❌ No response received from WebSocket!"

        received_data = json.loads(response)
        print("📥 WebSocket received:", received_data)

        self.assertEqual(received_data, test_message)

        await redis_client.close()


if __name__ == "__main__":
    unittest.main()
