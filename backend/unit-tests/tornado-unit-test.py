import json
import unittest
import tornado.testing
import tornado.websocket
from tornado.ioloop import IOLoop
import os, sys

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server import make_app, SpreadsheetWebSocketHandler


class TestWebSocket(tornado.testing.AsyncHTTPTestCase):
    def get_app(self):
        return make_app()

    def test_websocket(self):
        ws_url = f"ws://localhost:{self.get_http_port()}/websocket"

        async def test():
            # Connect to the WebSocket
            ws = await tornado.websocket.websocket_connect(ws_url)
            print("WebSocket connected")

            # Send a test message
            await ws.write_message(json.dumps({"row": 1, "column": 1, "value": "Test"}))
            print("Message sent")

            # Wait for the broadcasted message
            response = await ws.read_message()
            self.assertEqual(json.loads(response), {"row": 1, "column": 1, "value": "Test"})
            print("Message received")

        IOLoop.current().run_sync(test)


if __name__ == "__main__":
    unittest.main()