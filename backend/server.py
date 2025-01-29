import asyncio

import tornado.ioloop
import tornado.web
import tornado.websocket
from database import init_db, update_cell, get_all_cells, close_db
from redis_stream import publish_update, listen_to_stream
import json


class SpreadsheetWebSocketHandler(tornado.websocket.WebSocketHandler):
    clients = set()

    def open(self):
        """Handles new WebSocket connections."""
        self.clients.add(self)
        print(f"✅ New WebSocket connection: {self.request.remote_ip}")

    def on_message(self, message):
        """Handles incoming WebSocket messages and broadcasts them."""
        print(f"📥 WebSocket received: {message}")
        asyncio.create_task(self.process_message(message))

    async def process_message(self, message):
        """Processes the message and sends it to Redis."""
        redis_client = await aioredis.create_redis_pool("redis://localhost:6379")
        data = json.loads(message)
        await redis_client.xadd("spreadsheet_updates", data)
        redis_client.close()
        await redis_client.wait_closed()
        await self.broadcast_message(message)  # Ensure message is broadcasted

    async def broadcast_message(self, message):
        """Sends the message to all connected clients."""
        print(f"📢 Broadcasting message: {message}")
        for client in self.clients:
            if client.ws_connection and client.ws_connection.stream.socket:
                await client.write_message(message)

    def on_close(self):
        """Handles WebSocket closure."""
        self.clients.remove(self)
        print(f"❌ WebSocket closed: {self.request.remote_ip}")



# Periodic task to listen to Redis Stream and broadcast updates
async def stream_to_clients():
    redis_client = await aioredis.from_url("redis://localhost:6379")

    # Listening to the Redis stream
    while True:
        # Wait for new messages in the stream
        result = await redis_client.xread({'spreadsheet_updates': '0'}, block=0, count=1)
        for stream, messages in result:
            for message in messages:
                # Parse the message
                message_data = message[1]
                row = message_data['row']
                column = message_data['column']
                value = message_data['value']
                # Broadcast the update to clients
                print(f"Broadcasting update to clients: {row}, {column}, {value}")
                await SpreadsheetWebSocketHandler.broadcast_update(row, column, value)


def make_app():
    app = tornado.web.Application([
        (r"/websocket", SpreadsheetWebSocketHandler),
    ])
    app.db_conn = init_db()  # Initialize database connection
    return app


if __name__ == "__main__":
    app = make_app()
    app.listen(8888)

    # Start the Redis Stream listener
    tornado.ioloop.IOLoop.current().spawn_callback(stream_to_clients)

    print("Server started at ws://localhost:8888/websocket")
    try:
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        print("Shutting down server...")
        close_db(app.db_conn)  # Close database connection