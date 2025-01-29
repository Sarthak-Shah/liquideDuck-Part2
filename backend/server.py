import tornado.ioloop
import tornado.web
import tornado.websocket
from database import init_db, update_cell, get_all_cells, close_db
from redis_stream import publish_update, listen_to_stream
import json


class SpreadsheetWebSocketHandler(tornado.websocket.WebSocketHandler):
    clients = set()

    def open(self):
        SpreadsheetWebSocketHandler.clients.add(self)
        print("WebSocket opened: New client connected.")

    def on_message(self, message):
        try:
            print(f"Received message from client: {message}")
            data = json.loads(message)
            row, column, value = data['row'], data['column'], data['value']

            # Update database
            update_cell(self.application.db_conn, row, column, value)
            print(f"Database updated: row={row}, column={column}, value={value}")

            # Publish the update to Redis
            tornado.ioloop.IOLoop.current().spawn_callback(publish_update, row, column, value)
            print(f"Update published to Redis: row={row}, column={column}, value={value}")
        except Exception as e:
            print(f"Error processing message: {e}")

    def on_close(self):
        SpreadsheetWebSocketHandler.clients.remove(self)
        print("WebSocket closed: Client disconnected.")

    @classmethod
    async def broadcast_update(cls, row, column, value):
        update_message = json.dumps({"row": row, "column": column, "value": value})
        for client in cls.clients:
            try:
                client.write_message(update_message)
            except Exception as e:
                print(f"Error broadcasting update to client: {e}")


# Periodic task to listen to Redis Stream and broadcast updates
async def stream_to_clients():
    print("Listening to Redis Stream for updates...")
    async for update in listen_to_stream():
        row = update["row"]
        column = update["column"]
        value = update["value"]
        print(f"Received update from Redis: row={row}, column={column}, value={value}")
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