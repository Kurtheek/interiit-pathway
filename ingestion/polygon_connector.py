import pathway as pw
import websocket
import json
import os
import time
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
from schemas.price_schema import PriceSchema

load_dotenv()

class PolygonConnector(pw.io.python.ConnectorSubject):
    def __init__(self, tickers: list):
        super().__init__()
        self.tickers = tickers
        self.api_key = os.getenv("POLYGON_API_KEY")

    def run(self):
        def on_open(ws):
            print("Connected to Polygon")
            ws.send(json.dumps({"action": "auth", "params": self.api_key}))
            subs = ",".join([f"T.{t}" for t in self.tickers])
            ws.send(json.dumps({"action": "subscribe", "params": subs}))

        def on_message(ws, message):
            events = json.loads(message)
            for event in events:
                if event.get("ev") == "T":
                    self.next(
                        timestamp=event.get("t", int(time.time() * 1000)),
                        ticker=event.get("sym", ""),
                        price=float(event.get("p", 0)),
                        volume=float(event.get("s", 0)),
                        source="polygon"
                    )

        def on_error(ws, error):
            print(f"Polygon error: {error}")

        def on_close(ws, *args):
            print("Polygon WS closed")

        ws = websocket.WebSocketApp(
            "wss://socket.polygon.io/stocks",
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever()

def get_live_stream(tickers: list) -> pw.Table:
    return pw.io.python.read(
        PolygonConnector(tickers),
        schema=PriceSchema
    )