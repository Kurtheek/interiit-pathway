import os
import sys
import pathway as pw

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

def get_price_stream(tickers: list) -> pw.Table:
    mode = os.getenv("STREAM_MODE", "simulated")

    if mode == "live":
        from ingestion.polygon_connector import get_live_stream
        print(f"[LIVE] Polygon stream: {tickers}")
        return get_live_stream(tickers)
    else:
        from ingestion.simulated_stream import get_simulated_stream
        print(f"[SIMULATED] Replaying CSV: {tickers}")
        return get_simulated_stream()