# ingestion/macro_connector.py
# P4 — External Data & Compliance
import pathway as pw
import urllib.request
import json
import os
from datetime import datetime, timezone

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

SERIES = {
    "FEDFUNDS": "Federal Funds Rate",
    "CPIAUCSL": "Consumer Price Index",
    "GS10":     "10-Year Treasury Rate",
}

def audit_log(event: str, details: str) -> None:
    """Write an audit trail entry to data/audit_log.jsonl"""
    import json, os
    log_path = os.path.join(os.path.dirname(__file__), "..", "data", "audit_log.jsonl")
    entry = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "event": event,
        "details": details,
    }
    with open(log_path, "a") as f:
        f.write(json.dumps(entry) + "\n")

class MacroSchema(pw.Schema):
    series_id:    str
    series_name:  str
    date:         str
    value:        str
    last_updated: str

def fetch_latest_observation(series_id: str, series_name: str) -> dict:
    url = (
        f"{FRED_BASE_URL}?series_id={series_id}"
        f"&api_key={FRED_API_KEY}"
        f"&file_type=json&sort_order=desc&limit=1"
    )
    with urllib.request.urlopen(url) as r:
        data = json.load(r)
    obs = data["observations"][0]
    return {
        "series_id":    series_id,
        "series_name":  series_name,
        "date":         obs["date"],
        "value":        obs["value"],
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

def fetch_macro_entries() -> list[dict]:
    rows = []
    for series_id, series_name in SERIES.items():
        try:
            row = fetch_latest_observation(series_id, series_name)
            rows.append(row)
            audit_log("FETCH_SUCCESS", f"{series_id}: {row['value']} as of {row['date']}")
            print(f"[macro_connector] {series_id}: {row['value']} ({row['date']})")
        except Exception as e:
            print(f"[macro_connector] ERROR fetching {series_id}: {e}")
            audit_log("FETCH_ERROR", f"{series_id}: {str(e)}")
    return rows

class MacroConnector(pw.io.python.ConnectorSubject):
    def __init__(self, poll_interval_seconds: int = 3600):
        super().__init__()
        self.poll_interval = poll_interval_seconds

    def run(self):
        import time
        while True:
            try:
                print("[macro_connector] Fetching FRED macro data...")
                rows = fetch_macro_entries()
                for row in rows:
                    self.next(**row)
                print(f"[macro_connector] Emitted {len(rows)} rows.")
            except Exception as e:
                print(f"[macro_connector] ERROR: {e}")
            time.sleep(self.poll_interval)

def get_macro_stream(poll_interval_seconds: int = 3600) -> pw.Table:
    """
    Entry point for P8 to import.
    Returns a live Pathway table of macro indicators.

    Usage:
        from ingestion.macro_connector import get_macro_stream
        macro_table = get_macro_stream()
    """
    connector = MacroConnector(poll_interval_seconds=poll_interval_seconds)
    return pw.io.python.read(connector, schema=MacroSchema)