# ingestion/news_connector.py
# P4 — External Data & Compliance
# Fetches RSS news feeds from Yahoo Finance for tracked tickers

import pathway as pw
import urllib.request
import xml.etree.ElementTree as ET
import json
import os
from datetime import datetime, timezone


TICKERS = ["AAPL", "MSFT", "GOOGL", "JPM", "GS"]

RSS_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US&count=20"


class NewsSchema(pw.Schema):
    ticker:       str
    guid:         str
    headline:     str
    summary:      str
    url:          str
    published_at: str
    fetched_at:   str


def audit_log(event: str, details: str) -> None:
    log_path = os.path.join(os.path.dirname(__file__), "..", "data", "audit_log.jsonl")
    entry = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "event": event,
        "details": details,
    }
    with open(log_path, "a") as f:
        f.write(json.dumps(entry) + "\n")


def fetch_news_for_ticker(ticker: str) -> list[dict]:
    url = RSS_URL.format(ticker=ticker)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    with urllib.request.urlopen(req) as r:
        raw = r.read()

    root = ET.fromstring(raw)
    channel = root.find("channel")
    if channel is None:
        return []

    rows = []
    for item in channel.findall("item"):
        guid    = (item.findtext("guid") or "").strip()
        title   = (item.findtext("title") or "").strip()
        desc    = (item.findtext("description") or "").strip()
        link    = (item.findtext("link") or "").strip()
        pub     = (item.findtext("pubDate") or "").strip()

        rows.append({
            "ticker":       ticker,
            "guid":         guid,
            "headline":     title,
            "summary":      desc,
            "url":          link,
            "published_at": pub,
            "fetched_at":   fetched_at,
        })

    return rows


def fetch_all_news() -> list[dict]:
    all_rows = []
    for ticker in TICKERS:
        try:
            rows = fetch_news_for_ticker(ticker)
            all_rows.extend(rows)
            audit_log("FETCH_SUCCESS", f"RSS: {len(rows)} articles for {ticker}")
            print(f"[news_connector] {ticker}: {len(rows)} articles")
        except Exception as e:
            print(f"[news_connector] ERROR fetching {ticker}: {e}")
            audit_log("FETCH_ERROR", f"RSS {ticker}: {str(e)}")
    return all_rows


class NewsConnector(pw.io.python.ConnectorSubject):
    def __init__(self, poll_interval_seconds: int = 900):
        super().__init__()
        self.poll_interval = poll_interval_seconds

    def run(self):
        import time
        seen_guids = set()
        while True:
            try:
                print("[news_connector] Fetching RSS feeds...")
                rows = fetch_all_news()
                new_rows = [r for r in rows if r["guid"] not in seen_guids]
                for row in new_rows:
                    self.next(**row)
                    seen_guids.add(row["guid"])
                print(f"[news_connector] Emitted {len(new_rows)} new articles.")
            except Exception as e:
                print(f"[news_connector] ERROR: {e}")
            time.sleep(self.poll_interval)


def get_news_stream(poll_interval_seconds: int = 900) -> pw.Table:
    """
    Entry point for P7 and P8 to import.
    Returns a live Pathway table of news articles, deduplicated by guid.

    Usage:
        from ingestion.news_connector import get_news_stream
        news_table = get_news_stream()
    """
    connector = NewsConnector(poll_interval_seconds=poll_interval_seconds)
    return pw.io.python.read(connector, schema=NewsSchema)