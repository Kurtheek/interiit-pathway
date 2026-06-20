import yfinance as yf
import pandas as pd
import pathway as pw
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from schemas.price_schema import PriceSchema

def download_historical(tickers: list, period: str = "2y"):
    frames = []
    for ticker in tickers:
        df = yf.download(ticker, period=period, interval="1d", auto_adjust=True)
        df = df[["Close", "Volume"]].copy()
        df.columns = ["price", "volume"]
        df["ticker"] = ticker
        df["source"] = "simulated"
        df.index.name = "timestamp"
        frames.append(df)

    combined = pd.concat(frames)
    combined = combined.reset_index()
    combined["timestamp"] = (
        pd.to_datetime(combined["timestamp"]).astype(int) // 10**6
    )
    combined = combined[["timestamp", "ticker", "price", "volume", "source"]]
    combined = combined.sort_values("timestamp")

    os.makedirs("data", exist_ok=True)
    combined.to_csv("data/historical_prices.csv", index=False)
    print(f"Saved {len(combined)} rows to data/historical_prices.csv")

def get_simulated_stream() -> pw.Table:
    table = pw.demo.replay_csv(
        "data/historical_prices.csv",
        schema=PriceSchema,
        input_rate=100
    )
    return table

if __name__ == "__main__":
    download_historical(["AAPL", "MSFT", "GOOGL", "JPM", "GS"])