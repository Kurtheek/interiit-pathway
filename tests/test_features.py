import pathway as pw
import pandas as pd
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.stream_factory import get_price_stream
from ingestion.feature_engineering import compute_features

price_table = get_price_stream(["AAPL", "MSFT", "GOOGL", "JPM", "GS"])
features_table = compute_features(price_table)
pw.io.csv.write(features_table, "data/features_raw.csv")
pw.run()

# Post-process: one clean row per ticker per timestamp
df = pd.read_csv("data/features_raw.csv")
df = df.sort_values("rolling_std", ascending=False)
df = df.drop_duplicates(subset=["ticker", "timestamp"], keep="first")
df = df.sort_values(["ticker", "timestamp"])
df = df[["timestamp", "ticker", "price", "volume", "rolling_mean", "rolling_std", "zscore", "rsi"]]
df.to_csv("data/features_output.csv", index=False)
print(f"Clean features: {df.shape[0]} rows, {df['ticker'].nunique()} tickers")
print(df.head(10).to_string())
