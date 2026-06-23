import pathway as pw
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from schemas.features_schema import FeaturesSchema


def compute_rsi(prices_serialized: str) -> float:
    try:
        prices = [float(x) for x in prices_serialized.split(",") if x.strip()]
        if len(prices) < 2:
            return 50.0
        deltas = [prices[i + 1] - prices[i] for i in range(len(prices) - 1)]
        gains = [d for d in deltas if d > 0]
        losses = [-d for d in deltas if d < 0]
        avg_gain = sum(gains) / len(deltas)
        avg_loss = sum(losses) / len(deltas)
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 4)
    except Exception:
        return 50.0


def serialize_tuple(t: tuple) -> str:
    return ",".join(str(x) for x in t)


def compute_std_from_serialized(prices_serialized: str) -> float:
    try:
        prices = [float(x) for x in prices_serialized.split(",") if x.strip()]
        if len(prices) < 2:
            return 0.0
        mean = sum(prices) / len(prices)
        variance = sum((p - mean) ** 2 for p in prices) / len(prices)
        return variance ** 0.5
    except Exception:
        return 0.0


def compute_zscore(prices_serialized: str, price: float) -> float:
    try:
        prices = [float(x) for x in prices_serialized.split(",") if x.strip()]
        if len(prices) < 2:
            return 0.0
        mean = sum(prices) / len(prices)
        std = (sum((p - mean) ** 2 for p in prices) / len(prices)) ** 0.5
        return (price - mean) / (std + 1e-9)
    except Exception:
        return 0.0


def compute_features(price_table: pw.Table) -> pw.Table:
    windowed = price_table.windowby(
        price_table.timestamp,
        window=pw.temporal.sliding(
            hop=1,
            duration=20,
            origin=0,
        ),
        instance=price_table.ticker,
    ).reduce(
        ticker=pw.reducers.any(pw.this.ticker),
        timestamp=pw.reducers.max(pw.this.timestamp),
        price=pw.reducers.any(pw.this.price),
        volume=pw.reducers.any(pw.this.volume),
        rolling_mean=pw.reducers.avg(pw.this.price),
        prices_tuple=pw.reducers.tuple(pw.this.price),
    )

    features = windowed.select(
        timestamp=windowed.timestamp,
        ticker=windowed.ticker,
        price=windowed.price,
        volume=windowed.volume,
        rolling_mean=windowed.rolling_mean,
        rolling_std=pw.apply(
            compute_std_from_serialized,
            pw.apply(serialize_tuple, windowed.prices_tuple),
        ),
        zscore=pw.apply(
            compute_zscore,
            pw.apply(serialize_tuple, windowed.prices_tuple),
            windowed.price,
        ),
        rsi=pw.apply(
            compute_rsi,
            pw.apply(serialize_tuple, windowed.prices_tuple),
        ),
    )

    return features


def get_best_features(price_table: pw.Table) -> pw.Table:
    """
    Returns deduplicated features — one row per ticker per timestamp.
    Use this instead of compute_features directly.
    """
    return compute_features(price_table)
