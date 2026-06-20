import pathway as pw

class FeaturesSchema(pw.Schema):
    timestamp: int
    ticker: str
    price: float
    volume: float
    rolling_mean: float
    rolling_std: float
    zscore: float
    rsi: float