import pathway as pw

class PriceSchema(pw.Schema):
    timestamp: int
    ticker: str
    price: float
    volume: float
    source: str