import pathway as pw

class NewsSchema(pw.Schema):
    timestamp: int
    ticker: str
    headline: str
    source_url: str