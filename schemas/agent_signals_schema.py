"""


P3 — Data Storage & State Management

PURPOSE

Defines the Pathway schema for the "agent_signals" table.

This schema is the *contract* between:
  - P6 (Streaming ML) — who writes anomaly scores and trade signals
  - P7 (RAG / LLM)    — who writes LLM-generated text rationale
  - P8 (Agent Orchestration) — who READS this table and routes decisions

Think of a schema like a typed database table definition.  In Pathway, every
table has a schema that tells the engine the name and type of each column.
Pathway uses this at pipeline construction time to catch type errors early.

WHY A SEPARATE SCHEMA FILE?

Both P6 and P8 need to agree on the column names and types.  By putting the
schema in one file:
  - There is a single source of truth — no silent mismatches.
  - Both modules import from here rather than copy-pasting column names.
  - If a column is added, it is changed in one place only.

SIGNAL SEMANTICS

Each row in agent_signals_table represents one *decision point*:
  - For a specific (ticker, timestamp) pair.
  - P6 fills in the numeric fields (ml_score, signal_strength, etc.).
  - P7 fills in the text rationale.
  - P8 reads the row and routes it to investment_agent or fraud_agent.

The `signal_type` field acts as the routing key for P8:
  - "BUY" / "SELL" / "HOLD" → routed to investment_agent
  - "FRAUD_ALERT"           → routed to fraud_agent
  - "ANOMALY"               → routed to fraud_agent (for investigation)
"""

import pathway as pw


class AgentSignalSchema(pw.Schema):
    """
    Schema for the agent_signals table.

    Produced by: P6 (ml_score, signal_strength, shap_top_feature) +
                 P7 (rationale, news_citations)
    Consumed by: P8 (orchestrator routes on signal_type)
    """

    # Identity columns 

    timestamp: int
    """
    Day-index integer (1718–1781), matching the integer timestamps used
    throughout the pipeline (see price_schema.py and features_schema.py).
    This is NOT a Unix timestamp.
    """

    ticker: str
    """
    Stock symbol, one of: AAPL, MSFT, GOOGL, JPM, GS.
    """

    # Signal columns (written by P6) 

    signal_type: str
    """
    The primary decision label.  One of:
      "BUY"         — model recommends buying this ticker
      "SELL"        — model recommends selling
      "HOLD"        — no action recommended
      "ANOMALY"     — statistical anomaly detected (z-score extreme)
      "FRAUD_ALERT" — spoofing / wash-trade pattern detected
    P8 uses this field to route to investment_agent vs fraud_agent.
    """

    ml_score: float
    """
    Raw output from the River online-learning model (P6).
    For anomaly detection: probability that this row is anomalous (0.0–1.0).
    For classification: probability of the predicted class.
    """

    signal_strength: float
    """
    Normalised confidence in the signal, on a scale of 0.0 (no confidence)
    to 1.0 (maximum confidence).  Derived from ml_score but clamped and
    scaled so P8 can apply a consistent threshold across signal_types.

    Example: a FRAUD_ALERT with signal_strength < 0.6 might be logged but
    not trigger a human-in-the-loop alert.
    """

    shap_top_feature: str
    """
    Name of the most influential feature for this prediction, from SHAP
    explainability (P6).  E.g. "zscore", "rsi", "rolling_std".
    Included in the alert shown to the human reviewer so they understand
    *why* the model fired.
    """

    shap_top_value: float
    """
    The actual value of shap_top_feature at this (ticker, timestamp).
    Together with shap_top_feature, this lets a human read: "the model
    flagged AAPL because its zscore was 3.7 (well above the normal range)".
    """

    #  Rationale columns (written by P7)

    rationale: str
    """
    LLM-generated natural-language explanation of the signal.  Written by P7
    after querying the vector index and running an LLM prompt.
    Example: "AAPL shows a z-score of 3.7, consistent with unusual selling
    pressure. Recent Reuters article (2024-06-20) reported insider selling."
    Defaults to "" until P7 fills it in (pipeline is eventually consistent).
    """

    news_citations: str
    """
    Comma-separated list of source document paths or headlines that P7 cited
    when generating the rationale.  Used for the audit trail.
    Example: "data/news_docs/reuters_20240620.pdf|paragraph 3"
    Defaults to "" if no relevant news was found.
    """

    # Audit columns 

    source_pipeline: str
    """
    Which sub-pipeline produced this signal.
    One of: "streaming_ml", "fraud_agent", "investment_agent", "combined".
    Helps P10 build the audit trail in the demo.
    """


# Convenience: default values for optional fields 
# P6 writes the numeric fields first; P7 fills in rationale later.
# These defaults are used when constructing rows without RAG output yet.

AGENT_SIGNAL_DEFAULTS = {
    "rationale": "",
    "news_citations": "",
    "source_pipeline": "streaming_ml",
}

# Signal type constants — use these in code instead of raw strings 
# Avoids typos ("BUy" vs "BUY") that would silently break P8's routing logic.

SIGNAL_BUY = "BUY"
SIGNAL_SELL = "SELL"
SIGNAL_HOLD = "HOLD"
SIGNAL_ANOMALY = "ANOMALY"
SIGNAL_FRAUD_ALERT = "FRAUD_ALERT"

INVESTMENT_SIGNALS = {SIGNAL_BUY, SIGNAL_SELL, SIGNAL_HOLD}
FRAUD_SIGNALS = {SIGNAL_ANOMALY, SIGNAL_FRAUD_ALERT}
