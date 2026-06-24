"""

P3 — Data Storage & State Management

PURPOSE
Defines the Pathway schema for the "sanctions_table" produced by P4
(External Data & Compliance) and consumed by P8 (Agent Orchestration →
fraud_agent).

WHAT IS A SANCTIONS LIST?

The OFAC (Office of Foreign Assets Control) SDN (Specially Designated Nationals)
list is a public XML file published by the US Treasury.  It lists individuals,
companies, and countries that US persons/entities are prohibited from doing
business with.

In this pipeline:
  - P4 (sanctions_connector.py) downloads and parses the OFAC XML.
  - It emits rows into sanctions_table using this schema.
  - P8 (fraud_agent.py) joins sanctions_table against incoming trades:
      If a trade involves a counterparty whose name or entity matches a
      sanctions entry, the fraud_agent raises a FRAUD_ALERT signal.

DATA FLOW

  OFAC XML (public URL)
      │
      │ P4: sanctions_connector.py
      ▼
  sanctions_table  (this schema)
      │
      │ P8: fraud_agent.py — join with trade data
      ▼
  FRAUD_ALERT signals → agent_signals_table → P8 orchestrator

NOTE ON LIVE UPDATES

The OFAC list is updated irregularly (usually a few times per week).
P4's connector should poll for updates and emit deletes+inserts when the list
changes.  Pathway's streaming model handles this naturally — when an entity is
removed from the list, Pathway propagates the deletion downstream automatically.
"""

import pathway as pw


class SanctionsSchema(pw.Schema):
    """
    Schema for one entry in the OFAC SDN sanctions list.

    Produced by: P4 (ingestion/sanctions_connector.py)
    Consumed by: P8 (agents/fraud_agent.py)
    """

    # Identifier columns 

    sdn_uid: str
    """
    OFAC's unique identifier for this SDN entry.
    Example: "7905", "36830".
    Use this as the stable join key — names can have multiple spellings.
    """

    entity_name: str
    """
    Primary name of the sanctioned individual or entity.
    Example: "PUTIN, Vladimir Vladimirovich" or "BANK MELLAT".
    P8 normalises this (lowercase, strip punctuation) before matching.
    """

    entity_name_normalized: str
    """
    Lowercase, punctuation-stripped version of entity_name for fuzzy matching.
    Pre-computed by P4 at ingest time so P8 does not repeat the work.
    Example: "putin vladimir vladimirovich"
    """

    # Classification columns 

    entity_type: str
    """
    Type of the sanctioned entity.  One of:
      "individual"   — a natural person
      "entity"       — a company, organisation, or vessel
      "vessel"       — a specific ship (relevant for commodities trading)
      "aircraft"     — a specific aircraft
    """

    program: str
    """
    The sanctions program this entry belongs to.
    Example: "RUSSIA-EO14024", "IRAN", "SDGT" (Global Terrorism).
    A single SDN entry can appear under multiple programs; in that case
    P4 emits one row per program so P8 can filter by program if needed.
    """

    #  Contact / identification columns 

    country: str
    """
    Country associated with the entity (nationality or registration country).
    ISO-3166 two-letter code where available, otherwise full name.
    Example: "RU", "IR", "CN".
    Empty string if not available.
    """

    alias: str
    """
    Pipe-separated list of alternative names / aliases for the entity.
    Example: "GAZPROMBANK|GPB|Газпромбанк"
    P8 also checks aliases when screening trade counterparty names.
    Empty string if no aliases.
    """

    # Metadata columns 

    list_date: str
    """
    Date this entry was added to the SDN list (ISO 8601 string: "YYYY-MM-DD").
    Example: "2022-02-24".
    Used in the audit trail so analysts can see how long an entity has been
    sanctioned.
    """

    last_updated: str
    """
    Timestamp (ISO 8601) of when P4 last fetched and processed this entry.
    Example: "2024-06-20T14:30:00Z".
    Helps detect stale data if the OFAC fetch job fails.
    """

    remarks: str
    """
    Free-text remarks from the OFAC list.  Often contains additional
    identifying information (passport numbers, addresses, vessel details).
    Empty string if not present.
    """


# Lookup helper (for P8 to import) 

def is_sanctioned(entity_name: str, sanctions_table: pw.Table) -> bool:
    """
    IMPORTANT: This is a reference implementation for documentation purposes.
    In Pathway you do NOT call Python functions like this at runtime — you use
    Pathway's declarative join/filter operators.

    P8 should implement the sanctions check as a Pathway join:

        # In agents/fraud_agent.py (P8):
        from schemas.sanctions_schema import SanctionsSchema

        # Normalize incoming counterparty names
        trades_with_normalized = trades_table.select(
            *pw.this,
            counterparty_normalized=pw.apply(normalize_name, pw.this.counterparty)
        )

        # Left join with sanctions table
        flagged = trades_with_normalized.join_left(
            sanctions_table,
            pw.left.counterparty_normalized == pw.right.entity_name_normalized,
        ).select(
            *pw.left,
            is_sanctioned=pw.right.sdn_uid.is_not_none(),
            sanctions_program=pw.right.program,
        )

        # Filter to only flagged trades
        fraud_signals = flagged.filter(pw.this.is_sanctioned)

    This docstring is here so P8 knows the correct Pathway pattern.
    """
    raise NotImplementedError(
        "Use Pathway declarative joins in fraud_agent.py — see docstring above."
    )


# Known OFAC programs (non-exhaustive, for P8 filtering) 
# P8 can filter sanctions_table by program to focus on the most relevant lists.

PROGRAM_RUSSIA = "RUSSIA-EO14024"
PROGRAM_IRAN = "IRAN"
PROGRAM_GLOBAL_TERRORISM = "SDGT"
PROGRAM_NORTH_KOREA = "DPRK"
PROGRAM_CHINA_MILITARY = "CMIC"

# High-priority programs that should always trigger a FRAUD_ALERT regardless
# of signal_strength threshold:
HIGH_PRIORITY_PROGRAMS = {
    PROGRAM_RUSSIA,
    PROGRAM_IRAN,
    PROGRAM_GLOBAL_TERRORISM,
    PROGRAM_NORTH_KOREA,
}
