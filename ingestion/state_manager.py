"""
This module provides two things:
  1. get_persistence_backend() — returns a pw.persistence.Backend for use with
     server.run(cache_backend=...) in vector_index.py.
  2. get_persistence_config()  — returns a pw.persistence.Config for use with
     pw.run(persistence_config=...) in any other pipeline entry-point.

WHY TWO SEPARATE FUNCTIONS?

Pathway 0.31.1 has two distinct persistence call sites that expect different types:

  a) DocumentStoreServer.run(cache_backend=<Backend>)
       → Internally wraps it into a Config with persistence_mode=UDF_CACHING.
       → You must pass a RAW Backend object here, NOT a Config.

  b) pw.run(persistence_config=<Config>)
       → Expects a fully constructed Config object.
       → Used when you want full pipeline state persistence (source offsets,
         window state, join state, etc.).

Passing a Config where a Backend is expected (or vice versa) causes a
runtime AttributeError on on_before_run() — which is exactly the error
'Config object has no attribute store_path_in_env_variable'.

HOW PATHWAY PERSISTENCE WORKS (simplified)
------------------------------------------
  ┌──────────────┐     ┌─────────────────────┐     ┌───────────────┐
  │  Data Source  │────▶│  Pathway Engine      │────▶│  Output Sink  │
  │  (CSV/stream) │     │  (operators/windows) │     │  (CSV/print)  │
  └──────────────┘     └─────────────────────┘     └───────────────┘
                                   │
                          pw.run(persistence_config=...)
                                   │
                        ┌──────────▼──────────┐
                        │  data/persistence/   │
                        │  ├── metadata/       │  ← tracks source offsets
                        │  └── snapshot/       │  ← operator state snapshots
                        └─────────────────────┘

The `name=` parameter on connectors (e.g. pw.io.fs.read(..., name="price_source"))
is the *unique persistent ID*. Without a name, Pathway auto-assigns IDs based on
construction order — which breaks if you ever refactor the pipeline.
Always set explicit names on connectors when persistence is enabled.
"""

import os
import pathway as pw




PERSISTENCE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),   # repo root
    "data",
    "persistence",
)


SNAPSHOT_INTERVAL_MS = 5_000


#Public API 

def get_persistence_backend() -> pw.persistence.Backend:
    """
    Build and return a raw Pathway filesystem Backend.

    Use this when calling DocumentStoreServer.run(cache_backend=...).
    server.run() wraps the Backend internally — passing a Config here
    causes an AttributeError at startup.

    Usage

        from ingestion.state_manager import get_persistence_backend

        server.run(
            with_cache=True,
            cache_backend=get_persistence_backend(),   # ← Backend, not Config
        )

    Returns
  
    pw.persistence.Backend
        A filesystem backend rooted at data/persistence/.
    """
    os.makedirs(PERSISTENCE_DIR, exist_ok=True)
    return pw.persistence.Backend.filesystem(PERSISTENCE_DIR)


def get_persistence_config() -> pw.persistence.Config:
    """
    Build and return a fully constructed Pathway persistence Config.

    Use this when calling pw.run(persistence_config=...) directly —
    i.e., in pipeline entry-points that are NOT going through
    DocumentStoreServer.run().

    Usage
   
        from ingestion.state_manager import get_persistence_config

        pw.run(
            persistence_config=get_persistence_config(),
        )

    Returns
  
    pw.persistence.Config
        Pass this directly to pw.run(persistence_config=...).
    """
    
    backend = get_persistence_backend()

   
    config = pw.persistence.Config(
        backend,
        snapshot_interval_ms=SNAPSHOT_INTERVAL_MS,
    )

    return config


def make_persistent_name(role: str) -> str:
    """
    Utility: generate a stable, human-readable persistent ID for a connector.

    Pathway matches a connector's persisted state across restarts using the `name`
    parameter on pw.io connectors.  If you don't pass a name, Pathway assigns one
    automatically based on construction order — which breaks if you ever reorder
    code.  Always prefer explicit names.

    Usage

        files = pw.io.fs.read(
            "data/news_docs/",
            format="binary",
            mode="streaming",
            name=make_persistent_name("news_docs"),   # ← stable across restarts
        )

    Parameters
   
    role : str
        A short descriptive label, e.g. "price_source", "news_docs".

    Returns
    
    str
        A stable name like "interiit_price_source".
    """
    return f"interiit_{role}"
