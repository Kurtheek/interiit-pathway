"""
Data Storage & State Management (vector index half)

This module:
  1. Watches data/news_docs/ for new PDF / text files dropped by P4.
  2. Parses each document into text chunks.
  3. Embeds every chunk using a local SentenceTransformer model (all-MiniLM-L6-v2).
     No API key required — runs fully offline, 384-dimensional vectors.
  4. Maintains a live, always-up-to-date vector index in memory.
  5. Serves similarity-search queries over HTTP on localhost:8765.

Person 7 (RAG pipeline) is BLOCKED until this server is running and accepts
POST requests on http://localhost:8765/v1/retrieve.

HOW THE PIPELINE FLOWS
-----------------------
                  data/news_docs/
                  ├── earnings_q1.pdf   ← P4 drops files here
                  ├── reuters_news.txt
                  └── ...
                         │
                         │ pw.io.fs.read(format="binary", mode="streaming")
                         ▼
                   raw_files_table          (columns: data:bytes, _metadata:dict)
                         │
                         │ UnstructuredParser  (PDF/text → text chunks)
                         ▼
                   documents_table          (columns: text:str, _metadata:dict)
                         │
                         │ DocumentStore builds KNN index over local embeddings
                         ▼
                   in-memory vector index   (384-dim, BruteForceKNN)
                         │
                         │ DocumentStoreServer exposes REST API
                         ▼
              http://localhost:8765/v1/retrieve   ← P7 (RAG) queries here

WHAT IS A VECTOR INDEX?

Each document chunk is converted to a list of 384 numbers (an "embedding")
by the all-MiniLM-L6-v2 SentenceTransformer model.  Text with similar meaning
ends up with numerically close embeddings.  The index stores all these
number-lists so that at query time we can instantly find the N nearest chunks
to a query embedding — this is k-Nearest-Neighbours (KNN) search.

Because Pathway runs this as a streaming pipeline, the index updates
*automatically* when a new file appears in data/news_docs/.  No restart needed.

PERSISTENCE BEHAVIOUR (important for the crash-recovery test)

server.run(cache_backend=<Backend>) uses UDF_CACHING mode internally.
UDF_CACHING only caches embedding results (text → vector).  It does NOT
persist the index state itself.  On restart:
  - Files are re-read from data/news_docs/ (source is not persisted).
  - Embeddings are served from cache (fast, no API call).
  - The index is rebuilt from the cached embeddings.
  - During rebuilding, queries return [].

This is expected behaviour for server.run() with UDF_CACHING.
For the persistence test, wait for the "Starting DocumentStoreServer" log,
then wait an additional ~5 seconds for the index to rebuild before querying.
The DocumentStoreClient.get_vectorstore_statistics() endpoint can be polled
to detect when file_count > 0, indicating the index is ready.

REST API REFERENCE (for Person 7)

Use DocumentStoreClient (built into Pathway) instead of raw curl — it handles
response parsing correctly:

    from pathway.xpacks.llm.document_store import DocumentStoreClient
    client = DocumentStoreClient(host="127.0.0.1", port=8765)
    results = client.query("AAPL earnings surprise", k=5)
    # results: list of {"text": str, "dist": float, "metadata": dict}

Or raw HTTP:
  POST http://localhost:8765/v1/retrieve
  Body (JSON): {"query": "AAPL earnings surprise", "k": 5}
  Response: [{"text": "...", "dist": 0.12, "metadata": {...}}, ...]

  POST http://localhost:8765/v1/inputs
  Body: {}

  POST http://localhost:8765/v1/statistics
  Body: {}
"""

import os
import logging
import time

import pathway as pw
from pathway.xpacks.llm.parsers import UnstructuredParser
from pathway.xpacks.llm.splitters import TokenCountSplitter
from pathway.xpacks.llm.embedders import SentenceTransformerEmbedder
from pathway.xpacks.llm.document_store import DocumentStore, DocumentStoreClient
from pathway.xpacks.llm.servers import DocumentStoreServer
from pathway.stdlib.indexing.nearest_neighbors import BruteForceKnnFactory

from ingestion.state_manager import get_persistence_backend, make_persistent_name

# Logging 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [vector_index] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration
NEWS_DOCS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),   # repo root
    "data",
    "news_docs",
)

VECTOR_SERVER_HOST = "127.0.0.1"
VECTOR_SERVER_PORT = 8765

# Local SentenceTransformer model — no API key, fully offline, 384-dim vectors.
EMBEDDING_MODEL = "all-MiniLM-L6-v2"

CHUNK_MAX_TOKENS = 300
CHUNK_MIN_TOKENS = 80
AUTOCOMMIT_MS = 1_000

# How long to wait between readiness poll attempts (seconds).
_READINESS_POLL_INTERVAL_S = 2
# How many times to poll before giving up.
_READINESS_POLL_ATTEMPTS = 30


#Main entry-point 

def build_and_run_vector_index(threaded: bool = False) -> None:
    """
    Build the document indexing pipeline and start the HTTP server.

    No environment variables are required — SentenceTransformer runs locally.

    Parameters
    ----------
    threaded : bool
        If True, runs in a background thread (useful for notebooks or combined
        pipelines).  If False (default), this call blocks the process.

    Persistence note
    ----------------
    server.run() uses UDF_CACHING mode which caches embedding results but does
    NOT persist the index itself.  On restart the index rebuilds from the cached
    embeddings (~seconds for hundreds of docs).  Use wait_until_ready() after
    starting in threaded mode to block until the index is available.
    """
    os.makedirs(NEWS_DOCS_DIR, exist_ok=True)
    logger.info("Watching %s for new documents", NEWS_DOCS_DIR)

  
    # mode="streaming": watches continuously, picks up new files automatically.
    # format="binary": raw bytes, parsed in Step 2.
    # with_metadata=True: adds _metadata dict (path, modified_at, size) per file.
    # name=: stable persistent ID — Pathway uses this to match state on restart.
    raw_files_table = pw.io.fs.read(
        NEWS_DOCS_DIR,
        format="binary",
        mode="streaming",
        with_metadata=True,
        autocommit_duration_ms=AUTOCOMMIT_MS,
        name=make_persistent_name("news_docs"),
    )

    
    # UnstructuredParser handles PDF, DOCX, HTML, plain text.
    # chunking_mode="elements": returns individual paragraphs/headings as
    # separate elements for finer-grained retrieval.
    parser = UnstructuredParser(chunking_mode="elements")

    # Step 3: Chunk long texts 
    splitter = TokenCountSplitter(
        min_tokens=CHUNK_MIN_TOKENS,
        max_tokens=CHUNK_MAX_TOKENS,
    )

    
    # SentenceTransformerEmbedder: downloads all-MiniLM-L6-v2 once, then runs
    # locally.  No API key.  384-dimensional vectors.
    embedder = SentenceTransformerEmbedder(model=EMBEDDING_MODEL)

    #Step 5: Create retriever factory
    #
    # BruteForceKnnFactory: exact cosine-similarity KNN.
    # Correct and fast for hundreds of chunks.
    retriever_factory = BruteForceKnnFactory(embedder=embedder)

    
    # Orchestrates parse → chunk → embed → index automatically.
    # Updates live as new files arrive.
    store = DocumentStore(
        docs=raw_files_table,
        retriever_factory=retriever_factory,
        parser=parser,
        splitter=splitter,
    )

   
    server = DocumentStoreServer(
        host=VECTOR_SERVER_HOST,
        port=VECTOR_SERVER_PORT,
        document_store=store,
    )

    logger.info(
        "Starting DocumentStoreServer on http://%s:%d",
        VECTOR_SERVER_HOST,
        VECTOR_SERVER_PORT,
    )

    
    # cache_backend must be a pw.persistence.Backend — NOT a Config.
    
    # server.run() wraps it internally:
    #   pw.persistence.Config(backend, persistence_mode=UDF_CACHING)
    
    # UDF_CACHING = only embedding results are cached to disk.
    # The index itself is NOT persisted — it rebuilds on every restart
    # from the cached embeddings (fast).  This is expected behaviour.
    #
    # Passing a Config here (instead of Backend) causes:
    #   AttributeError: 'Config' object has no attribute 'store_path_in_env_variable'
    server.run(
        with_cache=True,
        cache_backend=get_persistence_backend(),   # Backend, not Config
        threaded=threaded,
    )



def wait_until_ready(
    timeout_s: int = 60,
    poll_interval_s: int = _READINESS_POLL_INTERVAL_S,
) -> bool:
    """
    Block until the vector index server has at least one document indexed,
    or until timeout_s seconds have passed.

    Use this after build_and_run_vector_index(threaded=True) to avoid
    querying the server before the index has finished rebuilding.

    Parameters
    ----------
    timeout_s : int
        Maximum seconds to wait.
    poll_interval_s : int
        Seconds between readiness checks.

    Returns
    -------
    bool
        True if the server became ready within timeout_s, False otherwise.

    Example
    -------
        t = threading.Thread(
            target=build_and_run_vector_index,
            kwargs={"threaded": True},
            daemon=True,
        )
        t.start()
        if not wait_until_ready(timeout_s=60):
            raise RuntimeError("Vector index did not become ready in time")
        results = query_vector_index("AAPL earnings", k=3)
    """
    import requests

    stats_url = f"http://{VECTOR_SERVER_HOST}:{VECTOR_SERVER_PORT}/v1/statistics"
    deadline = time.time() + timeout_s

    while time.time() < deadline:
        try:
            resp = requests.post(stats_url, json={}, timeout=3)
            if resp.status_code == 200:
                data = resp.json()
                # data is the raw result value from QueryResultSchema
                # statistics returns {"file_count": N, ...} or {"result": {...}}
                # Handle both wrapped and unwrapped formats
                stats = data.get("result", data) if isinstance(data, dict) else data
                file_count = stats.get("file_count", 0) if isinstance(stats, dict) else 0
                if file_count is not None and file_count > 0:
                    logger.info("Vector index ready: %d file(s) indexed", file_count)
                    return True
        except Exception:
            pass   # server not up yet — keep polling
        logger.info("Waiting for vector index to become ready...")
        time.sleep(poll_interval_s)

    logger.warning("Vector index did not become ready within %d seconds", timeout_s)
    return False


# Query helper (for P7)

def query_vector_index(query_text: str, k: int = 5) -> list[dict]:
    """
    Query the running vector index and return the top-k results.

    Uses Pathway's built-in DocumentStoreClient which handles response
    parsing correctly for all Pathway versions.

    The server must already be running.  Call wait_until_ready() first
    if you started the server in threaded mode.

    Parameters
    ----------
    query_text : str
        The search query, e.g. "AAPL earnings Q1 2024".
    k : int
        Number of top results to return.

    Returns
    -------
    list[dict]
        Each dict: {"text": str, "dist": float, "metadata": dict}.
        Sorted by dist ascending (smaller = more similar).

    Example (for P7)
    ----------------
        from ingestion.vector_index import query_vector_index, wait_until_ready

        wait_until_ready()
        results = query_vector_index("AAPL earnings surprise", k=3)
        for r in results:
            print(f"dist={r['dist']:.4f}  {r['text'][:100]}")
    """
    import requests

    client = DocumentStoreClient(
        host=VECTOR_SERVER_HOST,
        port=VECTOR_SERVER_PORT,
    )
    try:
        return client.query(query_text, k=k)
    except requests.exceptions.ConnectionError:
        raise RuntimeError(
            f"Cannot connect to vector index at "
            f"http://{VECTOR_SERVER_HOST}:{VECTOR_SERVER_PORT}. "
            "Is build_and_run_vector_index() running?"
        )




if __name__ == "__main__":
    # python -m ingestion.vector_index
    build_and_run_vector_index(threaded=False)
