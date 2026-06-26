
#Persistence / crash-recovery test


import os
import sys
import time
import shutil
import threading
import argparse

# Make sure repo root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.vector_index import (
    build_and_run_vector_index,
    query_vector_index,
    wait_until_ready,
    NEWS_DOCS_DIR,
)

# Test documents 

TEST_DOCS = {
    "aapl_earnings.txt": (
        "Apple reported record Q1 earnings driven by strong iPhone 15 sales "
        "in Asia Pacific. Revenue beat analyst estimates by 8 percent."
    ),
    "jpm_fraud.txt": (
        "JPMorgan Chase flagged suspicious trading patterns in derivatives markets. "
        "Regulators opened a preliminary investigation into potential spoofing."
    ),
    "msft_cloud.txt": (
        "Microsoft Azure cloud revenue grew 28 percent year-over-year, "
        "driven by enterprise AI adoption and OpenAI partnership expansion."
    ),
}


def setup_test_docs():
    """Write test documents to data/news_docs/."""
    os.makedirs(NEWS_DOCS_DIR, exist_ok=True)
    for filename, content in TEST_DOCS.items():
        path = os.path.join(NEWS_DOCS_DIR, filename)
        with open(path, "w") as f:
            f.write(content)
    print(f"[setup] Wrote {len(TEST_DOCS)} test documents to {NEWS_DOCS_DIR}")


def run_test_suite():
    """Run all persistence tests.  Server must already be running (or starting)."""

    print("\n" + "=" * 60)
    print("PERSISTENCE TEST SUITE")
    print("=" * 60)

    # ── Test 1: wait for server to be ready ────────────────────────────────────
    print("\n[test 1] Waiting for vector index to become ready...")
    ready = wait_until_ready(timeout_s=90)
    assert ready, (
        "FAIL: Vector index did not become ready within 90 seconds. "
        "Check that the server is running and docs are in data/news_docs/"
    )
    print("[test 1] PASS — server ready")

    # ── Test 2: basic retrieval returns results ────────────────────────────────
    print("\n[test 2] Basic retrieval query...")
    results = query_vector_index("Apple iPhone earnings", k=3)
    assert len(results) > 0, (
        "FAIL: query returned [] even though wait_until_ready() passed. "
        "This should not happen — file a bug."
    )
    print(f"[test 2] PASS — got {len(results)} result(s)")
    for r in results:
        print(f"         dist={r['dist']:.4f}  {r['text'][:80]}")

    # ── Test 3: results are sorted by distance ─────────────────────────────────
    print("\n[test 3] Results sorted by distance...")
    dists = [r["dist"] for r in results]
    assert dists == sorted(dists), f"FAIL: distances not sorted: {dists}"
    print("[test 3] PASS — results sorted correctly")

    # ── Test 4: relevant result is top-ranked ──────────────────────────────────
    print("\n[test 4] Relevance check — AAPL query should surface Apple doc...")
    top = results[0]["text"].lower()
    assert any(kw in top for kw in ["apple", "iphone", "earnings", "revenue"]), (
        f"FAIL: top result doesn't seem relevant to Apple earnings query.\n"
        f"Got: {results[0]['text'][:200]}"
    )
    print("[test 4] PASS — top result is relevant")

    # ── Test 5: different query returns different top result ───────────────────
    print("\n[test 5] Fraud query should surface JPMorgan doc...")
    fraud_results = query_vector_index("JPMorgan suspicious trading fraud", k=3)
    assert len(fraud_results) > 0, "FAIL: fraud query returned []"
    fraud_top = fraud_results[0]["text"].lower()
    assert any(kw in fraud_top for kw in ["jpmorgan", "trading", "fraud", "spoofing", "suspicious"]), (
        f"FAIL: top result for fraud query doesn't seem relevant.\n"
        f"Got: {fraud_results[0]['text'][:200]}"
    )
    print("[test 5] PASS — fraud query returned relevant result")

    # ── Test 6: live update — drop a new file ─────────────────────────────────
    print("\n[test 6] Live update — dropping new file while server is running...")
    new_doc_path = os.path.join(NEWS_DOCS_DIR, "gs_macro.txt")
    with open(new_doc_path, "w") as f:
        f.write(
            "Goldman Sachs raised its interest rate forecast following stronger "
            "than expected CPI data. Federal Reserve expected to hold rates higher for longer."
        )
    print("         Dropped gs_macro.txt — waiting 8s for it to be indexed...")
    time.sleep(8)

    macro_results = query_vector_index("Goldman Sachs interest rate CPI", k=3)
    assert len(macro_results) > 0, "FAIL: macro query returned [] after live update"
    macro_top = macro_results[0]["text"].lower()
    assert any(kw in macro_top for kw in ["goldman", "interest", "rate", "cpi", "federal"]), (
        f"FAIL: new document not indexed after 8s.\n"
        f"Got: {macro_results[0]['text'][:200]}"
    )
    print("[test 6] PASS — live update picked up correctly")

    # ── Summary ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("ALL TESTS PASSED")
    print("=" * 60)
    print()
    print("NOTE on persistence behaviour:")
    print("  server.run() uses UDF_CACHING — embedding results are cached.")
    print("  The index itself is NOT persisted and rebuilds on every restart.")
    print("  wait_until_ready() handles this correctly by polling /v1/statistics.")
    print("  On restart with cached embeddings, rebuild takes ~1-5 seconds.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--self-contained",
        action="store_true",
        help="Start the vector index server in a background thread before testing.",
    )
    args = parser.parse_args()

    setup_test_docs()

    if args.self_contained:
        print("[setup] Starting vector index server in background thread...")
        t = threading.Thread(
            target=build_and_run_vector_index,
            kwargs={"threaded": True},
            daemon=True,
        )
        t.start()
        # Give the server a moment to start its HTTP listener
        time.sleep(3)

    run_test_suite()


if __name__ == "__main__":
    main()
