#!/usr/bin/env python3
"""Compare benchmark results from two JSON files side by side.

Usage:
    python3 benchmarks/compare.py results/zeppelin.json results/turbopuffer.json
"""

import json
import sys


def load_results(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def format_ms(val: float | None) -> str:
    if val is None:
        return "N/A"
    return f"{val:.1f}ms"


def format_num(val: float | int | None) -> str:
    if val is None:
        return "N/A"
    if isinstance(val, float):
        return f"{val:,.1f}"
    return f"{val:,}"


def compare_latency(a: dict, b: dict, label_a: str, label_b: str) -> None:
    keys = ["p50_ms", "p95_ms", "p99_ms", "max_ms", "mean_ms"]
    header = f"{'Metric':<12} {label_a:>12} {label_b:>12} {'Ratio':>8}"
    print(header)
    print("-" * len(header))

    for key in keys:
        va = a.get(key)
        vb = b.get(key)
        ratio = ""
        if va and vb and vb > 0:
            r = va / vb
            ratio = f"{r:.2f}x"
        print(f"{key:<12} {format_ms(va):>12} {format_ms(vb):>12} {ratio:>8}")


def main():
    if len(sys.argv) < 3:
        print("Usage: compare.py <result_a.json> <result_b.json>")
        sys.exit(1)

    a = load_results(sys.argv[1])
    b = load_results(sys.argv[2])

    label_a = a.get("target", "A")
    label_b = b.get("target", "B")

    print(f"\n=== Benchmark Comparison ===")
    print(f"Scenario: {a.get('scenario', '?')}")
    print(f"  {label_a}: {sys.argv[1]}")
    print(f"  {label_b}: {sys.argv[2]}")
    print()

    ra = a.get("results", {})
    rb = b.get("results", {})

    # Try to find latency data at various nesting levels
    def extract_latency(r: dict) -> dict | None:
        # Direct latency keys
        if "p50_ms" in r:
            return r
        # Nested under common keys
        for key in ["concurrency_levels", "filter_results", "query_results",
                     "batch_results", "scale_results", "index_comparison"]:
            if key in r and isinstance(r[key], list) and r[key]:
                return r[key][0]  # Show first entry
        return r

    lat_a = extract_latency(ra)
    lat_b = extract_latency(rb)

    if lat_a and lat_b:
        compare_latency(lat_a, lat_b, label_a, label_b)
    else:
        print("Could not extract comparable latency data.")
        print(f"\n{label_a} results:")
        print(json.dumps(ra, indent=2))
        print(f"\n{label_b} results:")
        print(json.dumps(rb, indent=2))

    # QPS comparison
    qps_a = ra.get("qps") or (lat_a or {}).get("qps")
    qps_b = rb.get("qps") or (lat_b or {}).get("qps")
    if qps_a or qps_b:
        print(f"\n{'QPS':<12} {format_num(qps_a):>12} {format_num(qps_b):>12}", end="")
        if qps_a and qps_b and qps_b > 0:
            print(f" {qps_a / qps_b:>7.2f}x")
        else:
            print()


if __name__ == "__main__":
    main()
