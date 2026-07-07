"""Render benchmark/results/throughput.json into one chart. Run after harness."""

from __future__ import annotations

import json
from pathlib import Path

RESULTS = Path(__file__).parent / "results"


def main() -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rows = json.loads((RESULTS / "throughput.json").read_text())
    mbs = [r["log_mb"] for r in rows]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))
    ax1.plot(mbs, [r["parse_p50_ms"] / 1000 for r in rows],
             marker="o", color="#16a085", label="parse p50")
    ax1.plot(mbs, [r["parse_p99_ms"] / 1000 for r in rows],
             marker="o", color="#c0392b", label="parse p99")
    ax1.set_xscale("log")
    ax1.set_yscale("log")
    ax1.set_xlabel("log size (MB)")
    ax1.set_ylabel("seconds")
    ax1.set_title("parse time vs log size")
    ax1.grid(True, which="both", alpha=0.3)
    ax1.legend()

    ax2.plot(mbs, [r["mb_per_sec"] for r in rows], marker="o",
             color="#b9770e")
    ax2.set_xscale("log")
    ax2.set_ylim(bottom=0)
    ax2.set_xlabel("log size (MB)")
    ax2.set_ylabel("MB / second")
    ax2.set_title("throughput stays flat")
    ax2.grid(True, alpha=0.3)

    fig.suptitle("ballast parse, single thread, 50:1 noise ratio")
    fig.tight_layout()
    out = RESULTS / "throughput.png"
    fig.savefig(out, dpi=110)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
