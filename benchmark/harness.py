"""
benchmark/harness.py -- how big a log can ballast chew?

Real event logs from long jobs run to gigabytes, and the parse (not the
analysis) is where the time goes: task-end events are ~2% of a real log's
lines, and the parser's whole strategy is refusing to look past the Event
field of the other 98%.

The harness generates synthetic logs at increasing task counts with a
realistic 50:1 noise ratio (block updates, executor metrics) around the
task events, then measures parse and analyze separately, 15 repeats,
p50/p95/p99.

Usage:  python -m benchmark.harness
Writes: benchmark/results/throughput.json
"""

from __future__ import annotations

import json
import random
import statistics
import tempfile
import time
from pathlib import Path

from ballast.analyze import analyze
from ballast.events import parse
from ballast.simulate import _stage_completed, _task_end

TASKS = [1_000, 5_000, 25_000, 100_000]
NOISE_PER_TASK = 50
REPEATS = 15
RESULTS = Path(__file__).parent / "results"

_NOISE = json.dumps({"Event": "SparkListenerBlockUpdated",
                     "Block Updated Info": {"Block ID": "rdd_42_7",
                                            "Memory Size": 1048576}})


def synth_log(path: Path, n_tasks: int, seed: int = 5) -> int:
    rng = random.Random(seed)
    lines = 0
    with path.open("w") as f:
        f.write(json.dumps({"Event": "SparkListenerApplicationStart",
                            "App Name": "bench", "App ID": "app_1"}) + "\n")
        lines += 1
        stage, in_stage = 0, 0
        for t in range(n_tasks):
            if in_stage == 0:
                stage += 1
            f.write(json.dumps(_task_end(
                stage, t, int(rng.uniform(5_000, 60_000)),
                gc_ms=int(rng.uniform(0, 2_000)),
                read_b=int(rng.uniform(0, 2e8)))) + "\n")
            lines += 1
            for _ in range(NOISE_PER_TASK):
                f.write(_NOISE + "\n")
            lines += NOISE_PER_TASK
            in_stage = (in_stage + 1) % 500
            if in_stage == 0:
                f.write(json.dumps(_stage_completed(
                    stage, f"stage {stage}", 500)) + "\n")
                lines += 1
    return lines


def pct(xs: list[float], p: int) -> float:
    return statistics.quantiles(xs, n=100, method="inclusive")[p - 1]


def run_one(n_tasks: int, tmp: Path) -> dict:
    log = tmp / f"log_{n_tasks}.jsonl"
    total_lines = synth_log(log, n_tasks)
    size_mb = log.stat().st_size / 1e6

    parse_ms, analyze_ms = [], []
    for _ in range(REPEATS):
        t0 = time.perf_counter()
        app = parse(log)
        parse_ms.append((time.perf_counter() - t0) * 1000)
        t0 = time.perf_counter()
        analyze(app)
        analyze_ms.append((time.perf_counter() - t0) * 1000)

    row = {
        "tasks": n_tasks,
        "log_lines": total_lines,
        "log_mb": round(size_mb, 1),
        "parse_p50_ms": round(pct(parse_ms, 50), 1),
        "parse_p95_ms": round(pct(parse_ms, 95), 1),
        "parse_p99_ms": round(pct(parse_ms, 99), 1),
        "analyze_p50_ms": round(pct(analyze_ms, 50), 2),
        "lines_per_sec": round(total_lines /
                               (statistics.median(parse_ms) / 1000)),
        "mb_per_sec": round(size_mb / (statistics.median(parse_ms) / 1000), 1),
    }
    print(f"  {n_tasks:>7,} tasks ({total_lines:>9,} lines, {size_mb:>7.1f}MB): "
          f"parse p50 {row['parse_p50_ms']:>8.1f}ms  "
          f"p99 {row['parse_p99_ms']:>8.1f}ms  "
          f"analyze p50 {row['analyze_p50_ms']:>7.2f}ms  "
          f"-> {row['mb_per_sec']:>6.1f} MB/s")
    return row


def main() -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    print(f"ballast benchmark ({REPEATS} repeats per size, "
          f"{NOISE_PER_TASK}:1 noise ratio):")
    rows = []
    with tempfile.TemporaryDirectory() as tmp:
        for n in TASKS:
            rows.append(run_one(n, Path(tmp)))
    out = RESULTS / "throughput.json"
    out.write_text(json.dumps(rows, indent=2))
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
