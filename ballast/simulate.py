"""
simulate.py -- an event log from a job having a bad day, fabricated.

The demo needs an event log shaped like a real ETL app so it can prove
ballast finds exactly what was planted:

    stage 4   "shuffle join at Enrich.scala:88"
              THE hot key. 200 tasks; task 4137 gets ~45% of the shuffle
              read and ~12x the median duration. data_skew + straggler.
    stage 7   "sortWithinPartitions at Dedup.scala:31"
              memory pressure: most tasks spill to disk. spill.
    stage 9   "aggregate at Rollup.scala:52"
              undersized executors: ~25% of task time is GC. gc_pressure.
    stage 11  "map at Score.scala:19"
              a straggler WITHOUT read skew: one task 6x median on uniform
              input. That is a slow node, not a hot key, and the report
              should distinguish the two.
    stage 13  "save at Publish.scala:44"
              three task failures (retried successfully; Spark does that).
    the rest  healthy stages of assorted sizes, because most of a real job
              is fine and the report must not drown the signal.

Format matches what Spark 3.x writes closely enough that the parser cannot
tell the difference; only fields ballast reads are emitted, which keeps the
demo log honest about what the parser actually uses.

Deterministic (seeded) so the README numbers are reproducible.
"""

from __future__ import annotations

import json
import random
from pathlib import Path

APP_NAME = "nightly_enrichment_v3"
APP_ID = "application_1751760000000_0042"


def _task_end(stage_id: int, task_id: int, dur_ms: int, *, gc_ms: int = 0,
              read_b: int = 0, write_b: int = 0, spill_disk: int = 0,
              spill_mem: int = 0, failed: bool = False,
              t0: int = 1_751_760_000_000) -> dict:
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage_id,
        "Task End Reason": {"Reason": "Success" if not failed
                            else "ExceptionFailure"},
        "Task Info": {
            "Task ID": task_id,
            "Launch Time": t0,
            "Finish Time": t0 + dur_ms,
        },
        "Task Metrics": {
            "JVM GC Time": gc_ms,
            "Disk Bytes Spilled": spill_disk,
            "Memory Bytes Spilled": spill_mem,
            "Shuffle Read Metrics": {
                "Remote Bytes Read": int(read_b * 0.8),
                "Local Bytes Read": read_b - int(read_b * 0.8),
            },
            "Shuffle Write Metrics": {"Shuffle Bytes Written": write_b},
        },
    }


def _stage_completed(stage_id: int, name: str, num_tasks: int) -> dict:
    return {
        "Event": "SparkListenerStageCompleted",
        "Stage Info": {"Stage ID": stage_id, "Stage Name": name,
                       "Number of Tasks": num_tasks},
    }


def build_log(seed: int = 21) -> list[dict]:
    rng = random.Random(seed)
    events: list[dict] = [{
        "Event": "SparkListenerApplicationStart",
        "App Name": APP_NAME, "App ID": APP_ID,
    }]
    task_id = 4000

    def healthy(stage_id: int, name: str, n: int, base_ms: int,
                read_b: int = 64_000_000) -> None:
        nonlocal task_id
        for _ in range(n):
            events.append(_task_end(
                stage_id, task_id,
                int(base_ms * rng.uniform(0.75, 1.3)),
                gc_ms=int(base_ms * rng.uniform(0.01, 0.05)),
                read_b=int(read_b * rng.uniform(0.8, 1.2)),
                write_b=int(read_b * rng.uniform(0.4, 0.7))))
            task_id += 1
        events.append(_stage_completed(stage_id, name, n))

    # Healthy opening stages.
    healthy(1, "load at Ingest.scala:12", 64, 9_000)
    healthy(2, "filter at Clean.scala:27", 64, 4_000)
    healthy(3, "repartition at Clean.scala:55", 128, 6_000)

    # Stage 4: THE hot key.
    n = 200
    for i in range(n):
        if i == 137:
            events.append(_task_end(
                4, task_id, 480_000,
                gc_ms=9_000, read_b=5_400_000_000, write_b=2_100_000_000))
        else:
            events.append(_task_end(
                4, task_id, int(40_000 * rng.uniform(0.7, 1.3)),
                gc_ms=int(rng.uniform(300, 1200)),
                read_b=int(33_000_000 * rng.uniform(0.7, 1.3)),
                write_b=int(15_000_000 * rng.uniform(0.7, 1.3))))
        task_id += 1
    events.append(_stage_completed(4, "shuffle join at Enrich.scala:88", n))

    healthy(5, "map at Enrich.scala:104", 200, 7_000)
    healthy(6, "union at Merge.scala:9", 96, 5_000)

    # Stage 7: the spill.
    n = 120
    for _ in range(n):
        dur = int(55_000 * rng.uniform(0.8, 1.25))
        events.append(_task_end(
            7, task_id, dur,
            gc_ms=int(dur * 0.06),
            read_b=int(210_000_000 * rng.uniform(0.85, 1.15)),
            spill_disk=int(150_000_000 * rng.uniform(0.6, 1.4)),
            spill_mem=int(420_000_000 * rng.uniform(0.6, 1.4))))
        task_id += 1
    events.append(_stage_completed(
        7, "sortWithinPartitions at Dedup.scala:31", n))

    healthy(8, "distinct at Dedup.scala:40", 120, 8_000)

    # Stage 9: the GC fire.
    n = 80
    for _ in range(n):
        dur = int(30_000 * rng.uniform(0.8, 1.2))
        events.append(_task_end(
            9, task_id, dur, gc_ms=int(dur * rng.uniform(0.20, 0.30)),
            read_b=int(96_000_000 * rng.uniform(0.8, 1.2))))
        task_id += 1
    events.append(_stage_completed(9, "aggregate at Rollup.scala:52", n))

    healthy(10, "join at Rollup.scala:71", 160, 11_000)

    # Stage 11: straggler WITHOUT skew (the slow node).
    n = 100
    for i in range(n):
        dur = 108_000 if i == 55 else int(18_000 * rng.uniform(0.8, 1.2))
        events.append(_task_end(
            11, task_id, dur, gc_ms=int(dur * 0.03),
            read_b=int(52_000_000 * rng.uniform(0.9, 1.1))))
        task_id += 1
    events.append(_stage_completed(11, "map at Score.scala:19", n))

    healthy(12, "coalesce at Publish.scala:30", 32, 6_000)

    # Stage 13: three failures, retried (Spark emits both attempts).
    n = 48
    for i in range(n):
        if i in (7, 21, 33):
            events.append(_task_end(13, task_id, 12_000, failed=True))
            task_id += 1
        events.append(_task_end(
            13, task_id, int(10_000 * rng.uniform(0.8, 1.2)),
            write_b=int(40_000_000 * rng.uniform(0.8, 1.2))))
        task_id += 1
    events.append(_stage_completed(13, "save at Publish.scala:44", n))

    return events


def write_log(path: str | Path, seed: int = 21) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        for e in build_log(seed):
            f.write(json.dumps(e) + "\n")
    return out
