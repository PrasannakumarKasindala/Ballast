"""
analyze.py -- the trim report.

Per stage, from task metrics alone, four findings:

    straggler    max task duration >> median. The stage's wall clock is the
                 straggler's wall clock; everyone else finished and waited.
    data_skew    max shuffle-read >> median shuffle-read. Names the CAUSE
                 behind most stragglers: one hot key, one fat partition.
                 A straggler without data skew smells like a slow node or
                 GC instead, and the report says which.
    spill        the stage wrote spill to disk. Memory pressure turned CPU
                 time into I/O time; usually a partition-count or
                 executor-memory conversation.
    gc_pressure  stage-level GC time above 15% of task time. The JVM is
                 running the job in its spare time.

The headline number per stage is DRAG: max task duration minus median task
duration, in executor-seconds. Read it as "the wall-clock this stage spent
waiting on its slowest task beyond a typical one." It is a deliberately
conservative, single-number lower bound: it ignores multi-wave scheduling
(where fixing skew saves even more) and it never multiplies by idle slots
(which would inflate). When ballast says a job has 40 minutes of drag, the
real prize is usually bigger. Under-claiming is the correct failure mode
for a diagnostic.

Thresholds are arguments with defaults, not magic: straggler_ratio 4x,
skew_ratio 4x, min_tasks 8 (below that, "skew" is noise), gc 15%.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field

from .events import AppLog, StageInfo


@dataclass
class StageVerdict:
    stage_id: int
    name: str
    tasks: int
    failed_tasks: int
    wall_ms: int              # max task duration: the critical path
    median_ms: int
    drag_ms: int              # wall - median
    straggler_ratio: float    # max / median duration
    skew_ratio: float         # max / median shuffle read (0 if no shuffle)
    spill_bytes: int
    gc_ratio: float           # sum(gc) / sum(duration)
    findings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = self.__dict__.copy()
        d["findings"] = list(self.findings)
        return d


def _ratio(values: list[int]) -> tuple[float, int, int]:
    """(max/median, max, median) with a zero-safe median."""
    mx = max(values)
    med = int(statistics.median(values))
    return (mx / med if med > 0 else 0.0), mx, med


def verdict_for(stage: StageInfo, *, straggler_ratio: float = 4.0,
                skew_ratio: float = 4.0, min_tasks: int = 8,
                gc_ratio: float = 0.15) -> StageVerdict:
    tasks = stage.tasks
    durations = [t.duration_ms for t in tasks] or [0]
    reads = [t.shuffle_read_bytes for t in tasks]
    dur_ratio, wall, med = _ratio(durations)
    rd_ratio = 0.0
    if any(reads):
        rd_ratio, _, _ = _ratio(reads)

    total_dur = sum(durations)
    gc = sum(t.gc_time_ms for t in tasks)
    spill = sum(t.spill_disk_bytes + t.spill_memory_bytes for t in tasks)
    failed = sum(1 for t in tasks if t.failed)

    v = StageVerdict(
        stage_id=stage.stage_id,
        name=stage.name or f"stage {stage.stage_id}",
        tasks=len(tasks),
        failed_tasks=failed,
        wall_ms=wall,
        median_ms=med,
        drag_ms=max(0, wall - med),
        straggler_ratio=round(dur_ratio, 2),
        skew_ratio=round(rd_ratio, 2),
        spill_bytes=spill,
        gc_ratio=round(gc / total_dur, 4) if total_dur else 0.0,
    )

    if len(tasks) >= min_tasks and dur_ratio >= straggler_ratio:
        if rd_ratio >= skew_ratio:
            v.findings.append("data_skew")   # the straggler has a cause
        v.findings.append("straggler")
    if spill > 0:
        v.findings.append("spill")
    if v.gc_ratio >= gc_ratio:
        v.findings.append("gc_pressure")
    if failed:
        v.findings.append("task_failures")
    return v


def analyze(app: AppLog, **thresholds) -> list[StageVerdict]:
    """One verdict per stage, sorted by drag descending: the report reads
    top-down as 'this stage is why the job is slow'."""
    verdicts = [verdict_for(s, **thresholds) for s in app.stages.values()
                if s.tasks]
    verdicts.sort(key=lambda v: -v.drag_ms)
    return verdicts
