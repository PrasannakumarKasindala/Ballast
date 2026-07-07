"""
events.py -- the typed view over Spark's event log.

With spark.eventLog.enabled=true (which every serious deployment sets,
because the history server needs it), Spark writes one JSON object per
listener event. ballast reads three of the dozens:

    SparkListenerApplicationStart   app name and id, for the report header
    SparkListenerTaskEnd            per-task metrics: the raw material
    SparkListenerStageCompleted     stage names, so findings have names
                                    humans recognize instead of stage ids

Everything else is skipped without parsing beyond the Event field, which is
most of why parsing is fast: a large event log is mostly executor adds,
block updates, and environment dumps we never touch.

Only the metrics ballast uses are lifted. Durations come in milliseconds
from Spark and stay milliseconds here; conversion happens at the report
edge, once.
"""

from __future__ import annotations

import gzip
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, TextIO


@dataclass(frozen=True)
class TaskMetrics:
    stage_id: int
    task_id: int
    duration_ms: int
    gc_time_ms: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    spill_disk_bytes: int
    spill_memory_bytes: int
    failed: bool


@dataclass
class StageInfo:
    stage_id: int
    name: str = ""
    num_tasks: int = 0
    tasks: list[TaskMetrics] = field(default_factory=list)


@dataclass
class AppLog:
    app_name: str = ""
    app_id: str = ""
    stages: dict[int, StageInfo] = field(default_factory=dict)

    def stage(self, stage_id: int) -> StageInfo:
        if stage_id not in self.stages:
            self.stages[stage_id] = StageInfo(stage_id=stage_id)
        return self.stages[stage_id]


def _int(doc: dict, *path: str) -> int:
    cur = doc
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return 0
        cur = cur[key]
    try:
        return int(cur)
    except (TypeError, ValueError):
        return 0


def _task_end(doc: dict) -> TaskMetrics:
    info = doc.get("Task Info") or {}
    m = doc.get("Task Metrics") or {}
    end_reason = (doc.get("Task End Reason") or {}).get("Reason", "")
    return TaskMetrics(
        stage_id=_int(doc, "Stage ID"),
        task_id=_int(info, "Task ID"),
        duration_ms=max(0, _int(info, "Finish Time") - _int(info, "Launch Time")),
        gc_time_ms=_int(m, "JVM GC Time"),
        shuffle_read_bytes=(_int(m, "Shuffle Read Metrics", "Remote Bytes Read")
                            + _int(m, "Shuffle Read Metrics", "Local Bytes Read")),
        shuffle_write_bytes=_int(m, "Shuffle Write Metrics",
                                 "Shuffle Bytes Written"),
        spill_disk_bytes=_int(m, "Disk Bytes Spilled"),
        spill_memory_bytes=_int(m, "Memory Bytes Spilled"),
        failed=(end_reason != "Success"),
    )


_WANTED = ("SparkListenerTaskEnd", "SparkListenerStageCompleted",
           "SparkListenerApplicationStart")


def _open(path: str | Path) -> TextIO:
    """Real deployments set spark.eventLog.compress=true; a skew tool that
    cannot read .gz would fail on the first production log it met."""
    p = Path(path)
    if p.suffix == ".gz":
        return gzip.open(p, "rt")
    return p.open()


def parse(path: str | Path) -> AppLog:
    """Parse one event log file (the uncompressed JSON-lines form).

    Unknown events are skipped by design; a malformed line among the events
    we DO read raises with its line number, because an analyzer that
    silently ate half a log would report half the skew and all of the
    confidence.

    The substring pre-filter below is what makes the docstring's "skipped
    without parsing" claim true. The first benchmark caught the code
    json.loads-ing every one of the ~98% noise lines anyway (52 MB/s). An
    event's type appears verbatim in its line, so lines containing none of
    the wanted names cannot be wanted events: no false negatives, and any
    false positive still faces the authoritative Event check after parsing.
    """
    app = AppLog()
    with _open(path) as f:
        for lineno, line in enumerate(f, 1):
            if not any(w in line for w in _WANTED):
                continue
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"{path}:{lineno}: bad event: {e}") from e

            event = doc.get("Event", "")
            if event == "SparkListenerTaskEnd":
                t = _task_end(doc)
                app.stage(t.stage_id).tasks.append(t)
            elif event == "SparkListenerStageCompleted":
                info = doc.get("Stage Info") or {}
                s = app.stage(_int(info, "Stage ID"))
                s.name = str(info.get("Stage Name") or s.name)
                s.num_tasks = _int(info, "Number of Tasks") or s.num_tasks
            elif event == "SparkListenerApplicationStart":
                app.app_name = str(doc.get("App Name") or "")
                app.app_id = str(doc.get("App ID") or "")
    return app


def iter_events(path: str | Path) -> Iterator[dict]:
    """Raw event iterator, for anyone digging past what ballast lifts."""
    with _open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)
