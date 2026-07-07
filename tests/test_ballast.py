"""
Tests for the promises, not the plumbing. Fixture builders are tiny
hand-rolled stages so a failure points at the math, not at 1,400 tasks of
demo log.
"""

import json

import pytest

from ballast.analyze import analyze, verdict_for
from ballast.events import StageInfo, TaskMetrics, parse
from ballast.simulate import build_log, write_log


def T(stage=1, task=1, dur=10_000, gc=0, read=0, write=0,
      spill_d=0, spill_m=0, failed=False):
    return TaskMetrics(stage_id=stage, task_id=task, duration_ms=dur,
                       gc_time_ms=gc, shuffle_read_bytes=read,
                       shuffle_write_bytes=write, spill_disk_bytes=spill_d,
                       spill_memory_bytes=spill_m, failed=failed)


def stage_of(tasks, name="s"):
    s = StageInfo(stage_id=tasks[0].stage_id, name=name)
    s.tasks = list(tasks)
    return s


def test_straggler_with_read_skew_is_data_skew():
    tasks = [T(task=i, dur=10_000, read=10_000_000) for i in range(19)]
    tasks.append(T(task=99, dur=120_000, read=200_000_000))
    v = verdict_for(stage_of(tasks))
    assert "data_skew" in v.findings
    assert "straggler" in v.findings


def test_straggler_with_uniform_reads_is_not_data_skew():
    tasks = [T(task=i, dur=10_000, read=10_000_000) for i in range(19)]
    tasks.append(T(task=99, dur=120_000, read=10_000_000))
    v = verdict_for(stage_of(tasks))
    assert "straggler" in v.findings
    assert "data_skew" not in v.findings


def test_small_stages_never_flag_skew():
    # 4 tasks, one slow: below min_tasks this is noise, not skew.
    tasks = [T(task=i, dur=10_000) for i in range(3)] + [T(task=9, dur=90_000)]
    v = verdict_for(stage_of(tasks), min_tasks=8)
    assert "straggler" not in v.findings


def test_drag_is_wall_minus_median():
    tasks = [T(task=i, dur=10_000) for i in range(9)] + [T(task=9, dur=60_000)]
    v = verdict_for(stage_of(tasks))
    assert v.wall_ms == 60_000
    assert v.median_ms == 10_000
    assert v.drag_ms == 50_000


def test_spill_flags_on_any_bytes():
    v = verdict_for(stage_of([T(task=1, spill_d=1)]))
    assert "spill" in v.findings
    assert verdict_for(stage_of([T(task=1)])).findings == []


def test_gc_pressure_is_stage_level():
    # Two tasks: one clean, one GC-heavy; stage-level ratio decides.
    tasks = [T(task=1, dur=10_000, gc=0), T(task=2, dur=10_000, gc=4_000)]
    v = verdict_for(stage_of(tasks))          # 20% overall
    assert "gc_pressure" in v.findings
    calm = [T(task=1, dur=10_000, gc=500), T(task=2, dur=10_000, gc=500)]
    assert "gc_pressure" not in verdict_for(stage_of(calm)).findings


def test_failed_tasks_surface():
    v = verdict_for(stage_of([T(task=1), T(task=2, failed=True)]))
    assert "task_failures" in v.findings
    assert v.failed_tasks == 1


def test_report_sorts_by_drag():
    a = stage_of([T(stage=1, task=i, dur=10_000) for i in range(9)]
                 + [T(stage=1, task=9, dur=30_000)], name="small-drag")
    b = stage_of([T(stage=2, task=i, dur=10_000) for i in range(9)]
                 + [T(stage=2, task=9, dur=200_000)], name="big-drag")
    from ballast.events import AppLog
    app = AppLog(stages={1: a, 2: b})
    verdicts = analyze(app)
    assert [v.name for v in verdicts] == ["big-drag", "small-drag"]


def test_parse_lifts_the_three_events(tmp_path):
    p = write_log(tmp_path / "events.jsonl")
    app = parse(p)
    assert app.app_name == "nightly_enrichment_v3"
    assert app.stages[4].name == "shuffle join at Enrich.scala:88"
    assert len(app.stages[4].tasks) == 200


def test_parse_skips_unknown_events(tmp_path):
    p = tmp_path / "log.jsonl"
    lines = [
        json.dumps({"Event": "SparkListenerBlockManagerAdded", "junk": 1}),
        json.dumps({"Event": "SparkListenerEnvironmentUpdate"}),
    ]
    p.write_text("\n".join(lines) + "\n")
    app = parse(p)
    assert app.stages == {}


def test_parse_bad_line_raises_with_line_number(tmp_path):
    p = tmp_path / "log.jsonl"
    p.write_text('{"Event": "SparkListenerTaskEnd"}\nnot json\n')
    with pytest.raises(ValueError, match="log.jsonl:2"):
        parse(p)


def test_demo_findings_are_exactly_the_planted_ones(tmp_path):
    app = parse(write_log(tmp_path / "events.jsonl"))
    verdicts = analyze(app)
    findings = {v.stage_id: set(v.findings) for v in verdicts if v.findings}
    assert findings == {
        4: {"data_skew", "straggler"},
        7: {"spill"},
        9: {"gc_pressure"},
        11: {"straggler"},
        13: {"task_failures"},
    }
