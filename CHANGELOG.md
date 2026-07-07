# Changelog

Format loosely follows Keep a Changelog. Every number was measured; the
perf entry cites its before and after.

## [0.2.0] -- 2026-07-06
### Added
- Gzipped event logs (spark.eventLog.compress=true is the common
  production setting), with a test pinning identical analysis to plain.
- Benchmark: parse and analyze measured separately, 15 repeats, p50/p95/p99,
  on synthetic logs with a realistic 50:1 noise ratio. Results and chart
  committed.
- Makefile, CI (lint + tests + a demo smoke test asserting exit code 1,
  since the demo plants skew and finding it is the point).
### Fixed
- The parser claimed to skip noise "without parsing beyond the Event
  field" while json.loads-ing every line: 52.8 MB/s. A substring
  pre-filter on the three wanted event names made the docstring true:
  150 MB/s, a 597MB log from 11.6s to 4.0s. Deliberate contract change,
  pinned by test: corrupt noise is skipped, corruption in lines we read
  still raises with its line number.

## [0.1.0] -- 2026-07-06
### Added
- Event-log parser lifting TaskEnd / StageCompleted / ApplicationStart.
- The trim report: straggler, data_skew (the cause behind a straggler,
  or its absence naming a slow node instead), spill, gc_pressure,
  task_failures. Headline number is drag (wall minus median), a
  deliberate single-wave lower bound.
- CLI (analyze, demo, shanty), the demo log with planted problems,
  12 tests (13 with the gz regression test added in 0.2.0).
