"""
cli.py -- the trim report, printed.

Plain argparse, stdlib only. Commands:

    ballast analyze    read a Spark event log, print the trim report
    ballast demo       fabricate a job having a bad day, analyze it
    ballast shanty     ...heave away.

Exit code is 1 when any stage carries a straggler or data_skew finding, so
a scheduled run can flag regressions before the on-call does.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from . import __version__, simulate
from .analyze import analyze
from .events import parse

_TTY = sys.stdout.isatty()
_COLOR = _TTY and "NO_COLOR" not in os.environ


def _c(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _COLOR else text


def _bold(t): return _c(t, "1")
def _red(t): return _c(t, "31")
def _yellow(t): return _c(t, "33")
def _green(t): return _c(t, "32")
def _dim(t): return _c(t, "2")

_FINDING_STYLE = {"data_skew": _red, "straggler": _red,
                  "spill": _yellow, "gc_pressure": _yellow,
                  "task_failures": _yellow}


def _gb(b: int) -> str:
    return f"{b / 1e9:.1f}GB"


# --- analyze command ---
def cmd_analyze(args: argparse.Namespace) -> int:
    app = parse(args.log)
    verdicts = analyze(app,
                       straggler_ratio=args.straggler_ratio,
                       skew_ratio=args.skew_ratio,
                       min_tasks=args.min_tasks)

    header = app.app_name or Path(args.log).name
    total_tasks = sum(v.tasks for v in verdicts)
    print(_bold(f"ballast: {header}  "
                f"({len(verdicts)} stages, {total_tasks:,} tasks)"))
    print()

    flagged = [v for v in verdicts if v.findings]
    if not flagged:
        print(_green("Trim and level. No findings."))
        return 0

    for v in flagged:
        tags = " ".join(_FINDING_STYLE.get(f, str)(_bold(f))
                        for f in v.findings)
        print(f"  {_bold(f's{v.stage_id:>3}')} {v.name}")
        print(f"       {tags}")
        detail = [f"drag {v.drag_ms / 1000:.1f}s "
                  f"(wall {v.wall_ms / 1000:.1f}s, median "
                  f"{v.median_ms / 1000:.1f}s over {v.tasks} tasks)"]
        if "data_skew" in v.findings:
            detail.append(f"read skew {v.skew_ratio:.0f}x median: "
                          f"repartition or salt the hot key")
        elif "straggler" in v.findings:
            detail.append(f"reads are uniform ({v.skew_ratio:.1f}x): "
                          f"suspect a slow node or GC, not a hot key")
        if "spill" in v.findings:
            detail.append(f"spilled {_gb(v.spill_bytes)}")
        if "gc_pressure" in v.findings:
            detail.append(f"GC {v.gc_ratio:.0%} of task time")
        if "task_failures" in v.findings:
            detail.append(f"{v.failed_tasks} failed task(s), retried")
        print(_dim(f"       {'; '.join(detail)}"))
        print()

    total_drag = sum(v.drag_ms for v in verdicts) / 1000
    skew_stages = [v for v in verdicts
                   if "straggler" in v.findings or "data_skew" in v.findings]
    print(_bold("=== Trim ==="))
    print(f"  stages flagged : {len(flagged)} of {len(verdicts)}")
    print(f"  total drag     : {total_drag:.1f}s of wall clock spent "
          f"waiting on stragglers")
    print(_dim("  drag is a single-wave lower bound; the real prize is "
               "usually bigger"))

    if args.json_out:
        import json
        Path(args.json_out).write_text(json.dumps(
            [v.to_dict() for v in verdicts], indent=2))
        print(_dim(f"\n  json: {args.json_out}"))

    return 1 if skew_stages else 0


# --- demo command ---
def cmd_demo(args: argparse.Namespace) -> int:
    log = Path(args.data_dir) / "events.jsonl"
    print(_dim("Fabricating an event log from a job having a bad day..."))
    simulate.write_log(log)
    print(f"  log : {log}")
    print()
    return cmd_analyze(argparse.Namespace(
        log=str(log), straggler_ratio=4.0, skew_ratio=4.0, min_tasks=8,
        json_out=None))


def cmd_shanty(args: argparse.Namespace) -> int:
    print()
    print("   Oh the partitions they were even, boys,")
    print("   the shuffle it ran true,")
    print("   till one hot key took the whole broadside")
    print("   and the job it listed too.")
    print()
    print(_dim("   Heave away. Repartition. Heave away."))
    print()
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ballast",
        description="Finds the skew that makes your Spark jobs list. "
                    "Reads event logs; no Spark install needed.")
    p.add_argument("--version", action="version",
                   version=f"ballast {__version__}")
    sub = p.add_subparsers(dest="command")

    pa = sub.add_parser("analyze", help="read an event log, print the trim report")
    pa.add_argument("--log", required=True,
                    help="path to a Spark event log (JSON lines)")
    pa.add_argument("--straggler-ratio", type=float, default=4.0)
    pa.add_argument("--skew-ratio", type=float, default=4.0)
    pa.add_argument("--min-tasks", type=int, default=8)
    pa.add_argument("--json-out", default=None, metavar="PATH")
    pa.set_defaults(func=cmd_analyze)

    pd = sub.add_parser("demo", help="fabricate a bad job, find the problems")
    pd.add_argument("--data-dir", default="data")
    pd.set_defaults(func=cmd_demo)

    ps = sub.add_parser("shanty", help=argparse.SUPPRESS)
    ps.set_defaults(func=cmd_shanty)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 0
    try:
        return args.func(args)
    except FileNotFoundError as e:
        print(_red(str(e)), file=sys.stderr)
        return 2
    except BrokenPipeError:
        try:
            sys.stdout.close()
        except OSError:
            pass
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
