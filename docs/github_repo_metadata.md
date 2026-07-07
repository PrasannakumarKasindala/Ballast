# GitHub repo metadata

## Repository name

    ballast

## Description (shows under the repo name)

Finds the skew that makes your Spark jobs list. Reads the event logs Spark
already writes: stragglers with their cause (hot key vs slow node), spill,
GC pressure, priced in drag seconds. No Spark install needed; 600MB logs
parsed in 4s.

## Topics

spark, data-skew, stragglers, event-logs, performance, data-engineering,
shuffle, observability, python, hot-key, gc, spill

## `gh` CLI one-liner (after pushing)

```bash
gh repo edit \
  --description "Finds the skew that makes your Spark jobs list. Reads event logs: stragglers with their cause (hot key vs slow node), spill, GC pressure. No Spark install; 600MB logs in 4s." \
  --add-topic spark --add-topic data-skew --add-topic stragglers \
  --add-topic event-logs --add-topic performance --add-topic data-engineering \
  --add-topic shuffle --add-topic observability --add-topic python \
  --add-topic hot-key --add-topic gc --add-topic spill
```

## Suggested pinned-repo caption (for your profile)

> Spark skew diagnostics from event logs alone. Tells a hot key apart from
> a slow node (the read-skew ratio travels with every straggler), counts
> the drag in seconds, and parses 150 MB/s after the benchmark caught the
> parser lying about its own strategy.
