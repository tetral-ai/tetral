# observe

How the CLI records belongs here: structured records, the log file, the
declared catalogue of events and metrics, the metrics registry, trace context
and stopwatches.

## Design rules

- It depends on no other workspace crate.
- No crate records any other way; `protocol`, `state` and `sdk`, which may not
  depend on it, record nothing.

## Where things are

- `lib.rs`: this documentation only.
