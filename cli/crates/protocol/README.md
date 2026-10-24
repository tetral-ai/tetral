# protocol

The types that the other crates share belong here: the wire types of the
protocol with Engine, the bodies of the local API, and, in `ports`, the
interfaces through which peer crates reach each other.

## Design rules

- It depends on no other workspace crate and records nothing.

## Where things are

- `ports.rs`: `ReportSink`, `SessionFactory`, `SessionHandle` and
  `ConnectionPort`.
