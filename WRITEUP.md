# Write-up

## Overview

A streaming NEM12 → PostgreSQL INSERT converter written in TypeScript. Reads a
NEM12 file (from disk or stdin), parses each record with a line-based state
machine, and emits batched `INSERT INTO meter_readings` statements to stdout or
a file. The full pipeline is backpressure-aware and never holds more than
`batchSize` rows in memory, so a 1 GB input file runs in near-constant memory.

## Q1. Rationale for the technologies

**TypeScript on Node.js 20, zero runtime dependencies, Vitest for tests.**

- **TypeScript is the right tool for this problem.** It gives strong typing
  over the NEM12 record structures, async generator support for streaming,
  and lands natively in any Node-based service without a build translation
  step.
- **Streaming is a first-class Node primitive.** `fs.createReadStream` +
  `readline` gives a zero-dependency async-iterable of lines out of the box —
  which is the cleanest way to solve "handle files of very large sizes".
- **No CSV parser or ORM.** NEM12 is fixed-format with no quoting rules, so a
  CSV library would buy nothing and add supply-chain surface. An ORM would
  actively hurt because the brief asks for raw INSERT statements, not a
  live DB connection.
- **`AsyncGenerator` as the unit of composition.** It gives a linear,
  top-to-bottom mental model (`lines → readings → SQL`) without callbacks or
  event emitters, and makes each stage independently unit-testable.
- **Vitest** over Jest for simpler TypeScript configuration — trivially
  interchangeable if the project standardises on Jest later.

## Q2. What I would do differently with more time

Rough order by ROI:

1. **Direct DB ingestion via `COPY FROM STDIN`.** For production loads,
   `COPY` is 10–50× faster than multi-row INSERTs. A `pg` + `pg-copy-streams`
   sink would replace the SQL writer for the live path, while keeping the SQL
   writer as a side-by-side path for diffing, backfills, and offline debugging.
2. **File-level idempotency.** A `nem12_ingest_log` table keyed on
   `(file_hash, received_at)` so re-running a batch is a deliberate operator
   action, not an accident. The row-level `UNIQUE(nmi, timestamp)` already
   prevents duplicates, but knowing a file completed cleanly avoids wasted work.
3. **Timezone handling.** NEM12 timestamps are wall-clock (typically AEST) with
   no TZ suffix. I've treated them as naive UTC because the target column is
   `timestamp` (without time zone) — which matches the format but silently
   punts on a real-world concern. I'd take a `--business-timezone` flag and
   migrate the schema to `timestamptz` in the same cycle.
4. **Structured logging** (pino) with per-record context — `{ lineNumber, nmi,
   recordType }` — so operators can triage failures from a log aggregator
   rather than reading raw stderr.
5. **Full NEM12 coverage.** 400 records (quality events) and the `Qv` quality
   flag on 300 records are currently discarded. A real ingest pipeline should
   preserve these in a sibling table so downstream reports can distinguish
   estimated from actual data.
6. **Property-based tests** with `fast-check`: generate random-but-valid NEM12
   files and assert invariants (one reading per interval, correct timestamp
   stride, round-trip equality).
7. **Observability hooks.** A progress callback emitting row counts and
   throughput so the caller (CLI today, Lambda tomorrow) can ship CloudWatch
   metrics without touching parser code.
8. **Parallel ingest for multi-NMI files.** Today the pipeline is serial. For
   10 GB+ files, a worker pool sharded by NMI could saturate disk and DB.

## Q3. Rationale for the design choices

### Three-stage pipeline: read → parse → write

The three concerns — I/O, domain logic, persistence — live in three modules
connected by pure-ish seams:

- `readLinesFromFile` / `readLinesFromStream` yield strings.
- `Nem12Parser` consumes strings and yields `MeterReading` domain objects.
- `SqlInsertWriter` consumes `MeterReading` and writes SQL to a `Writable`.

Each is independently testable, each has one reason to change, and a future
"write to S3 as Parquet" requirement replaces only the last stage.

### State machine over callbacks / visitors

The parser is a plain `switch` keyed on record type with one piece of mutable
state (`currentNmi`). NEM12 is inherently sequential and hierarchical, so
modelling it this way is the minimum viable abstraction. I deliberately did
not reach for Chain of Responsibility or Visitor — if and when 400/500 records
become in-scope, they become two more cases in the switch.

### Strict vs lenient mode

Real-world meter data is dirty. Failing the whole file on one bad `300` record
means a single malformed NMI blocks a nightly load, which is the wrong default
for an ingest pipeline. So the default is **lenient + warn to stderr**, with a
`--strict` flag for CI or dev runs where loud failure is preferred. Structural
violations (missing 200 before 300, orphan data after 900) are modelled as a
subclass (`Nem12StructureError`) so callers can treat them differently from
per-record errors — surfacing "the file isn't NEM12" vs "one row is junk"
as distinct signals.

### Batched multi-row INSERTs inside a transaction

- Multi-row INSERTs are the simplest output format that still performs
  acceptably on Postgres (one parse + one round-trip per batch). `COPY` is
  faster but requires a live connection, which the brief explicitly excludes.
- Wrapping in `BEGIN; ... COMMIT;` means a killed process or a broken pipe
  never leaves the table half-loaded.
- `batchSize` defaults to 1000 — the usual sweet spot between statement size
  and round-trip count. It's configurable so operators can tune against
  observed DB behaviour without a rebuild.

### `ON CONFLICT DO NOTHING` as the default

The table already carries `UNIQUE(nmi, timestamp)`. Re-running a partial file
should be safe and quiet, so the default emits `ON CONFLICT DO NOTHING`.
Operators can opt into `--on-conflict error` when they want collisions to
surface loudly (e.g. during a migration).

### UTC-based timestamp arithmetic

All timestamp math goes through UTC accessors to dodge the JS `Date` DST
foot-gun — `setHours` can silently skip or repeat an hour on a DST day, but
`setUTCMinutes` never does. The format emitted (`YYYY-MM-DD HH:MM:SS`, no TZ
suffix) lands in a Postgres `timestamp` column verbatim.

### End-of-interval timestamp convention

NEM12 values are "stamped" at the end of the interval they cover — the first
value on a day covers `[00:00, 00:30)` and is written as `00:30`. This is the
dominant convention in the Australian energy sector and matches how Flo's
downstream analytics (load curves, settlement calcs) would read it. It's
centralised in `parseIntervalRecord` so a migration to a start-of-interval
model is a one-line change.

### Defensive SQL escaping

NMI is validated at parse time to be ≤10 characters, and the NEM12 spec
doesn't allow quoting characters, so today there's nothing to escape. The
`escapeSqlString` helper stays anyway so a future parser change can't silently
become a SQL injection vector. Consumption values are numeric and written
unquoted.

### Zero runtime dependencies

Everything at runtime comes from the Node standard library. This is a
deliberate choice for a data pipeline: fewer transitive dependencies means
fewer CVE emails, faster cold starts (if this ever lands in a Lambda), and no
version drift between the CLI and a future library form of the parser.
