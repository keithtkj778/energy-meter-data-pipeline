# Energy Meter Data Pipeline

A memory-efficient, streaming pipeline that converts **NEM12** energy meter-data
files into PostgreSQL `INSERT` statements — built in TypeScript with a focus on
correctness, robustness, and handling files far larger than available memory.

> **NEM12** is the standard flat-file format used across the Australian energy
> market to exchange interval metering data (electricity consumption recorded at
> fixed time intervals). Files can span millions of readings and reach several
> gigabytes.

## Why it's interesting

- **Constant memory, any file size.** Input is streamed line-by-line and readings
  are flushed in configurable batches, so a multi-GB file runs comfortably in a
  low-MB heap — memory use is independent of file size.
- **Clean, testable architecture.** A small pipeline of single-responsibility
  components (line reader → parser state machine → domain model → SQL writer),
  each independently unit-tested.
- **Resilient by default.** Malformed records are logged and skipped so one bad
  row doesn't sink an entire file, with an opt-in `--strict` mode for pipelines
  that need to fail fast.
- **Composable CLI.** Reads from files or stdin and writes to files or stdout, so
  it drops straight into shell pipelines (including `gzip`/`zcat`).

## How it works

```
NEM12 file ─▶ lines ─▶ Nem12Parser ─▶ MeterReading ─▶ SqlInsertWriter ─▶ SQL
         (streamed)   (state machine)   (async gen)      (batched txn)
```

Each interval reading is parsed into a `MeterReading` domain object and written
out in batched, transaction-wrapped `INSERT` statements targeting a
`meter_readings` table.

## Getting started

**Prerequisites:** Node.js 20+ (uses the built-in `node:util` `parseArgs`).

```bash
npm install
npm run build
```

## Usage

```bash
# File in, file out
node dist/index.js --input data.nem12 --output inserts.sql

# Or via stdin / stdout — composes with shell pipelines and gzip
zcat data.nem12.gz | node dist/index.js > inserts.sql

# Run the TypeScript directly during development
npm start -- --input test/fixtures/sample.nem12
```

### Options

```
-i, --input <file>        NEM12 input file (default: stdin)
-o, --output <file>       SQL output file (default: stdout)
    --batch-size <n>      Rows per INSERT statement (default: 1000)
    --strict              Fail on the first malformed record (default: warn + continue)
    --on-conflict <mode>  ignore | error (default: ignore)
-h, --help
```

## Testing

```bash
npm test            # run once
npm run test:watch  # watch mode
npm run typecheck   # tsc --noEmit
```

Three suites cover the pipeline end to end:

- **`Nem12Parser.test.ts`** — parser state machine, interval arithmetic, strict
  vs. lenient behaviour, invalid calendar dates, and unsupported interval lengths.
- **`SqlInsertWriter.test.ts`** — batching, transaction wrapping, conflict mode,
  timestamp formatting, and defensive value escaping.
- **`integration.test.ts`** — a full sample file flows through the entire
  pipeline and asserts the expected row count and representative values.

## Project layout

```
src/
  domain/MeterReading.ts     Domain type for a single row
  errors.ts                  Typed parse errors with line context
  io/FileLineReader.ts       Streaming line reader over fs/stdin
  parser/
    Nem12Parser.ts           State machine over NEM12 records
    RecordTypes.ts           Record-type constants
  sql/SqlInsertWriter.ts     Batched, transactional INSERT writer
  index.ts                   CLI entry point
test/
  Nem12Parser.test.ts
  SqlInsertWriter.test.ts
  integration.test.ts
  fixtures/sample.nem12
```

## Design notes

See [`WRITEUP.md`](./WRITEUP.md) for the reasoning behind the technology choices,
architecture, and the trade-offs made along the way.
