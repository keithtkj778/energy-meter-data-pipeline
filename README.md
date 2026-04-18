# nem12-parser

Streaming NEM12 → PostgreSQL `INSERT` converter for the `meter_readings` table.

```
NEM12 file ─▶ lines ─▶ Nem12Parser ─▶ MeterReading ─▶ SqlInsertWriter ─▶ SQL
         (streamed)   (state machine)   (async gen)      (batched txn)
```

Memory usage is independent of file size — only `batchSize` rows are buffered at
any point, so a multi-GB NEM12 file runs comfortably in low-MB heap.

## Prerequisites

- Node.js 20+ (uses the built-in `node:util` `parseArgs`).

## Install and build

```bash
npm install
npm run build
```

## Run

```bash
# File in, file out
node dist/index.js --input data.nem12 --output inserts.sql

# Or via stdin / stdout (composes with shell pipelines and gzip)
zcat data.nem12.gz | node dist/index.js > inserts.sql

# During development — runs TS directly via tsx
npm start -- --input test/fixtures/sample.nem12
```

## Options

```
-i, --input <file>        NEM12 input file (default: stdin)
-o, --output <file>       SQL output file (default: stdout)
    --batch-size <n>      Rows per INSERT statement (default: 1000)
    --strict              Fail on the first malformed record (default: warn + continue)
    --on-conflict <mode>  ignore | error (default: ignore)
-h, --help
```

## Tests

```bash
npm test            # one-shot
npm run test:watch  # watch mode
npm run typecheck   # tsc --noEmit
```

Three suites:

- `Nem12Parser.test.ts` — parser state machine, interval arithmetic, strict vs
  lenient behaviour, invalid calendar dates, unsupported interval lengths.
- `SqlInsertWriter.test.ts` — batching, transaction wrapping, conflict mode,
  timestamp formatting, defensive escaping.
- `integration.test.ts` — the full sample file flows through the pipeline
  end-to-end and asserts the expected row count and representative values.

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

See `WRITEUP.md` for the rationale behind the technology, design, and
trade-offs (Q1–Q3).
