#!/usr/bin/env node
import { createWriteStream } from 'node:fs';
import process from 'node:process';
import { parseArgs } from 'node:util';
import { readLinesFromFile, readLinesFromStream } from './io/FileLineReader';
import { Nem12Parser } from './parser/Nem12Parser';
import { SqlInsertWriter } from './sql/SqlInsertWriter';

interface CliOptions {
  input: string | undefined;
  output: string | undefined;
  batchSize: number;
  strict: boolean;
  onConflict: 'error' | 'ignore';
}

function parseCli(argv: string[]): CliOptions {
  const { values } = parseArgs({
    args: argv,
    options: {
      input: { type: 'string', short: 'i' },
      output: { type: 'string', short: 'o' },
      'batch-size': { type: 'string', default: '1000' },
      strict: { type: 'boolean', default: false },
      'on-conflict': { type: 'string', default: 'ignore' },
      help: { type: 'boolean', short: 'h', default: false },
    },
    strict: true,
  });

  if (values.help) {
    printHelp();
    process.exit(0);
  }

  const batchSize = Number.parseInt(String(values['batch-size']), 10);
  if (!Number.isInteger(batchSize) || batchSize <= 0) {
    throw new Error('--batch-size must be a positive integer');
  }

  const onConflict = String(values['on-conflict']);
  if (onConflict !== 'ignore' && onConflict !== 'error') {
    throw new Error('--on-conflict must be "ignore" or "error"');
  }

  return {
    input: values.input,
    output: values.output,
    batchSize,
    strict: Boolean(values.strict),
    onConflict,
  };
}

function printHelp(): void {
  process.stdout.write(
    [
      'nem12-parser — convert NEM12 files to meter_readings INSERT statements',
      '',
      'Usage:',
      '  nem12-parser [--input <file>] [--output <file>] [options]',
      '',
      'Reads from stdin when --input is omitted; writes to stdout when --output is omitted.',
      '',
      'Options:',
      '  -i, --input <file>        NEM12 input file',
      '  -o, --output <file>       Output SQL file',
      '      --batch-size <n>      Rows per INSERT statement (default: 1000)',
      '      --strict              Fail on first malformed record (default: warn & continue)',
      '      --on-conflict <mode>  ignore | error (default: ignore)',
      '  -h, --help                Show this help',
      '',
    ].join('\n'),
  );
}

async function main(): Promise<void> {
  const opts = parseCli(process.argv.slice(2));

  const lines = opts.input
    ? readLinesFromFile(opts.input)
    : readLinesFromStream(process.stdin);

  const parser = new Nem12Parser({
    strict: opts.strict,
    onWarning: msg => process.stderr.write(`[warn] ${msg}\n`),
  });

  const writer = new SqlInsertWriter({
    batchSize: opts.batchSize,
    onConflict: opts.onConflict,
  });

  const output = opts.output ? createWriteStream(opts.output) : process.stdout;
  const startedAt = Date.now();

  const { rows, batches } = await writer.write(parser.parse(lines), output);

  if (output !== process.stdout) {
    await new Promise<void>(resolve => output.end(() => resolve()));
  }

  const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(2);
  process.stderr.write(`[done] ${rows} rows in ${batches} batches (${elapsedSec}s)\n`);
}

main().catch((err: Error) => {
  process.stderr.write(`[error] ${err.message}\n`);
  process.exit(1);
});
