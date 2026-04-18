import { PassThrough } from 'node:stream';
import { describe, it, expect } from 'vitest';
import { MeterReading } from '../src/domain/MeterReading';
import { SqlInsertWriter } from '../src/sql/SqlInsertWriter';

async function* fromArray(items: MeterReading[]): AsyncGenerator<MeterReading> {
  for (const r of items) yield r;
}

async function captureOutput(source: PassThrough): Promise<string> {
  let out = '';
  for await (const chunk of source) out += chunk.toString();
  return out;
}

const sampleReading = (iso: string, consumption: number, nmi = 'NMI1'): MeterReading => ({
  nmi,
  timestamp: new Date(iso),
  consumption,
});

describe('SqlInsertWriter', () => {
  it('wraps output in a transaction and batches rows', async () => {
    const writer = new SqlInsertWriter({ batchSize: 2 });
    const stream = new PassThrough();
    const captured = captureOutput(stream);

    const result = await writer.write(
      fromArray([
        sampleReading('2005-03-01T00:30:00Z', 1.0),
        sampleReading('2005-03-01T01:00:00Z', 2.0),
        sampleReading('2005-03-01T01:30:00Z', 3.0),
      ]),
      stream,
    );
    stream.end();
    const output = await captured;

    expect(result).toEqual({ rows: 3, batches: 2 });
    expect(output.startsWith('BEGIN;')).toBe(true);
    expect(output.trimEnd().endsWith('COMMIT;')).toBe(true);
    expect((output.match(/INSERT INTO meter_readings/g) ?? []).length).toBe(2);
    expect(output).toContain('ON CONFLICT ("nmi", "timestamp") DO NOTHING');
  });

  it('omits ON CONFLICT when configured to error on conflict', async () => {
    const writer = new SqlInsertWriter({ batchSize: 10, onConflict: 'error' });
    const stream = new PassThrough();
    const captured = captureOutput(stream);

    await writer.write(
      fromArray([sampleReading('2005-03-01T00:30:00Z', 1.0)]),
      stream,
    );
    stream.end();
    const output = await captured;

    expect(output).not.toContain('ON CONFLICT');
  });

  it('formats timestamps as Postgres-compatible naive strings', async () => {
    const writer = new SqlInsertWriter({ batchSize: 10 });
    const stream = new PassThrough();
    const captured = captureOutput(stream);

    await writer.write(
      fromArray([sampleReading('2005-03-01T00:30:00Z', 0.461, 'NEM1201009')]),
      stream,
    );
    stream.end();
    const output = await captured;

    expect(output).toContain("('NEM1201009', '2005-03-01 00:30:00', 0.461)");
  });

  it('escapes single quotes in NMI defensively', async () => {
    const writer = new SqlInsertWriter({ batchSize: 10 });
    const stream = new PassThrough();
    const captured = captureOutput(stream);

    await writer.write(
      fromArray([sampleReading('2005-03-01T00:30:00Z', 1, "a'b")]),
      stream,
    );
    stream.end();
    const output = await captured;

    expect(output).toContain("'a''b'");
  });

  it('rejects non-positive batch sizes', () => {
    expect(() => new SqlInsertWriter({ batchSize: 0 })).toThrow(RangeError);
    expect(() => new SqlInsertWriter({ batchSize: -1 })).toThrow(RangeError);
  });

  it('emits no INSERT when there are no readings', async () => {
    const writer = new SqlInsertWriter({ batchSize: 10 });
    const stream = new PassThrough();
    const captured = captureOutput(stream);

    const result = await writer.write(fromArray([]), stream);
    stream.end();
    const output = await captured;

    expect(result).toEqual({ rows: 0, batches: 0 });
    expect(output).not.toContain('INSERT INTO');
    expect(output).toContain('BEGIN;');
    expect(output).toContain('COMMIT;');
  });
});
