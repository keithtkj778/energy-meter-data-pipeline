import { PassThrough } from 'node:stream';
import path from 'node:path';
import { describe, it, expect } from 'vitest';
import { readLinesFromFile } from '../src/io/FileLineReader';
import { Nem12Parser } from '../src/parser/Nem12Parser';
import { SqlInsertWriter } from '../src/sql/SqlInsertWriter';

describe('end-to-end pipeline against sample.nem12', () => {
  it('produces the expected row count from the assessment sample', async () => {
    const fixture = path.join(__dirname, 'fixtures', 'sample.nem12');

    const parser = new Nem12Parser();
    const writer = new SqlInsertWriter({ batchSize: 100 });
    const stream = new PassThrough();
    const chunks: string[] = [];
    stream.on('data', chunk => chunks.push(chunk.toString()));

    const result = await writer.write(
      parser.parse(readLinesFromFile(fixture)),
      stream,
    );
    stream.end();
    const output = chunks.join('');

    // 2 NMIs × 4 days × 48 half-hour intervals.
    expect(result.rows).toBe(2 * 4 * 48);
    expect(output).toContain('NEM1201009');
    expect(output).toContain('NEM1201010');
    // Sanity-check a known value from the fixture.
    expect(output).toContain("'2005-03-01 06:30:00', 0.461");
  });
});
