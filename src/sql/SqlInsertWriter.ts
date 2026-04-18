import { Writable } from 'node:stream';
import { MeterReading } from '../domain/MeterReading';

export interface SqlInsertWriterOptions {
  /**
   * Rows per INSERT statement. Larger batches mean fewer round-trips
   * when the SQL is replayed, but a bigger single statement to parse.
   * 1000 is the usual sweet spot for Postgres.
   */
  batchSize?: number;
  /**
   * Strategy for the `(nmi, timestamp)` uniqueness conflict already
   * declared on the target table.
   *
   *   - `ignore` (default): emit `ON CONFLICT DO NOTHING`, making the
   *     output idempotent — replaying a file is safe.
   *   - `error`: emit no conflict clause, so a duplicate surfaces as a
   *     constraint-violation error when the SQL is executed.
   */
  onConflict?: 'error' | 'ignore';
}

const DEFAULT_BATCH_SIZE = 1000;

export interface WriteResult {
  readonly rows: number;
  readonly batches: number;
}

/**
 * Consumes an async iterable of MeterReading objects and writes batched,
 * multi-row `INSERT INTO meter_readings` statements to a Writable stream,
 * wrapped in a single transaction.
 *
 * Backpressure-aware: awaits `drain` before queueing more data, so piping
 * to a slow sink (e.g. a gzip stream or a TCP socket) does not balloon the
 * process heap.
 */
export class SqlInsertWriter {
  private readonly batchSize: number;
  private readonly conflictClause: string;

  constructor(options: SqlInsertWriterOptions = {}) {
    this.batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
    if (!Number.isInteger(this.batchSize) || this.batchSize <= 0) {
      throw new RangeError('batchSize must be a positive integer');
    }
    this.conflictClause =
      (options.onConflict ?? 'ignore') === 'ignore'
        ? ' ON CONFLICT ("nmi", "timestamp") DO NOTHING'
        : '';
  }

  async write(
    readings: AsyncIterable<MeterReading>,
    out: Writable,
  ): Promise<WriteResult> {
    await this.writeChunk(out, 'BEGIN;\n');

    let buffer: MeterReading[] = [];
    let rows = 0;
    let batches = 0;

    for await (const reading of readings) {
      buffer.push(reading);
      if (buffer.length >= this.batchSize) {
        await this.flush(buffer, out);
        rows += buffer.length;
        batches++;
        buffer = [];
      }
    }

    if (buffer.length > 0) {
      await this.flush(buffer, out);
      rows += buffer.length;
      batches++;
    }

    await this.writeChunk(out, 'COMMIT;\n');
    return { rows, batches };
  }

  private async flush(batch: MeterReading[], out: Writable): Promise<void> {
    const values = batch.map(r => this.formatRow(r)).join(',\n  ');
    const sql =
      'INSERT INTO meter_readings ("nmi", "timestamp", "consumption") VALUES\n  ' +
      values +
      this.conflictClause +
      ';\n';
    await this.writeChunk(out, sql);
  }

  private formatRow(r: MeterReading): string {
    return `('${escapeSqlString(r.nmi)}', '${formatTimestamp(r.timestamp)}', ${r.consumption})`;
  }

  private writeChunk(out: Writable, text: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const flushed = out.write(text, err => {
        if (err) reject(err);
      });
      if (flushed) resolve();
      else out.once('drain', () => resolve());
    });
  }
}

/**
 * NEM12 timestamps are wall-clock (local to the meter), and the target
 * column is `timestamp` without a timezone. We use UTC accessors purely
 * to avoid the JS Date DST foot-gun: during a DST transition, `getHours`
 * can silently skip or repeat an hour, but `getUTCHours` never does.
 *
 * Format: `YYYY-MM-DD HH:MM:SS` — accepted verbatim by Postgres.
 */
function formatTimestamp(d: Date): string {
  const pad = (n: number): string => n.toString().padStart(2, '0');
  return (
    `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ` +
    `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`
  );
}

/**
 * NMI is validated at parse time (<=10 chars, alphanumeric in practice),
 * so this never has anything to escape today. It stays here so that a
 * future upstream change can't silently turn into a SQL injection vector.
 */
function escapeSqlString(s: string): string {
  return s.replace(/'/g, "''");
}
