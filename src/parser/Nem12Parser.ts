import { MeterReading } from '../domain/MeterReading';
import { Nem12ParseError, Nem12StructureError } from '../errors';
import {
  INTERVAL_VALUES_START_INDEX,
  MAX_NMI_LENGTH,
  MINUTES_PER_DAY,
  RecordType,
} from './RecordTypes';

/**
 * The "current" 200 record context that subsequent 300 records inherit.
 * NEM12 is a hierarchical format: a 200 record opens a block, and every
 * 300 until the next 200 belongs to that block.
 */
interface NmiContext {
  readonly nmi: string;
  readonly intervalLengthMinutes: number;
  readonly intervalsPerDay: number;
}

export interface ParserOptions {
  /**
   * When true, malformed records abort the parse.
   * When false (default), malformed records are reported via `onWarning`
   * and skipped so one bad row doesn't block an otherwise-good batch.
   */
  strict?: boolean;
  /**
   * Invoked with a human-readable message when a non-fatal issue is
   * encountered in lenient mode. Defaults to a no-op.
   */
  onWarning?: (message: string) => void;
}

/**
 * Streaming NEM12 parser.
 *
 * Consumes an async iterable of lines and yields one MeterReading per
 * interval value. The parser holds only `O(1)` state (the current 200
 * block), so memory usage is independent of input size.
 *
 * Usage:
 *
 *   const parser = new Nem12Parser();
 *   for await (const reading of parser.parse(lines)) { ... }
 */
export class Nem12Parser {
  private readonly strict: boolean;
  private readonly onWarning: (message: string) => void;

  constructor(options: ParserOptions = {}) {
    this.strict = options.strict ?? false;
    this.onWarning = options.onWarning ?? (() => {});
  }

  async *parse(lines: AsyncIterable<string>): AsyncGenerator<MeterReading> {
    let lineNumber = 0;
    let currentNmi: NmiContext | null = null;
    let seenHeader = false;
    let seenEndOfData = false;

    for await (const rawLine of lines) {
      lineNumber++;
      const line = rawLine.trim();
      if (line.length === 0) continue;

      if (seenEndOfData) {
        this.report(new Nem12StructureError('data after 900 record', lineNumber, line));
        continue;
      }

      const fields = line.split(',');
      const recordType = fields[0];

      switch (recordType) {
        case RecordType.Header: {
          if (seenHeader) {
            this.report(
              new Nem12StructureError('multiple 100 records', lineNumber, line),
            );
          }
          seenHeader = true;
          break;
        }

        case RecordType.NmiDataDetails: {
          currentNmi = this.parseNmiRecord(fields, lineNumber, line);
          break;
        }

        case RecordType.IntervalData: {
          if (!currentNmi) {
            this.report(
              new Nem12StructureError(
                '300 record encountered before any 200 record',
                lineNumber,
                line,
              ),
            );
            continue;
          }
          yield* this.parseIntervalRecord(fields, lineNumber, line, currentNmi);
          break;
        }

        case RecordType.IntervalEvent:
        case RecordType.B2BDetails:
          // Intentionally ignored for this extraction.
          // Quality events (400) and B2B details (500) are out of scope
          // for the meter_readings table; a follow-up could route them to
          // sibling tables without touching the main pipeline.
          break;

        case RecordType.EndOfData:
          seenEndOfData = true;
          break;

        default:
          this.report(
            new Nem12ParseError(
              `unknown record type "${recordType ?? ''}"`,
              lineNumber,
              line,
            ),
          );
      }
    }

    if (!seenEndOfData) {
      this.report(
        new Nem12StructureError('file ended without a 900 record', lineNumber),
      );
    }
  }

  private parseNmiRecord(
    fields: string[],
    lineNumber: number,
    line: string,
  ): NmiContext {
    const nmi = fields[1] ?? '';
    const intervalLengthStr = fields[8] ?? '';

    if (nmi.length === 0 || nmi.length > MAX_NMI_LENGTH) {
      throw new Nem12ParseError(`invalid NMI "${nmi}"`, lineNumber, line);
    }

    const intervalLengthMinutes = Number.parseInt(intervalLengthStr, 10);
    if (
      !Number.isFinite(intervalLengthMinutes) ||
      intervalLengthMinutes <= 0 ||
      MINUTES_PER_DAY % intervalLengthMinutes !== 0
    ) {
      throw new Nem12ParseError(
        `invalid interval length "${intervalLengthStr}"`,
        lineNumber,
        line,
      );
    }

    return {
      nmi,
      intervalLengthMinutes,
      intervalsPerDay: MINUTES_PER_DAY / intervalLengthMinutes,
    };
  }

  private *parseIntervalRecord(
    fields: string[],
    lineNumber: number,
    line: string,
    ctx: NmiContext,
  ): Generator<MeterReading> {
    const dateStr = fields[1] ?? '';
    const date = this.parseIntervalDate(dateStr);
    if (!date) {
      this.report(new Nem12ParseError(`invalid date "${dateStr}"`, lineNumber, line));
      return;
    }

    const endIndex = INTERVAL_VALUES_START_INDEX + ctx.intervalsPerDay;
    if (fields.length < endIndex) {
      this.report(
        new Nem12ParseError(
          `expected ${ctx.intervalsPerDay} interval values, got ${
            fields.length - INTERVAL_VALUES_START_INDEX
          }`,
          lineNumber,
          line,
        ),
      );
      return;
    }

    for (let i = INTERVAL_VALUES_START_INDEX; i < endIndex; i++) {
      const raw = fields[i] ?? '';
      const consumption = Number.parseFloat(raw);
      if (!Number.isFinite(consumption)) {
        this.report(
          new Nem12ParseError(
            `invalid consumption "${raw}" at field index ${i}`,
            lineNumber,
            line,
          ),
        );
        continue;
      }

      const intervalIndex = i - INTERVAL_VALUES_START_INDEX;
      // NEM12 values are stamped at the END of each interval:
      // the first value on a day covers [00:00, intervalLengthMinutes)
      // and is timestamped at intervalLengthMinutes past midnight.
      const timestamp = new Date(
        date.getTime() + (intervalIndex + 1) * ctx.intervalLengthMinutes * 60_000,
      );

      yield { nmi: ctx.nmi, timestamp, consumption };
    }
  }

  private parseIntervalDate(dateStr: string): Date | null {
    if (!/^\d{8}$/.test(dateStr)) return null;
    const year = Number.parseInt(dateStr.slice(0, 4), 10);
    const month = Number.parseInt(dateStr.slice(4, 6), 10);
    const day = Number.parseInt(dateStr.slice(6, 8), 10);
    const date = new Date(Date.UTC(year, month - 1, day));
    // Reject Feb 30 etc. — Date(...) would silently roll over.
    if (
      date.getUTCFullYear() !== year ||
      date.getUTCMonth() !== month - 1 ||
      date.getUTCDate() !== day
    ) {
      return null;
    }
    return date;
  }

  private report(err: Nem12ParseError): void {
    if (this.strict) throw err;
    this.onWarning(err.message);
  }
}
