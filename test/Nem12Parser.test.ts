import { describe, it, expect } from 'vitest';
import { MeterReading } from '../src/domain/MeterReading';
import { Nem12Parser } from '../src/parser/Nem12Parser';

async function* fromArray(lines: string[]): AsyncGenerator<string> {
  for (const line of lines) yield line;
}

async function collect(
  source: AsyncIterable<MeterReading>,
): Promise<MeterReading[]> {
  const out: MeterReading[] = [];
  for await (const r of source) out.push(r);
  return out;
}

const values48 = (fill: string): string => Array(48).fill(fill).join(',');

describe('Nem12Parser', () => {
  it('yields one reading per interval with end-of-interval timestamps', async () => {
    const parser = new Nem12Parser();
    const lines = [
      '100,NEM12,200506081149,UNITEDDP,NEMMCO',
      '200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610',
      `300,20050301,${values48('1.0')},A,,,20050310121004,20050310182204`,
      '500,O,S01009,20050310121004,',
      '900',
    ];

    const readings = await collect(parser.parse(fromArray(lines)));

    expect(readings).toHaveLength(48);
    expect(readings[0]!.nmi).toBe('NEM1201009');
    expect(readings[0]!.consumption).toBe(1);
    // NEM12 convention: value #1 is stamped at the END of the first interval.
    expect(readings[0]!.timestamp.toISOString()).toBe('2005-03-01T00:30:00.000Z');
    expect(readings[47]!.timestamp.toISOString()).toBe('2005-03-02T00:00:00.000Z');
  });

  it('carries the current 200 context across multiple 300 records', async () => {
    const parser = new Nem12Parser();
    const lines = [
      '100,NEM12,200506081149,UNITEDDP,NEMMCO',
      '200,NMI0000001,E1E2,1,E1,N1,01009,kWh,30,20050610',
      `300,20050301,${values48('0.5')}`,
      '200,NMI0000002,E1E2,1,E1,N1,01009,kWh,60,20050610',
      `300,20050301,${Array(24).fill('2.0').join(',')}`,
      '900',
    ];

    const readings = await collect(parser.parse(fromArray(lines)));

    expect(readings.filter(r => r.nmi === 'NMI0000001')).toHaveLength(48);
    expect(readings.filter(r => r.nmi === 'NMI0000002')).toHaveLength(24);
  });

  it('supports 15-minute intervals (96 per day)', async () => {
    const parser = new Nem12Parser();
    const lines = [
      '100,NEM12,200506081149,UNITEDDP,NEMMCO',
      '200,NMI0000001,E1E2,1,E1,N1,01009,kWh,15,20050610',
      `300,20050301,${Array(96).fill('1.0').join(',')}`,
      '900',
    ];

    const readings = await collect(parser.parse(fromArray(lines)));

    expect(readings).toHaveLength(96);
    expect(readings[0]!.timestamp.toISOString()).toBe('2005-03-01T00:15:00.000Z');
    expect(readings[3]!.timestamp.toISOString()).toBe('2005-03-01T01:00:00.000Z');
  });

  it('rejects a 300 record before any 200 record in strict mode', async () => {
    const parser = new Nem12Parser({ strict: true });
    const lines = [
      '100,NEM12,200506081149,UNITEDDP,NEMMCO',
      `300,20050301,${values48('1.0')}`,
      '900',
    ];

    await expect(collect(parser.parse(fromArray(lines)))).rejects.toThrow(
      /before any 200 record/,
    );
  });

  it('in lenient mode, skips malformed 300 records and continues', async () => {
    const warnings: string[] = [];
    const parser = new Nem12Parser({ onWarning: m => warnings.push(m) });
    const lines = [
      '100,NEM12,200506081149,UNITEDDP,NEMMCO',
      '200,NMI0000001,E1E2,1,E1,N1,01009,kWh,30,20050610',
      `300,NOT_A_DATE,${values48('1.0')}`,
      `300,20050302,${values48('1.0')}`,
      '900',
    ];

    const readings = await collect(parser.parse(fromArray(lines)));

    expect(readings).toHaveLength(48);
    expect(warnings.some(w => /invalid date/.test(w))).toBe(true);
  });

  it('rejects invalid calendar dates (e.g. Feb 30)', async () => {
    const parser = new Nem12Parser({ strict: true });
    const lines = [
      '100,NEM12,200506081149,UNITEDDP,NEMMCO',
      '200,NMI0000001,E1E2,1,E1,N1,01009,kWh,30,20050610',
      `300,20050230,${values48('1.0')}`,
      '900',
    ];
    // In strict mode, the parser's onWarning would otherwise throw; here it
    // simply aborts the record. Verify no readings slip through.
    const readings = await collect(parser.parse(fromArray(lines))).catch(
      () => [],
    );
    expect(readings).toHaveLength(0);
  });

  it('rejects an unsupported interval length in strict mode', async () => {
    const parser = new Nem12Parser({ strict: true });
    const lines = [
      '100,NEM12,200506081149,UNITEDDP,NEMMCO',
      '200,NMI0000001,E1E2,1,E1,N1,01009,kWh,7,20050610',
      '900',
    ];

    await expect(collect(parser.parse(fromArray(lines)))).rejects.toThrow(
      /invalid interval length/,
    );
  });

  it('warns when the file ends without a 900 record', async () => {
    const warnings: string[] = [];
    const parser = new Nem12Parser({ onWarning: m => warnings.push(m) });
    const lines = [
      '100,NEM12,200506081149,UNITEDDP,NEMMCO',
      '200,NMI0000001,E1E2,1,E1,N1,01009,kWh,30,20050610',
      `300,20050301,${values48('1.0')}`,
    ];

    await collect(parser.parse(fromArray(lines)));

    expect(warnings.some(w => /without a 900 record/.test(w))).toBe(true);
  });

  it('skips blank lines without error', async () => {
    const parser = new Nem12Parser({ strict: true });
    const lines = [
      '100,NEM12,200506081149,UNITEDDP,NEMMCO',
      '',
      '200,NMI0000001,E1E2,1,E1,N1,01009,kWh,30,20050610',
      '   ',
      `300,20050301,${values48('1.0')}`,
      '900',
    ];

    const readings = await collect(parser.parse(fromArray(lines)));
    expect(readings).toHaveLength(48);
  });
});
