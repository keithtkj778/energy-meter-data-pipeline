/**
 * A single meter reading destined for the `meter_readings` table.
 *
 * Kept as a plain interface — domain objects at this boundary are data,
 * not behaviour, and passing them through async generators is faster
 * and more predictable than wrapping them in a class.
 */
export interface MeterReading {
  readonly nmi: string;
  readonly timestamp: Date;
  readonly consumption: number;
}
