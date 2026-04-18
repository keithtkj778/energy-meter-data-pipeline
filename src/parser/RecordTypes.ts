export const RecordType = {
  Header: '100',
  NmiDataDetails: '200',
  IntervalData: '300',
  IntervalEvent: '400',
  B2BDetails: '500',
  EndOfData: '900',
} as const;

export const MAX_NMI_LENGTH = 10;
export const MINUTES_PER_DAY = 1440;

/**
 * NEM12 300-record layout relative to a fields[] split on ','.
 *
 *   [0]   = "300"
 *   [1]   = interval date (YYYYMMDD)
 *   [2..] = consumption values, one per interval in the day
 *   tail  = quality flag, optional reason codes, update timestamps
 *
 * The tail length varies by implementation but the consumption values
 * always start at index 2 and have a fixed length determined by the
 * interval length declared on the parent 200 record.
 */
export const INTERVAL_VALUES_START_INDEX = 2;
