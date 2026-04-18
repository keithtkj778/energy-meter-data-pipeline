export class Nem12ParseError extends Error {
  constructor(
    message: string,
    public readonly lineNumber: number,
    public readonly lineContent?: string,
  ) {
    super(`[line ${lineNumber}] ${message}`);
    this.name = 'Nem12ParseError';
  }
}

/**
 * Structural violations (missing 200 before 300, orphan data after 900, etc).
 * Distinct from {@link Nem12ParseError} so callers can treat them as
 * "the file is not NEM12" rather than "one record is bad".
 */
export class Nem12StructureError extends Nem12ParseError {
  constructor(message: string, lineNumber: number, lineContent?: string) {
    super(message, lineNumber, lineContent);
    this.name = 'Nem12StructureError';
  }
}
