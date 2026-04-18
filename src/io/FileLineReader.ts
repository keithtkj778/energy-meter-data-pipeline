import { createReadStream } from 'node:fs';
import { Readable } from 'node:stream';
import { createInterface } from 'node:readline';

/**
 * Yield lines from a file on disk without loading it into memory.
 *
 * `createReadStream` + `readline` handles any CR/LF mix and gives us an
 * async-iterable of strings, which the parser consumes directly. The 64KB
 * chunk size is Node's default — left explicit so it's tunable in one spot.
 */
export function readLinesFromFile(path: string): AsyncIterable<string> {
  const stream = createReadStream(path, {
    encoding: 'utf8',
    highWaterMark: 64 * 1024,
  });
  return createInterface({ input: stream, crlfDelay: Infinity });
}

/**
 * Yield lines from an arbitrary readable stream — typically `process.stdin`
 * so the CLI composes with shell pipelines.
 */
export function readLinesFromStream(stream: Readable): AsyncIterable<string> {
  return createInterface({ input: stream, crlfDelay: Infinity });
}
