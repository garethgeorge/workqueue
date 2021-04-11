import StreamCache from "stream-cache";
import stream from "stream";

declare interface Task<T> {}

/**
 * A logger for tracking task progress
 */
export interface Logger {
  /**
   * Sets the progress towards completion as a percentage 0-100
   * @param progress completion percentage 0-100
   */
  setProgress(progress: number): void;
  /**
   * Returns the progress towards completion as a percentage 0-100
   * @returns completion percentage 0-100.
   */
  getProgress(): number;
  writeStdout(error: string): void;
  writeStderr(message: string): void;
}

export interface LoggerFactory {
  createLogger<T>(task: Task<T>);
}

// TODO: make this a writable stream so that pipe operations can be supported
// to simply pipe process stdout / stderr to the logger
// TODO: split out a separate StreamLogger interface or something of that sort...
export class MemoryLogger implements Logger {
  private progress: number = 0;
  private stream = (new StreamCache() as undefined) as stream.Duplex;

  setProgress(progress: number) {
    this.progress = progress;
  }

  getProgress() {
    return this.progress;
  }

  writeStdout(message: string) {
    this.stream.write(message);
  }

  writeStderr(message: string) {
    this.stream.write(message);
  }

  getStream() {
    return this.stream as stream.Readable;
  }
}

export class MemoryLoggerFactory implements LoggerFactory {
  createLogger<T>(task: Task<T>) {
    return new MemoryLogger();
  }
}
