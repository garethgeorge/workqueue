import StreamCache from "stream-cache";
import stream from "stream";

declare interface Task<T> {}

/**
 * A logger for tracking task progress
 */
export interface Logger {
  setProgress(progress: number);
  getProgress(progress: number);
  writeStdout(error: string);
  writeStderr(message: string);
}

export interface LoggerFactory {
  createLogger<T>(task: Task<T>);
}

// TODO: make this a writable stream so that pipe operations can be supported
// to simply pipe process stdout / stderr to the logger
// TODO: split out a separate StreamLogger interface or something of that sort...
export class MemoryLogger implements Logger {
  private progress: number;
  private onProgress: (() => void)[];
  private stream = (new StreamCache() as undefined) as stream.Duplex;

  setProgress(progress: number) {
    this.progress = progress;
    for (const onProgress of this.onProgress) {
      onProgress();
    }
  }

  getProgress(progress: number) {
    return this.progress;
  }

  onProgressChange(callback: () => void) {
    this.onProgress.push(callback);
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
