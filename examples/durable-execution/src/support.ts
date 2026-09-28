import { spawn } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { MemorySaver } from "../../../libs/checkpoint/src/index.ts";

export interface Effect {
  name: string;
  detail?: string;
}

/**
 * Append-only side-effect log on disk.
 * This is the ground truth for "did the charge run twice?" — it outlives
 * both the graph process and the in-memory checkpointer.
 */
export class EffectJournal {
  constructor(private readonly filePath: string) {
    mkdirSync(path.dirname(filePath), { recursive: true });
    if (!existsSync(filePath)) writeFileSync(filePath, "[]");
  }

  record(name: string, detail?: string): void {
    const effects = this.read();
    effects.push(detail === undefined ? { name } : { name, detail });
    writeFileSync(this.filePath, JSON.stringify(effects));
  }

  read(): Effect[] {
    return JSON.parse(readFileSync(this.filePath, "utf8")) as Effect[];
  }

  count(name: string): number {
    return this.read().filter((effect) => effect.name === name).length;
  }

  names(): string[] {
    return this.read().map((effect) => effect.name);
  }
}

interface Snapshot {
  storage: MemorySaver["storage"];
  writes: MemorySaver["writes"];
}

function isByteView(value: unknown): value is Uint8Array {
  return ArrayBuffer.isView(value) && !Array.isArray(value);
}

function encodeSnapshot(snapshot: Snapshot): string {
  return JSON.stringify(snapshot, (_key, value: unknown) => {
    if (isByteView(value)) {
      return {
        __u8: Buffer.from(
          value.buffer,
          value.byteOffset,
          value.byteLength
        ).toString("base64"),
      };
    }
    return value;
  });
}

function decodeSnapshot(raw: string): Snapshot {
  return JSON.parse(raw, (_key, value: unknown) => {
    if (
      value != null &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      Object.keys(value as object).length === 1 &&
      typeof (value as { __u8?: unknown }).__u8 === "string"
    ) {
      return new Uint8Array(
        Buffer.from((value as { __u8: string }).__u8, "base64")
      );
    }
    return value;
  }) as Snapshot;
}

/**
 * MemorySaver that fsyncs itself to a JSON file after every checkpoint write.
 * Loading a new instance from that file is the stand-in for "the process died
 * and another worker picked up the thread."
 *
 * Not a production checkpointer. It exists so the POC can restart without
 * a native SQLite build.
 */
export class FileSaver extends MemorySaver {
  #tail: Promise<void> = Promise.resolve();

  constructor(private readonly filePath: string) {
    super();
    mkdirSync(path.dirname(filePath), { recursive: true });
    if (existsSync(filePath)) {
      const snapshot = decodeSnapshot(readFileSync(filePath, "utf8"));
      for (const thread of Object.values(snapshot.storage)) {
        for (const namespace of Object.values(thread)) {
          for (const tuple of Object.values(namespace)) {
            // JSON cannot store `undefined`, so a missing parent id comes
            // back as null. MemorySaver treats null as a real parent id.
            if (tuple[2] == null) tuple[2] = undefined;
          }
        }
      }
      this.storage = snapshot.storage;
      this.writes = snapshot.writes;
    }
  }

  override async put(
    ...args: Parameters<MemorySaver["put"]>
  ): ReturnType<MemorySaver["put"]> {
    return this.#exclusive(async () => {
      const config = await super.put(...args);
      this.#flush();
      return config;
    });
  }

  override async putWrites(
    ...args: Parameters<MemorySaver["putWrites"]>
  ): Promise<void> {
    await this.#exclusive(async () => {
      await super.putWrites(...args);
      this.#flush();
    });
  }

  async #exclusive<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.#tail.then(fn, fn);
    this.#tail = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  #flush(): void {
    const payload = encodeSnapshot({
      storage: this.storage,
      writes: this.writes,
    });
    const temporary = `${this.filePath}.tmp`;
    writeFileSync(temporary, payload);
    renameSync(temporary, this.filePath);
  }
}

export interface SaverFootprint {
  checkpoints: number;
  writeGroups: number;
  bytes: number;
}

export function saverFootprint(saver: MemorySaver): SaverFootprint {
  let checkpoints = 0;
  let writeGroups = 0;
  let bytes = 0;

  for (const thread of Object.values(saver.storage)) {
    for (const namespace of Object.values(thread)) {
      for (const tuple of Object.values(namespace)) {
        checkpoints += 1;
        bytes += tuple[0]?.byteLength ?? 0;
        bytes += tuple[1]?.byteLength ?? 0;
      }
    }
  }

  for (const group of Object.values(saver.writes)) {
    writeGroups += 1;
    for (const write of Object.values(group)) {
      bytes += write[2]?.byteLength ?? 0;
    }
  }

  return { checkpoints, writeGroups, bytes };
}

export function scratchDir(prefix: string): string {
  return path.join(
    tmpdir(),
    `lg-durable-${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`
  );
}

export interface WorkerResult {
  code: number | null;
  stdout: string;
  stderr: string;
  json: Record<string, unknown> | undefined;
}

export function runWorker(mode: string, dir: string): Promise<WorkerResult> {
  const script = fileURLToPath(new URL("./kill-worker.ts", import.meta.url));
  const tsxBin = fileURLToPath(
    new URL("../node_modules/.bin/tsx", import.meta.url)
  );

  return new Promise((resolve, reject) => {
    const child = spawn(tsxBin, [script, mode, dir], {
      cwd: fileURLToPath(new URL("..", import.meta.url)),
      env: process.env,
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk: string) => {
      stderr += chunk;
    });
    child.on("error", reject);
    child.on("close", (code) => {
      const line = stdout
        .split("\n")
        .map((entry) => entry.trim())
        .filter((entry) => entry.startsWith("{"))
        .at(-1);
      let json: Record<string, unknown> | undefined;
      if (line !== undefined) {
        json = JSON.parse(line) as Record<string, unknown>;
      }
      resolve({ code, stdout, stderr, json });
    });
  });
}
