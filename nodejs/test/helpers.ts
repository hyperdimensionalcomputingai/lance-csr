import { mkdtempSync, readdirSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { after } from "node:test";
import { type Table, tableToIPC } from "apache-arrow";

/** Columns as plain arrays, like pyarrow's `to_pydict`; 64-bit integers become numbers. */
export function columns(table: Table): Record<string, unknown[]> {
  const out: Record<string, unknown[]> = {};
  for (const field of table.schema.fields) {
    out[field.name] = [...(table.getChild(field.name) ?? [])].map(plain);
  }
  return out;
}

/** Rows as plain objects, like pyarrow's `to_pylist`. */
export function rows(table: Table): Record<string, unknown>[] {
  return table.toArray().map((row) => plain(row.toJSON()) as Record<string, unknown>);
}

function plain(value: unknown): unknown {
  if (typeof value === "bigint") return Number(value);
  if (value === null || typeof value !== "object") return value;
  if (typeof (value as { toJSON?: unknown }).toJSON === "function") {
    return plain((value as { toJSON(): unknown }).toJSON());
  }
  if (Array.isArray(value)) return value.map(plain);
  if (ArrayBuffer.isView(value)) return Array.from(value as unknown as ArrayLike<unknown>, plain);
  return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, plain(v)]));
}

/** A fresh directory, removed when the test file finishes. */
export function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), "lance-csr-node-"));
  after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

// The fixture writer isn't in the generated loader (it isn't public API), so load the binary
// directly: the one built here, or the one the platform package installed.
const nativeDir = fileURLToPath(new URL("..", import.meta.url));
const binary = readdirSync(nativeDir).find((name) => name.endsWith(".node"));
if (binary === undefined) {
  throw new Error(
    "No native binary in nodejs/: run `npm run build:native -- --features test-utils`",
  );
}
const testUtils = createRequire(import.meta.url)(join(nativeDir, binary)) as {
  _writeLanceDataset: (
    uri: string,
    ipc: Buffer,
    mode: string,
    maxRowsPerFile?: number,
  ) => Promise<number>;
};

/** Write `table` as a Lance dataset and return the new version. */
export async function writeLance(
  uri: string,
  table: Table,
  mode: "create" | "append" | "overwrite" = "create",
  maxRowsPerFile?: number,
): Promise<number> {
  return testUtils._writeLanceDataset(
    uri,
    Buffer.from(tableToIPC(table, "stream")),
    mode,
    maxRowsPerFile,
  );
}
