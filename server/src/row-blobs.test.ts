import { afterEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { RowBlobStore, sha256Utf8 } from "./row-blobs.js";

const dirs: string[] = [];
afterEach(() => {
  while (dirs.length > 0) fs.rmSync(dirs.pop()!, { recursive: true, force: true });
});

test("putJson is content-addressed and round-trips", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "row-blobs-"));
  dirs.push(dir);
  const store = new RowBlobStore(dir);
  const json = JSON.stringify({ hello: "world", n: 1 });
  const sha = store.putJson(json);
  expect(sha).toBe(sha256Utf8(json));
  expect(store.getJson(sha)).toBe(json);
  expect(store.putJson(json)).toBe(sha);
  expect(fs.readdirSync(path.join(dir, sha.slice(0, 2))).length).toBe(1);
});

test("getJson returns null for a truncated gzip", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "row-blobs-"));
  dirs.push(dir);
  const store = new RowBlobStore(dir);
  const json = JSON.stringify({ a: 1 });
  const sha = store.putJson(json);
  fs.writeFileSync(store.pathFor(sha), "not gzip");
  expect(store.getJson(sha)).toBeNull();
});
