import { createHash, randomBytes } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

export function sha256Utf8(text: string): string {
  return createHash("sha256").update(text).digest("hex");
}

export class RowBlobStore {
  constructor(private readonly root: string) {
    mkdirSync(root, { recursive: true });
  }

  pathFor(sha256: string): string {
    return join(this.root, sha256.slice(0, 2), `${sha256}.gz`);
  }

  putJson(json: string): string {
    const sha256 = sha256Utf8(json);
    const dest = this.pathFor(sha256);
    if (existsSync(dest)) return sha256;
    mkdirSync(dirname(dest), { recursive: true });
    const tmp = `${dest}.${randomBytes(8).toString("hex")}.tmp`;
    writeFileSync(tmp, Bun.gzipSync(Buffer.from(json)));
    try {
      renameSync(tmp, dest);
    } catch (err) {
      try { unlinkSync(tmp); } catch { /* tmp already gone */ }
      if (!existsSync(dest)) throw err;
    }
    return sha256;
  }

  getJson(sha256: string): string | null {
    const dest = this.pathFor(sha256);
    if (!existsSync(dest)) return null;
    try {
      return Buffer.from(Bun.gunzipSync(readFileSync(dest))).toString("utf8");
    } catch {
      return null;
    }
  }

  unlink(sha256: string): void {
    const dest = this.pathFor(sha256);
    if (existsSync(dest)) unlinkSync(dest);
  }
}
