import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
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
    if (!existsSync(dest)) {
      mkdirSync(dirname(dest), { recursive: true });
      writeFileSync(dest, Bun.gzipSync(Buffer.from(json)));
    }
    return sha256;
  }

  getJson(sha256: string): string | null {
    const dest = this.pathFor(sha256);
    if (!existsSync(dest)) return null;
    return Buffer.from(Bun.gunzipSync(readFileSync(dest))).toString("utf8");
  }

  unlink(sha256: string): void {
    const dest = this.pathFor(sha256);
    if (existsSync(dest)) unlinkSync(dest);
  }
}
