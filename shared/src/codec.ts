import { pack, unpack } from "msgpackr";
import { MSGPACK_CONTENT_TYPE } from "./protocol.js";

export function encodeMsgpack(value: unknown): Uint8Array {
  return pack(value);
}

export function decodeMsgpack(bytes: Uint8Array): unknown {
  return unpack(bytes);
}

export function isMsgpackContentType(header: string | null | undefined): boolean {
  return (header ?? "").toLowerCase().includes("msgpack");
}

export function acceptsMsgpack(acceptHeader: string | null | undefined): boolean {
  return (acceptHeader ?? "").toLowerCase().includes("msgpack");
}

export { MSGPACK_CONTENT_TYPE };
