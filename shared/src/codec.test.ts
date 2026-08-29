import { expect, test } from "bun:test";
import { decodeMsgpack, encodeMsgpack, isMsgpackContentType, acceptsMsgpack } from "./codec.js";

test("msgpack round-trips a sync envelope page", () => {
  const page = {
    server_seq: 12,
    envelopes: [
      {
        id: "ses_1",
        kind: "session",
        machine_id: "desktop",
        time_updated: 100,
        server_seq: 12,
        deleted: false,
        data: { id: "ses_1", title: "hello", nested: { n: 1 } },
      },
    ],
    more: false,
  };
  const decoded = decodeMsgpack(encodeMsgpack(page));
  expect(decoded).toEqual(page);
});

test("isMsgpackContentType matches vendor and charset variants", () => {
  expect(isMsgpackContentType("application/msgpack")).toBe(true);
  expect(isMsgpackContentType("application/msgpack; charset=utf-8")).toBe(true);
  expect(isMsgpackContentType("application/json")).toBe(false);
  expect(isMsgpackContentType(null)).toBe(false);
});

test("acceptsMsgpack reads Accept", () => {
  expect(acceptsMsgpack("application/msgpack, application/json")).toBe(true);
  expect(acceptsMsgpack("application/json")).toBe(false);
});
