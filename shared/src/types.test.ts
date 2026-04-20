import { expect, test } from "bun:test";
import { parseRowPrimaryKey, rowPrimaryKey } from "./types.js";

test("parseRowPrimaryKey returns single-element array for single-PK kinds", () => {
  expect(parseRowPrimaryKey("project", "proj_1")).toEqual(["proj_1"]);
  expect(parseRowPrimaryKey("session", "ses_abc")).toEqual(["ses_abc"]);
  expect(parseRowPrimaryKey("message", "msg_xyz")).toEqual(["msg_xyz"]);
  expect(parseRowPrimaryKey("part", "part_1")).toEqual(["part_1"]);
  expect(parseRowPrimaryKey("permission", "proj_1")).toEqual(["proj_1"]);
  expect(parseRowPrimaryKey("session_share", "ses_abc")).toEqual(["ses_abc"]);
});

test("parseRowPrimaryKey preserves colons in single-PK ids", () => {
  // Regression test for #4: a session id containing a colon must NOT be
  // split. The naive `id.split(":")` would silently drop the deletion.
  expect(parseRowPrimaryKey("session", "ses_part:1234")).toEqual(["ses_part:1234"]);
  expect(parseRowPrimaryKey("message", "msg::weird::id")).toEqual(["msg::weird::id"]);
});

test("parseRowPrimaryKey splits todo ids on the LAST colon", () => {
  // Standard case: <session_id>:<position>
  expect(parseRowPrimaryKey("todo", "ses_1:0")).toEqual(["ses_1", "0"]);
  expect(parseRowPrimaryKey("todo", "ses_1:42")).toEqual(["ses_1", "42"]);

  // Even if a session_id ever contains colons, splitting on the last `:`
  // still recovers the correct (session_id, position) pair.
  expect(parseRowPrimaryKey("todo", "ses_with:colon:7")).toEqual([
    "ses_with:colon",
    "7",
  ]);
});

test("parseRowPrimaryKey returns null for malformed todo ids", () => {
  expect(parseRowPrimaryKey("todo", "no_colon_here")).toBeNull();
});

test("parseRowPrimaryKey is the inverse of rowPrimaryKey", () => {
  // Round-trip every kind to make sure the two helpers stay aligned.
  const cases: Array<{ kind: Parameters<typeof rowPrimaryKey>[0]; row: Record<string, unknown>; expected: string[] }> = [
    { kind: "project", row: { id: "proj_1" }, expected: ["proj_1"] },
    { kind: "session", row: { id: "ses_1" }, expected: ["ses_1"] },
    { kind: "message", row: { id: "msg_1" }, expected: ["msg_1"] },
    { kind: "part", row: { id: "part_1" }, expected: ["part_1"] },
    { kind: "permission", row: { project_id: "proj_1" }, expected: ["proj_1"] },
    { kind: "session_share", row: { session_id: "ses_1" }, expected: ["ses_1"] },
    { kind: "todo", row: { session_id: "ses_1", position: 5 }, expected: ["ses_1", "5"] },
  ];

  for (const { kind, row, expected } of cases) {
    const id = rowPrimaryKey(kind, row);
    const parsed = parseRowPrimaryKey(kind, id);
    expect(parsed).toEqual(expected);
  }
});
