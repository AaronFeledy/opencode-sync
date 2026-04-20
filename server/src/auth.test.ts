import { expect, test } from "bun:test";
import { checkAuth } from "./auth.js";

const TOKEN = "s3cret-token-with-some-length";

function reqWithAuth(value: string | null): Request {
  const headers = new Headers();
  if (value !== null) headers.set("authorization", value);
  return new Request("http://localhost/anything", { headers });
}

test("checkAuth accepts the correct bearer token", () => {
  const result = checkAuth(reqWithAuth(`Bearer ${TOKEN}`), TOKEN);
  expect(result).toBeNull();
});

test("checkAuth rejects when Authorization header is missing", async () => {
  const res = checkAuth(reqWithAuth(null), TOKEN);
  expect(res).not.toBeNull();
  expect(res!.status).toBe(401);
  const body = (await res!.json()) as { error: string };
  expect(body.error).toContain("Missing");
});

test("checkAuth rejects malformed Authorization headers", async () => {
  // "Bearer" alone (no space, no token), wrong scheme, wrong casing, and
  // "Bearer " with empty token are all malformed. NOTE: "Bearer a b c" is
  // intentionally NOT in this list — see the next test, which exercises
  // tokens containing spaces.
  for (const malformed of ["Basic abc", "Bearer", "BEARER token", "Bearer "]) {
    const res = checkAuth(reqWithAuth(malformed), TOKEN);
    expect(res).not.toBeNull();
    expect(res!.status).toBe(401);
  }
});

test("checkAuth accepts tokens that contain spaces", () => {
  // Regression: the previous implementation split the header on every space
  // and required exactly two parts, so any token containing a space (e.g.
  // a user-chosen passphrase) was silently rejected with a confusing 401.
  // Treat everything after "Bearer " as the opaque token.
  const tokenWithSpaces = "my pass phrase token";
  const result = checkAuth(reqWithAuth(`Bearer ${tokenWithSpaces}`), tokenWithSpaces);
  expect(result).toBeNull();
});

test("checkAuth rejects an incorrect token of equal length", async () => {
  // Same length as TOKEN but different content — exercises the
  // timingSafeEqual path (lengths must match for it to even compare bytes).
  const wrong = "x".repeat(TOKEN.length);
  expect(wrong.length).toBe(TOKEN.length);
  const res = checkAuth(reqWithAuth(`Bearer ${wrong}`), TOKEN);
  expect(res).not.toBeNull();
  expect(res!.status).toBe(401);
});

test("checkAuth rejects tokens of different lengths without throwing", () => {
  // Regression test for #8: a naive timingSafeEqual without a length check
  // would throw "Input buffers must have the same byte length". The wrapper
  // must short-circuit on length mismatch.
  for (const wrong of ["short", "", TOKEN + "extra", TOKEN.slice(0, -1)]) {
    expect(() => checkAuth(reqWithAuth(`Bearer ${wrong}`), TOKEN)).not.toThrow();
    const res = checkAuth(reqWithAuth(`Bearer ${wrong}`), TOKEN);
    expect(res!.status).toBe(401);
  }
});
