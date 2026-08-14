import { describe, expect, it } from "vitest";

import { parseHealthResponse } from "./protocol";

describe("parseHealthResponse", () => {
  it("accepts the versioned daemon health shape", () => {
    expect(
      parseHealthResponse({
        protocol_version: 1,
        payload: { status: "healthy", daemon_version: "0.1.0", detail: null },
      }),
    ).toMatchObject({ protocol_version: 1, payload: { status: "healthy" } });
  });

  it("rejects an untyped backend response", () => {
    expect(() => parseHealthResponse({ status: "healthy" })).toThrow(
      "Invalid daemon health response",
    );
  });
});
