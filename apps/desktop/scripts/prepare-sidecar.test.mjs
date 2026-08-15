import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { sidecarName, targetDirectory } from "./prepare-sidecar.mjs";

test("uses Tauri's target-triple sidecar naming", () => {
  assert.equal(
    sidecarName("x86_64-unknown-linux-gnu", "linux"),
    "replicantd-x86_64-unknown-linux-gnu",
  );
  assert.equal(
    sidecarName("x86_64-pc-windows-msvc", "win32"),
    "replicantd-x86_64-pc-windows-msvc.exe",
  );
});

test("resolves relative Cargo target directories from the workspace", () => {
  assert.equal(
    targetDirectory("/workspace", "artifacts"),
    "/workspace/artifacts",
  );
});

test("packages a loopback sidecar without JavaScript shell permission", () => {
  const desktop = fileURLToPath(new URL("..", import.meta.url));
  const config = JSON.parse(
    readFileSync(`${desktop}/src-tauri/tauri.conf.json`, "utf8"),
  );
  const capability = JSON.parse(
    readFileSync(`${desktop}/src-tauri/capabilities/default.json`, "utf8"),
  );
  assert.deepEqual(config.bundle.externalBin, ["binaries/replicantd"]);
  assert.equal(config.build.beforeDevCommand, "npm --prefix ../web run dev");
  assert.equal(config.build.beforeBuildCommand, "node scripts/build-web.mjs");
  assert.match(config.app.security.csp, /127\.0\.0\.1:8080/);
  assert.deepEqual(capability.permissions, ["core:default"]);
});
