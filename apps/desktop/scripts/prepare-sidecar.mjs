import { execFileSync } from "node:child_process";
import { chmodSync, copyFileSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, isAbsolute, join, resolve } from "node:path";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const workspaceRoot = resolve(scriptDir, "../../..");

export function sidecarName(triple, platform = process.platform) {
  return `replicantd-${triple}${platform === "win32" ? ".exe" : ""}`;
}

export function targetDirectory(
  root,
  configured = process.env.CARGO_TARGET_DIR,
) {
  if (!configured) return join(root, "target");
  return isAbsolute(configured) ? configured : resolve(root, configured);
}

export function prepareSidecar(release = false) {
  const triple = execFileSync("rustc", ["--print", "host-tuple"], {
    encoding: "utf8",
  }).trim();
  const cargoArgs = [
    "build",
    "--package",
    "replicant-server",
    "--bin",
    "replicantd",
  ];
  if (release) cargoArgs.push("--release");
  execFileSync("cargo", cargoArgs, { cwd: workspaceRoot, stdio: "inherit" });

  const extension = process.platform === "win32" ? ".exe" : "";
  const profile = release ? "release" : "debug";
  const source = join(
    targetDirectory(workspaceRoot),
    profile,
    `replicantd${extension}`,
  );
  const destinationDir = join(workspaceRoot, "apps/desktop/src-tauri/binaries");
  const destination = join(destinationDir, sidecarName(triple));
  mkdirSync(destinationDir, { recursive: true });
  copyFileSync(source, destination);
  if (process.platform !== "win32") chmodSync(destination, 0o755);
  console.log(`Prepared ${destination}`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  prepareSidecar(process.argv.includes("--release"));
}
