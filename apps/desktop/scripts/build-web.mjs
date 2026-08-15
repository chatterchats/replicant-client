import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const desktopDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
execFileSync("npm", ["--prefix", "../web", "run", "build"], {
  cwd: desktopDir,
  env: {
    ...process.env,
    VITE_REPLICANT_DAEMON_ORIGIN: "http://127.0.0.1:8080",
  },
  stdio: "inherit",
});
