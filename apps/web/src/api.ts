import { parseHealthResponse, parseSnapshotResponse } from "./protocol";

async function get(path: string, signal?: AbortSignal): Promise<unknown> {
  const response = await fetch(path, { signal });
  if (!response.ok)
    throw new Error(`replicantd returned ${String(response.status)}`);
  return response.json() as Promise<unknown>;
}

export const daemonApi = {
  async health(signal?: AbortSignal) {
    return parseHealthResponse(await get("/api/health", signal)).payload;
  },
  async snapshot(signal?: AbortSignal) {
    return parseSnapshotResponse(await get("/api/snapshot", signal)).payload;
  },
};
