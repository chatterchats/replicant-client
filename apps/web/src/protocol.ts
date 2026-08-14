export type HealthStatus = "healthy" | "degraded" | "unhealthy";

export interface DaemonHealth {
  status: HealthStatus;
  daemon_version: string;
  detail: string | null;
}

export interface HealthEnvelope {
  protocol_version: number;
  payload: DaemonHealth;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

export function parseHealthResponse(value: unknown): HealthEnvelope {
  if (
    !isRecord(value) ||
    typeof value.protocol_version !== "number" ||
    !isRecord(value.payload)
  ) {
    throw new Error("Invalid daemon health response");
  }

  const { status, daemon_version: version, detail } = value.payload;
  if (
    !["healthy", "degraded", "unhealthy"].includes(String(status)) ||
    typeof version !== "string" ||
    (detail !== null && typeof detail !== "string")
  ) {
    throw new Error("Invalid daemon health payload");
  }

  return value as unknown as HealthEnvelope;
}
