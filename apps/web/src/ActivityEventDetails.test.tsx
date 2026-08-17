import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { ActivityEventDetails } from "./ActivityEventDetails";
import type { AccountEventSummary } from "./protocol";

const event: AccountEventSummary = {
  id: "evt-1",
  name: "ami.mining.digest",
  category: "ami",
  device: { kind: "device", id: "CTRL-1" },
  replicant: null,
  system: "SCEPTURUM",
  location: "SCEPTURUM-BELT-1",
  occurred_at: "2026-08-16T12:00:00Z",
  ami_digest: true,
  payload: {
    directive: "mine_resources",
    activity: {
      event_count: 4,
      counts: { "mining.started": 2, "mining.completed": 2 },
    },
    devices: [
      {
        device_code: "DRONE-1",
        status: "active",
        events: 3,
        last_event: "mining.completed",
      },
    ],
    report: {
      location: "SCEPTURUM-BELT-1",
      resources: {
        structural: { actual: 2, desired: 4, exhausted: false },
      },
    },
  },
};

describe("ActivityEventDetails", () => {
  it("renders AMI digests as labelled sections rather than raw JSON", () => {
    const html = renderToStaticMarkup(<ActivityEventDetails event={event} />);
    expect(html).toContain("Directive");
    expect(html).toContain("Mine Resources");
    expect(html).toContain("Managed devices");
    expect(html).toContain("DRONE-1");
    expect(html).toContain("Structural");
    expect(html).toContain("Desired");
    expect(html).not.toContain('"directive"');
  });
});
