import { describe, expect, it } from "vitest";

import {
  defaultMessageFilterState,
  filterInboxMessagesWithState,
  loadMessageFilterState,
  messageTitleFamily,
  saveMessageFilterState,
} from "./MessagesPage";
import type { InboxMessageSummary } from "./protocol";

function message(
  id: number,
  title: string,
  messageType: string,
  createdAt: string,
  overrides: Partial<InboxMessageSummary> = {},
): InboxMessageSummary {
  return {
    id,
    title,
    body: `${title} body`,
    category: "alert",
    message_type: messageType,
    is_read: true,
    created_at: createdAt,
    ...overrides,
  };
}

describe("Messages filters", () => {
  it("classifies the recurring persisted title families", () => {
    expect(messageTitleFamily("Salvage discovered: Crashed Vessel")).toEqual({
      key: "salvage-discovered",
      label: "Salvage Discovered",
    });
    expect(
      messageTitleFamily("New resource site discovered: SOL-BELT-1-SITE-2"),
    ).toEqual({
      key: "new-resource-site-discovered",
      label: "New Resource Site Discovered",
    });
    expect(
      messageTitleFamily("Broadcast intercepted: Construction Boom"),
    ).toEqual({
      key: "broadcast-intercepted",
      label: "Broadcast Intercepted",
    });
    expect(messageTitleFamily("System hub activated in SOL")).toEqual({
      key: "system-hub-activated",
      label: "System Hub Activated",
    });
    expect(messageTitleFamily("A one-off administrative notice")).toEqual({
      key: "other",
      label: "Other",
    });
  });

  it("excludes deselected title families without forcing one exclusive selection", () => {
    const messages = [
      message(
        1,
        "Salvage discovered: Crashed Vessel",
        "discovery",
        "2026-09-01T12:00:00Z",
      ),
      message(
        2,
        "Broadcast intercepted: Construction Boom",
        "discovery",
        "2026-09-02T12:00:00Z",
      ),
      message(
        3,
        "Event completed: Construction Boom",
        "notification",
        "2026-09-03T12:00:00Z",
      ),
    ];
    const filters = {
      ...defaultMessageFilterState(),
      excludedTitleFamilies: ["salvage-discovered"],
    };

    expect(
      filterInboxMessagesWithState(messages, filters).map((item) => item.id),
    ).toEqual([3, 2]);
  });

  it("combines type, category, read-state, search, and date exclusions", () => {
    const messages = [
      message(
        1,
        "Broadcast intercepted: Old Event",
        "discovery",
        "2026-08-20T12:00:00Z",
      ),
      message(
        2,
        "Broadcast intercepted: Current Event",
        "discovery",
        "2026-09-02T12:00:00Z",
        { is_read: false },
      ),
      message(
        3,
        "Event completed: Current Event",
        "notification",
        "2026-09-03T12:00:00Z",
        { category: "progression" },
      ),
    ];
    const filters = {
      ...defaultMessageFilterState(),
      search: "current",
      dateFrom: "2026-09-01",
      dateTo: "2026-09-03",
      excludedMessageTypes: ["notification"],
      excludedCategories: ["progression"],
      excludedReadStates: ["read"],
    };

    expect(
      filterInboxMessagesWithState(messages, filters).map((item) => item.id),
    ).toEqual([2]);
  });

  it("persists exclusions so future options stay visible by default", () => {
    const values = new Map<string, string>();
    const storage = {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => {
        values.set(key, value);
      },
    };
    const saved = {
      ...defaultMessageFilterState(),
      search: "signal",
      dateFrom: "2026-09-01",
      excludedMessageTypes: ["discovery"],
      excludedTitleFamilies: ["salvage-discovered"],
    };

    saveMessageFilterState(saved, storage);
    expect(loadMessageFilterState(storage)).toEqual(saved);
    expect(loadMessageFilterState(storage).excludedMessageTypes).not.toContain(
      "future-type",
    );
  });
});
