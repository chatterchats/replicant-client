import type { FiniteExecution } from "./protocol";

export interface ResultSection {
  title: string;
  columns: string[];
  rows: Record<string, string>[];
}

function display(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean")
    return value.toString();
  return JSON.stringify(value);
}

function label(value: string): string {
  return value.replaceAll("_", " ");
}

/** Converts an arbitrary safe result into generic sections and tables. */
export function resultSections(value: unknown): ResultSection[] {
  if (!value || typeof value !== "object" || Array.isArray(value))
    return [
      {
        title: "Result",
        columns: ["value"],
        rows: [{ value: display(value) }],
      },
    ];

  const sections: ResultSection[] = [];
  const overview: Record<string, string>[] = [];
  for (const [key, child] of Object.entries(value)) {
    if (Array.isArray(child)) {
      const objects = child.filter(
        (item): item is Record<string, unknown> =>
          !!item && typeof item === "object" && !Array.isArray(item),
      );
      const columns = objects.length
        ? [...new Set(objects.flatMap((item) => Object.keys(item)))]
        : ["value"];
      sections.push({
        title: label(key),
        columns,
        rows: objects.length
          ? objects.map((item) =>
              Object.fromEntries(
                columns.map((column) => [column, display(item[column])]),
              ),
            )
          : child.map((item) => ({ value: display(item) })),
      });
    } else if (child && typeof child === "object") {
      const entries = Object.entries(child as Record<string, unknown>);
      const fields = entries.filter(([, item]) => !Array.isArray(item));
      if (fields.length)
        sections.push({
          title: label(key),
          columns: ["field", "value"],
          rows: fields.map(([field, item]) => ({
            field: label(field),
            value: display(item),
          })),
        });
      for (const [field, items] of entries) {
        if (Array.isArray(items))
          sections.push(
            ...resultSections({ [field]: items }).map((section) => ({
              ...section,
              title: `${label(key)} · ${section.title}`,
            })),
          );
      }
    } else {
      overview.push({ field: label(key), value: display(child) });
    }
  }
  if (overview.length)
    sections.unshift({
      title: "Summary",
      columns: ["field", "value"],
      rows: overview,
    });
  return sections;
}

/** Safe, structured export text from the already-sanitized daemon record. */
export function executionExport(execution: FiniteExecution): string {
  return JSON.stringify(execution, null, 2);
}

/** Extracts bounded action event lines suitable for copying as a log excerpt. */
export function resultLogExcerpt(value: unknown): string {
  if (!value || typeof value !== "object" || Array.isArray(value)) return "";
  const report = (value as Record<string, unknown>).report;
  if (!report || typeof report !== "object" || Array.isArray(report)) return "";
  const events = (report as Record<string, unknown>).events;
  if (!Array.isArray(events)) return "";
  return events
    .slice(0, 100)
    .map((event) => {
      if (!event || typeof event !== "object" || Array.isArray(event))
        return "";
      const item = event as Record<string, unknown>;
      return `[${display(item.kind)}] ${display(item.subject)}: ${display(item.detail)}`;
    })
    .filter(Boolean)
    .join("\n");
}
