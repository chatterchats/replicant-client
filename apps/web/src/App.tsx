import { useEffect, useMemo, useState } from "react";

import { useDaemonConnection, useDaemonHealth } from "./daemon";

const navigation = [
  ["Operations", ["Overview", "Galaxy", "System"]],
  ["Assets", ["Devices", "Inventory", "Autofactory", "Cargo"]],
  ["Missions", ["Survey", "Mining", "Relay", "Events", "Bootstrap", "Trade"]],
  ["Automation", ["Automations", "Requirements", "History"]],
  [
    "Intelligence",
    ["Reports", "Messages", "Network", "Standing", "Leaderboards"],
  ],
] as const;

const commands = [...navigation.flatMap(([, items]) => items), "Settings"];

export function App() {
  const [selected, setSelected] = useState("Overview");
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [query, setQuery] = useState("");
  const health = useDaemonHealth();
  const { connection, syncing, revision } = useDaemonConnection();

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setPaletteOpen((open) => !open);
      }
      if (event.key === "Escape") setPaletteOpen(false);
    };
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
    };
  }, []);

  const matches = useMemo(
    () =>
      commands.filter((command) =>
        command.toLowerCase().includes(query.trim().toLowerCase()),
      ),
    [query],
  );

  const navigate = (destination: string) => {
    setSelected(destination);
    setPaletteOpen(false);
    setQuery("");
  };

  const status =
    connection === "connected" ? (health?.status ?? "healthy") : connection;

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <header className="brand">
          <span className="brand-mark">RS</span>
          <span>
            <strong>Replicant Space</strong>
            <small>Application console</small>
          </span>
        </header>
        <nav aria-label="Primary navigation">
          {navigation.map(([group, items]) => (
            <section key={group}>
              <h2>{group}</h2>
              {items.map((item) => (
                <button
                  className={selected === item ? "active" : ""}
                  key={item}
                  onClick={() => {
                    navigate(item);
                  }}
                >
                  {item}
                </button>
              ))}
            </section>
          ))}
        </nav>
        <button
          className={selected === "Settings" ? "active settings" : "settings"}
          onClick={() => {
            navigate("Settings");
          }}
        >
          Settings
        </button>
      </aside>

      <main>
        <header className="status-bar">
          <span className={`status-dot ${status}`} aria-hidden="true" />
          <span>replicantd: {status}</span>
          {health ? <small>v{health.daemon_version}</small> : null}
          {revision === null ? null : <small>revision {revision}</small>}
          <button
            className="palette-trigger"
            onClick={() => {
              setPaletteOpen(true);
            }}
          >
            Commands <kbd>⌘K</kbd>
          </button>
        </header>
        <article className="page">
          <p className="eyebrow">Operations</p>
          <h1>{selected}</h1>
          <p className="lede">
            The web application shell is ready. Live state and commands will
            come from the local daemon protocol.
          </p>
          <section className="connection-card">
            <span className={`status-dot ${status}`} aria-hidden="true" />
            <div>
              <strong>Daemon connection</strong>
              <p>
                {health?.detail ??
                  (connection === "offline"
                    ? "Start replicantd to connect."
                    : syncing
                      ? "Synchronizing daemon state…"
                      : "Daemon state is current.")}
              </p>
            </div>
          </section>
        </article>
      </main>

      {paletteOpen ? (
        <div
          className="palette-backdrop"
          role="presentation"
          onMouseDown={() => {
            setPaletteOpen(false);
          }}
        >
          <section
            className="palette"
            role="dialog"
            aria-modal="true"
            aria-label="Command palette"
            onMouseDown={(event) => {
              event.stopPropagation();
            }}
          >
            <input
              autoFocus
              aria-label="Search commands"
              placeholder="Go to…"
              value={query}
              onChange={(event) => {
                setQuery(event.target.value);
              }}
            />
            <div className="palette-results">
              {matches.map((command) => (
                <button
                  key={command}
                  onClick={() => {
                    navigate(command);
                  }}
                >
                  {command}
                </button>
              ))}
              {matches.length === 0 ? <p>No commands found.</p> : null}
            </div>
          </section>
        </div>
      ) : null}
    </div>
  );
}
