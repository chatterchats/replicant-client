import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "./App";
import { DaemonProvider } from "./daemon";
import { installQueryTelemetry } from "./queryTelemetry";
import { installBrowserTelemetry } from "./telemetry";
import "./styles.css";

const root = document.getElementById("root");

if (!root) throw new Error("Missing application root");

installBrowserTelemetry();
installQueryTelemetry();

createRoot(root).render(
  <StrictMode>
    <DaemonProvider>
      <App />
    </DaemonProvider>
  </StrictMode>,
);
