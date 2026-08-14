import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "./App";
import { DaemonProvider } from "./daemon";
import "./styles.css";

const root = document.getElementById("root");

if (!root) throw new Error("Missing application root");

createRoot(root).render(
  <StrictMode>
    <DaemonProvider>
      <App />
    </DaemonProvider>
  </StrictMode>,
);
