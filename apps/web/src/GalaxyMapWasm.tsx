import { useEffect, useRef, useState } from "react";

export function GalaxyMapWasm() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    let renderer: { free(): void } | undefined;

    void import("./wasm/galaxy_renderer/galaxy_renderer.js")
      .then(async ({ default: init, GalaxyRenderer }) => {
        const canvas = canvasRef.current;
        if (!canvas) return;

        await init();
        if (controller.signal.aborted) return;
        const galaxy = new GalaxyRenderer(canvas);
        renderer = galaxy;
        const bounds = canvas.getBoundingClientRect();
        galaxy.resize(bounds.width, bounds.height);
        galaxy.render();
      })
      .catch(() => {
        if (!controller.signal.aborted) setError(true);
      });

    return () => {
      controller.abort();
      renderer?.free();
    };
  }, []);

  return (
    <article className="galaxy-map">
      <header>
        <p className="eyebrow">Operations</p>
        <h1>Galaxy</h1>
        {error ? (
          <p className="galaxy-map-error" role="alert">
            WebGL galaxy renderer unavailable.
          </p>
        ) : null}
      </header>
      <canvas ref={canvasRef} aria-label="Interactive galaxy map" />
    </article>
  );
}
