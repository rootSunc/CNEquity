import assert from "node:assert/strict";
import test from "node:test";

// A page whose requests come back after the user has moved on must not paint
// over the page they moved to. Seen live: #/ops opened, then the overview's
// slow heatmap response replaced it while the address bar still read #/ops.
test("a render that finishes after the next route does not repaint the page", async () => {
  const pending = new Map();
  const deferred = (path) => new Promise((resolve, reject) => pending.set(path, { resolve, reject }));
  const app = { innerHTML: "", querySelectorAll: () => [] };
  const listeners = {};
  globalThis.location = { search: "", hash: "#/quality/slow-run" };
  globalThis.document = {
    getElementById: (id) => (id === "app" ? app : null),
    addEventListener: () => {},
    querySelectorAll: () => [],
    documentElement: { style: {} },
  };
  globalThis.window = { addEventListener: (name, fn) => { listeners[name] = fn; }, scrollTo: () => {},
    matchMedia: () => ({ matches: false, addEventListener: () => {} }),
  };
  // ECharts sniffs the user agent at import once a window exists.
  Object.defineProperty(globalThis, "navigator", {
    value: { ...globalThis.navigator, userAgent: "node" },
    configurable: true,
  });
  globalThis.fetch = (url) => {
    const path = String(url);
    const body = path.includes("fast-run")
      ? Promise.resolve({ trade_date: "FAST-RUN", run_id: "fast", findings: [], diffs: [] })
      : deferred(path);
    return body.then((json) => ({ ok: true, json: async () => json }));
  };

  await import("../src/main.js");
  assert.ok(pending.has("/api/quality/runs/slow-run"), "the first route is waiting on its request");

  location.hash = "#/quality/fast-run";
  await listeners.hashchange();
  assert.match(app.innerHTML, /FAST-RUN/);

  pending.get("/api/quality/runs/slow-run").resolve({ trade_date: "SLOW-RUN", run_id: "slow", findings: [], diffs: [] });
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.match(app.innerHTML, /FAST-RUN/);
  assert.doesNotMatch(app.innerHTML, /SLOW-RUN/);

  // A late failure must not replace the page with an error either.
  location.hash = "#/quality/slow-fail";
  const failing = listeners.hashchange();
  location.hash = "#/quality/fast-run";
  await listeners.hashchange();
  pending.get("/api/quality/runs/slow-fail").reject(new Error("late failure"));
  await failing;
  assert.match(app.innerHTML, /FAST-RUN/);
  assert.doesNotMatch(app.innerHTML, /late failure/);
});
