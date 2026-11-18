// curntValv dashboard: polls api/status and follows api/events.
"use strict";

const $ = (id) => document.getElementById(id);
const MAX_FEED = 200;

function el(tag, attrs, ...children) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (k === "class") e.className = v; else e.setAttribute(k, v);
  }
  for (const c of children) e.append(c instanceof Node ? c : document.createTextNode(String(c)));
  return e;
}

function badge(state) {
  return el("span", { class: "badge s-" + state }, state || "–");
}

function bytes(n) {
  if (n < 1024) return n + " B";
  const units = "KMGTPE";
  let i = -1;
  do { n /= 1024; i++; } while (n >= 1024 && i < units.length - 1);
  return n.toFixed(1) + " " + units[i] + "iB";
}

function duration(s) {
  if (s % 86400 === 0) return s / 86400 + "d";
  if (s % 3600 === 0) return s / 3600 + "h";
  return s + "s";
}

function renderNodes(view) {
  const body = $("nodes").tBodies[0];
  body.replaceChildren(...view.nodes.map((n) => {
    const num = (v) => el("td", { class: "num" }, n.error ? "–" : v);
    const tr = el("tr", n.addr === view.self ? { class: "self" } : {},
      el("td", {}, n.error ? "?" : n.id),
      el("td", {}, n.addr),
      el("td", n.error ? { title: n.error } : {}, badge(n.state)),
      num(n.term), num(n.first_index), num(n.last_index), num(n.commit_index), num(n.applied_index), num(n.snapshot_index));
    return tr;
  }));
  const leader = view.nodes.find((n) => n.state === "leader");
  $("meta").textContent = "v" + view.version + " · leader: " + (leader ? "node " + leader.id + " (" + leader.addr + ")" : "none");
}

function renderPipeline(p) {
  if (!p) {
    $("source").replaceChildren("no leader");
    $("source-error").textContent = "";
    $("retention").textContent = "–";
    $("retention-sub").textContent = "";
    $("sinks").tBodies[0].replaceChildren();
    return;
  }
  $("source").replaceChildren(p.source_state ? badge(p.source_state) : "not configured");
  $("source-error").textContent = p.source_error || "";

  const r = p.retention;
  if (r) {
    const limits = [];
    if (r.max_age_seconds) limits.push("age " + duration(r.max_age_seconds));
    if (r.max_bytes) limits.push("size " + bytes(r.max_bytes));
    $("retention").textContent = limits.length ? limits.join(", ") : "keep everything";
    let sub = "log " + bytes(r.log_bytes) + " on node " + p.node;
    if (r.blocked_by) sub += " · held back by sink " + r.blocked_by;
    else if (r.compact_limit) sub += " · removable up to entry " + r.compact_limit;
    $("retention-sub").textContent = sub;
  }

  const body = $("sinks").tBodies[0];
  if (!p.sinks.length) {
    body.replaceChildren(el("tr", {}, el("td", { colspan: "9" }, "no sinks configured")));
    return;
  }
  body.replaceChildren(...p.sinks.map((s) => el("tr", {},
    el("td", {}, s.name), el("td", {}, s.type), el("td", {}, s.policy), el("td", {}, badge(s.state)),
    el("td", { class: "num" }, s.offset), el("td", { class: "num" }, s.lag),
    el("td", { class: "num" }, s.delivered), el("td", { class: "num" }, s.failures),
    el("td", { class: "err" }, s.last_error || ""))));
}

async function poll() {
  try {
    const resp = await fetch("api/status", { cache: "no-store" });
    if (!resp.ok) throw new Error(resp.status + " " + resp.statusText);
    const view = await resp.json();
    renderNodes(view);
    renderPipeline(view.pipeline);
  } catch (err) {
    $("meta").textContent = "this node is not answering (" + err.message + ")";
  }
  setTimeout(poll, 2000);
}

// --- live feed --------------------------------------------------------------

let source = null;
let paused = false;
const seen = new Set();

function rowText(ch) {
  const row = ch.after || ch.before;
  return row ? JSON.stringify(row) : "";
}

function addChange(ch) {
  if (seen.has(ch.id)) return; // a reconnect resends its last entry
  seen.add(ch.id);
  const feed = $("feed");
  feed.prepend(el("li", {},
    el("span", { class: "op op-" + ch.op }, ch.op),
    el("span", { class: "t" }, ch.table + " · " + ch.id + " · " + ch.commit_time),
    rowText(ch)));
  while (feed.childElementCount > MAX_FEED) {
    feed.lastElementChild.remove();
  }
  if (seen.size > 10 * MAX_FEED) seen.clear();
}

function connect() {
  if (source) source.close();
  const tables = $("feed-table").value.split(",").map((t) => t.trim()).filter(Boolean);
  const qs = tables.map((t) => "table=" + encodeURIComponent(t)).join("&");
  source = new EventSource("api/events" + (qs ? "?" + qs : ""));
  $("feed-state").textContent = "connecting…";
  source.onopen = () => {
    $("feed-state").textContent = "following " + (tables.length ? tables.join(", ") : "all tables");
  };
  source.onmessage = (e) => {
    if (!paused) addChange(JSON.parse(e.data));
  };
  source.onerror = () => {
    $("feed-state").textContent = "disconnected, retrying…";
  };
}

$("feed-form").addEventListener("submit", (e) => {
  e.preventDefault();
  $("feed").replaceChildren();
  seen.clear();
  connect();
});
$("feed-pause").addEventListener("click", () => {
  paused = !paused;
  $("feed-pause").textContent = paused ? "Resume" : "Pause";
});

poll();
connect();
