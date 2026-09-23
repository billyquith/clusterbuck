// Timestamps are stored and served in UTC (contract: ISO-8601 with a 'Z' suffix). Every
// place that displays one renders a UTC fallback server-side (for no-JS clients) plus
// data-utc carrying the full value; this converts those to the viewer's own local time,
// "YYYY-MM-DD HH:MM:SS" 24h, once on load and again after every htmx panel swap.
(function () {
  function pad(n) {
    return n < 10 ? "0" + n : "" + n;
  }

  function formatLocal(iso) {
    var d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) + " " +
           pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
  }

  function formatTimestamps(root) {
    (root || document).querySelectorAll(".ts[data-utc]").forEach(function (el) {
      el.textContent = formatLocal(el.getAttribute("data-utc"));
    });
  }

  document.addEventListener("DOMContentLoaded", function () { formatTimestamps(document); });
  document.addEventListener("htmx:afterSwap", function (e) { formatTimestamps(e.target); });
})();

// Usage page only: a full-width, self-refreshing activity chart (uPlot — vendored, ~50KB,
// no CDN — over /ui/activity-series), switchable between local-vs-cloud and per-worker.
// No-ops on pages without the #activity-chart element or without uPlot loaded.
(function () {
  if (typeof uPlot === "undefined") return;

  var VIEW_KEY = "cbk.activity.view";
  var MAX_SERIES = 8;  // --series-1..8; past that, fold into "other" rather than invent hues

  function toUnix(day) {
    return Date.parse(day + "T00:00:00Z") / 1000;
  }

  function loadView() {
    try { return localStorage.getItem(VIEW_KEY) === "node" ? "node" : "venue"; }
    catch (e) { return "venue"; }
  }

  function saveView(v) {
    try { localStorage.setItem(VIEW_KEY, v); } catch (e) { /* private mode: not remembered */ }
  }

  document.addEventListener("DOMContentLoaded", function () {
    // Deferred to DOMContentLoaded: the script tag runs in <head>, before <body> — and
    // hence #activity-chart — exists.
    var el = document.getElementById("activity-chart");
    if (!el) return;

    var css = getComputedStyle(document.documentElement);
    function tok(name) { return css.getPropertyValue(name).trim(); }
    var muted = tok("--muted"), line = tok("--line"), accent = tok("--accent"), warn = tok("--warn");
    var palette = [];
    for (var i = 1; i <= MAX_SERIES; i++) palette.push(tok("--series-" + i));

    var view = loadView();
    var chart = null;
    // uPlot's setData cannot add or remove series, so a chart is rebuilt whenever its
    // shape changes: a view switch, or a worker's first job appearing in the window.
    var shape = null;

    function chartWidth() {
      return el.clientWidth || 900;
    }

    function axes(yLabel, withUsd) {
      var a = [
        {stroke: muted, grid: {stroke: line}, ticks: {stroke: line}},
        {scale: "jobs", label: yLabel, stroke: muted, grid: {stroke: line}, ticks: {stroke: line}},
      ];
      if (withUsd) {
        a.push({scale: "usd", label: "USD", side: 1, stroke: muted, grid: {show: false}, ticks: {stroke: line}});
      }
      return a;
    }

    function venueSpec(s) {
      return {
        shape: "venue",
        data: [s.days.map(toUnix), s.local_jobs, s.cloud_jobs, s.avoided_spend, s.cloud_spend],
        series: [
          {},
          {label: "local jobs", stroke: accent, width: 2, scale: "jobs"},
          {label: "cloud jobs", stroke: warn, width: 2, scale: "jobs"},
          {label: "avoided spend $", stroke: accent, width: 1.5, dash: [4, 3], scale: "usd"},
          {label: "cloud spend $", stroke: warn, width: 1.5, dash: [4, 3], scale: "usd"},
        ],
        scales: {x: {time: true}, jobs: {}, usd: {}},
        axes: axes("jobs", true),
      };
    }

    function nodeSpec(s) {
      var rows = s.series.slice(0, MAX_SERIES);
      var rest = s.series.slice(MAX_SERIES);
      if (rest.length) {
        // The eighth slot becomes "other" so no worker is ever drawn in a borrowed colour.
        rows = s.series.slice(0, MAX_SERIES - 1);
        var other = s.days.map(function () { return 0; });
        s.series.slice(MAX_SERIES - 1).forEach(function (r) {
          r.jobs.forEach(function (v, i) { other[i] += v; });
        });
        rows.push({key: "other", label: "other", cloud: false, jobs: other});
      }
      var series = [{}];
      var data = [s.days.map(toUnix)];
      rows.forEach(function (r, i) {
        series.push({label: r.label, stroke: palette[i], width: 2, scale: "jobs",
                     dash: r.cloud ? [4, 3] : undefined});
        data.push(r.jobs);
      });
      return {
        shape: "node:" + rows.map(function (r) { return r.key; }).join(","),
        data: data, series: series,
        scales: {x: {time: true}, jobs: {}},
        axes: axes("jobs", false),
        empty: rows.length === 0,
      };
    }

    function render(s) {
      var spec = view === "node" ? nodeSpec(s) : venueSpec(s);
      if (chart && spec.shape === shape) {
        chart.setData(spec.data);
        return;
      }
      if (chart) { chart.destroy(); chart = null; }
      el.textContent = "";
      shape = spec.shape;
      if (spec.empty) {
        el.innerHTML = '<p class="empty">no jobs from any worker in the last 30 days</p>';
        return;
      }
      chart = new uPlot({
        width: chartWidth(), height: 260, padding: [12, 12, 0, 8],
        series: spec.series, scales: spec.scales, axes: spec.axes,
        legend: {live: true},
      }, spec.data, el);
    }

    function tick() {
      var v = view;
      fetch("/ui/activity-series?by=" + v)
        .then(function (r) { return r.json(); })
        .then(function (s) { if (v === view) render(s); });  // drop a reply to a stale view
    }

    var buttons = document.querySelectorAll(".seg [data-view]");
    function markButtons() {
      buttons.forEach(function (b) {
        b.setAttribute("aria-pressed", b.getAttribute("data-view") === view ? "true" : "false");
      });
    }
    buttons.forEach(function (b) {
      b.addEventListener("click", function () {
        var v = b.getAttribute("data-view");
        if (v === view) return;
        view = v;
        saveView(v);
        markButtons();
        tick();
      });
    });
    markButtons();

    tick();
    setInterval(tick, 10000);
    window.addEventListener("resize", function () {
      if (chart) chart.setSize({width: chartWidth(), height: 260});
    });
  });
})();

// Models page tabs. The panels behind a hidden tab keep refreshing — so the proposals
// count on the tab stays live — and the chosen tab lives in the URL hash so a reload or a
// shared link lands on it.
(function () {
  document.addEventListener("DOMContentLoaded", function () {
    var tabs = document.querySelectorAll("nav.tabs [data-tab]");
    if (!tabs.length) return;
    var panels = document.querySelectorAll("[data-tab-panel]");

    function show(name) {
      var found = false;
      tabs.forEach(function (t) { if (t.getAttribute("data-tab") === name) found = true; });
      if (!found) name = tabs[0].getAttribute("data-tab");
      tabs.forEach(function (t) {
        t.setAttribute("aria-selected", t.getAttribute("data-tab") === name ? "true" : "false");
      });
      panels.forEach(function (p) { p.hidden = p.getAttribute("data-tab-panel") !== name; });
    }

    tabs.forEach(function (t) {
      t.addEventListener("click", function () {
        var name = t.getAttribute("data-tab");
        history.replaceState(null, "", "#" + name);
        show(name);
      });
    });
    window.addEventListener("hashchange", function () { show(location.hash.slice(1)); });
    show(location.hash.slice(1));
  });
})();
