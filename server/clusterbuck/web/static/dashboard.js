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

// Usage page only: a full-width, self-refreshing local-vs-cloud activity chart (uPlot —
// vendored, ~50KB, no CDN — over /ui/activity-series). No-ops on pages without the
// #activity-chart element (models, performance) or without uPlot loaded.
(function () {
  if (typeof uPlot === "undefined") return;

  function toUnix(day) {
    return Date.parse(day + "T00:00:00Z") / 1000;
  }

  document.addEventListener("DOMContentLoaded", function () {
    // Deferred to DOMContentLoaded: the script tag runs in <head>, before <body> — and
    // hence #activity-chart — exists.
    var el = document.getElementById("activity-chart");
    if (!el) return;

    var css = getComputedStyle(document.documentElement);
    var muted = css.getPropertyValue("--muted").trim();
    var line = css.getPropertyValue("--line").trim();
    var accent = css.getPropertyValue("--accent").trim();
    var warn = css.getPropertyValue("--warn").trim();

    var chart = null;

    function chartWidth() {
      return el.clientWidth || 900;
    }

    function render(series) {
      var data = [
        series.days.map(toUnix), series.local_jobs, series.cloud_jobs,
        series.avoided_spend, series.cloud_spend,
      ];

      if (chart) {
        chart.setData(data);
        return;
      }

      chart = new uPlot({
        width: chartWidth(),
        height: 260,
        padding: [12, 12, 0, 8],
        series: [
          {},
          {label: "local jobs", stroke: accent, width: 2, scale: "jobs"},
          {label: "cloud jobs", stroke: warn, width: 2, scale: "jobs"},
          {label: "avoided spend $", stroke: accent, width: 1.5, dash: [4, 3], scale: "usd"},
          {label: "cloud spend $", stroke: warn, width: 1.5, dash: [4, 3], scale: "usd"},
        ],
        scales: {x: {time: true}, jobs: {}, usd: {}},
        axes: [
          {stroke: muted, grid: {stroke: line}, ticks: {stroke: line}},
          {scale: "jobs", label: "jobs", stroke: muted, grid: {stroke: line}, ticks: {stroke: line}},
          {scale: "usd", label: "USD", side: 1, stroke: muted, grid: {show: false}, ticks: {stroke: line}},
        ],
        legend: {live: true},
      }, data, el);
    }

    function tick() {
      fetch("/ui/activity-series").then(function (r) { return r.json(); }).then(render);
    }

    tick();
    setInterval(tick, 10000);
    window.addEventListener("resize", function () {
      if (chart) chart.setSize({width: chartWidth(), height: 260});
    });
  });
})();
