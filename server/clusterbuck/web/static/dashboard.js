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
