/* MTL Horizon — the admin UI. A few things a page cannot do by itself:
   refreshing itself while it is looked at, showing times in the reader's
   zone, copying an address, asking before something is destroyed. Nothing
   here talks to any server: the refresh is the browser asking for the same
   page again, and that page asks nothing of Horizon or of the Notifier. */
(function () {
  "use strict";

  // -- times: UTC by default; the reader's zone when they ask for it ---------
  var KEY_ZONE = "mtlh-zone";
  function zone() {
    return localStorage.getItem(KEY_ZONE) === "local";
  }
  function showTimes() {
    var local = zone();
    document.querySelectorAll("time[datetime]").forEach(function (node) {
      var text = node.getAttribute("datetime");
      if (!text) return;
      if (!local) { node.textContent = text; return; }
      var date = new Date(text.replace(" UTC", "Z").replace(" ", "T"));
      node.textContent = isNaN(date.getTime()) ? text : date.toLocaleString() + " (моя зона)";
    });
  }
  var zoneToggle = document.getElementById("zone-toggle");
  if (zoneToggle) {
    zoneToggle.checked = zone();
    zoneToggle.addEventListener("change", function () {
      localStorage.setItem(KEY_ZONE, zoneToggle.checked ? "local" : "utc");
      showTimes();
    });
  }

  // -- the refresh: only while the page is looked at ---------------------------
  var KEY_REFRESH = "mtlh-refresh";
  var seconds = parseInt(document.body.getAttribute("data-refresh") || "0", 10);
  var refreshToggle = document.getElementById("refresh-toggle");
  function wanted() {
    return seconds > 0 && localStorage.getItem(KEY_REFRESH) !== "off" && document.visibilityState === "visible";
  }
  if (refreshToggle) {
    refreshToggle.checked = localStorage.getItem(KEY_REFRESH) !== "off";
    refreshToggle.addEventListener("change", function () {
      localStorage.setItem(KEY_REFRESH, refreshToggle.checked ? "on" : "off");
    });
  }
  if (seconds > 0) {
    var timer = null;
    var tick = function () {
      clearTimeout(timer);
      if (wanted()) timer = setTimeout(function () { location.reload(); }, seconds * 1000);
    };
    document.addEventListener("visibilitychange", tick);
    tick();
    // A form the reader is filling in is not taken away by a refresh.
    document.addEventListener("input", function () { clearTimeout(timer); }, { passive: true });
  }

  // -- copying an address ---------------------------------------------------------
  document.addEventListener("click", function (event) {
    var button = event.target.closest(".btn-copy");
    if (!button) return;
    var code = button.parentElement && button.parentElement.querySelector("[data-copy]");
    var value = code ? code.getAttribute("data-copy") : "";
    if (!value) return;
    var done = function () {
      var old = button.textContent;
      button.textContent = "✓";
      setTimeout(function () { button.textContent = old; }, 1200);
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(value).then(done, function () {});
    }
  });

  // -- asking before something is destroyed ---------------------------------------
  document.addEventListener("submit", function (event) {
    var form = event.target.closest("form[data-confirm]");
    if (!form) return;
    if (!window.confirm(form.getAttribute("data-confirm") || "Точно?")) event.preventDefault();
  });

  showTimes();
})();
