// =============================================================================
// PTO Calendar Sync — Google Apps Script
//
// Reads the leave tracker sheet, builds an ICS calendar, and pushes it to the
// Shift Scheduler app.
//
// SETUP:
//   1. Open the Google Sheet → Extensions → Apps Script
//   2. Paste this file's contents into Code.gs
//   3. Set APP_URL and API_KEY below
//   4. Go to Triggers (clock icon in the left sidebar) → Add Trigger:
//        Function:     pushToApp
//        Event source: Time-driven
//        Type:         Minutes timer
//        Interval:     Every 15 minutes
//   5. Authorise when prompted (script needs Sheets + UrlFetch permissions)
//
// You can also run pushToApp() manually from the editor to test immediately.
// =============================================================================

// ── Configuration ────────────────────────────────────────────────────────────
var APP_URL = "https://gls-cx-snow-frontend-scheduler.apps.tools-na100.dev.ole.redhat.com";
var API_KEY = "";            // must match PTO_UPLOAD_API_KEY on the server (leave empty if not set)
var CALENDAR_NAME = "CX Team";
var SHEET_NAME = new Date().getFullYear().toString();  // auto-selects the current year's tab
// ─────────────────────────────────────────────────────────────────────────────

var MONTH_MAP = {
  "Jan": 0, "Feb": 1, "Mar": 2, "Apr": 3, "May": 4, "Jun": 5,
  "Jul": 6, "Aug": 7, "Sep": 8, "Oct": 9, "Nov": 10, "Dec": 11
};

function parseHeaderDate(header) {
  if (!header) return null;

  // Already a Date object (Google Sheets may return these)
  if (typeof header === "object" && typeof header.getFullYear === "function") {
    if (isNaN(header.getTime())) return null;
    return new Date(header.getFullYear(), header.getMonth(), header.getDate());
  }

  var str = header.toString().trim();
  if (!str || str.toLowerCase() === "total") return null;

  // Try "D Mon YYYY" format first (e.g. "1 Jan 2026")
  var parts = str.split(" ");
  if (parts.length === 3) {
    var day = parseInt(parts[0], 10);
    var mon = MONTH_MAP[parts[1]];
    var year = parseInt(parts[2], 10);
    if (!isNaN(day) && mon !== undefined && !isNaN(year)) {
      return new Date(year, mon, day);
    }
  }

  // Fallback: let JS parse the string (handles Date.toString() output,
  // "Jan 06 2026 04:00:00 GMT+0000", ISO strings, etc.)
  var d = new Date(str);
  if (!isNaN(d.getTime())) {
    return new Date(d.getFullYear(), d.getMonth(), d.getDate());
  }
  return null;
}

function fmtDate(d) {
  var y = d.getFullYear();
  var m = ("0" + (d.getMonth() + 1)).slice(-2);
  var day = ("0" + d.getDate()).slice(-2);
  return "" + y + m + day;
}

function addDays(d, n) {
  var r = new Date(d);
  r.setDate(r.getDate() + n);
  return r;
}

function normaliseLeaveType(raw) {
  var text = (raw || "").toString().trim();
  if (!text) return "";
  var lower = text.toLowerCase();
  if (lower === "bh") return "Bank Holiday";
  if (lower === "leave") return "Leave";
  if (lower === "holiday") return "Holiday";
  return text;
}

function generateUid() {
  var chars = "abcdef0123456789";
  var segments = [8, 4, 4, 4, 12];
  var parts = [];
  for (var s = 0; s < segments.length; s++) {
    var seg = "";
    for (var i = 0; i < segments[s]; i++) {
      seg += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    parts.push(seg);
  }
  return parts.join("-") + "@lx-toolbox";
}

// ── ICS builder ──────────────────────────────────────────────────────────────

function buildIcsFromSheet() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) {
    sheet = ss.getSheets()[0];
  }
  var data = sheet.getDataRange().getValues();
  if (data.length < 2) return null;

  var headers = data[0];
  var dates = [];
  for (var c = 1; c < headers.length; c++) {
    var d = parseHeaderDate(headers[c]);
    if (d) dates.push({ col: c, date: d });
  }

  var lines = [
    "BEGIN:VCALENDAR",
    "VERSION:2.0",
    "PRODID:-//LX Toolbox//Team PTO Calendar//EN",
    "CALSCALE:GREGORIAN",
    "X-WR-CALNAME:" + CALENDAR_NAME
  ];

  for (var r = 1; r < data.length; r++) {
    var member = (data[r][0] || "").toString().trim();
    if (!member) continue;

    var entries = [];
    for (var di = 0; di < dates.length; di++) {
      var col = dates[di].col;
      if (col >= data[r].length) continue;
      var lt = normaliseLeaveType(data[r][col]);
      if (lt) entries.push({ date: dates[di].date, type: lt });
    }
    if (entries.length === 0) continue;

    // Merge consecutive days with the same leave type
    var merged = [];
    var runStart = entries[0].date;
    var runEnd = entries[0].date;
    var runType = entries[0].type;

    for (var ei = 1; ei < entries.length; ei++) {
      var e = entries[ei];
      var nextDay = addDays(runEnd, 1);
      if (e.type === runType && e.date.getTime() === nextDay.getTime()) {
        runEnd = e.date;
      } else {
        merged.push({ start: runStart, end: runEnd, type: runType });
        runStart = e.date;
        runEnd = e.date;
        runType = e.type;
      }
    }
    merged.push({ start: runStart, end: runEnd, type: runType });

    for (var mi = 0; mi < merged.length; mi++) {
      var ev = merged[mi];
      lines.push("BEGIN:VEVENT");
      lines.push("UID:" + generateUid());
      lines.push("SUMMARY:" + member);
      lines.push("DESCRIPTION:" + ev.type);
      lines.push("DTSTART;VALUE=DATE:" + fmtDate(ev.start));
      lines.push("DTEND;VALUE=DATE:" + fmtDate(addDays(ev.end, 1)));
      lines.push("TRANSP:TRANSPARENT");
      lines.push("END:VEVENT");
    }
  }

  lines.push("END:VCALENDAR");
  return lines.join("\r\n");
}

// ── Push to app ──────────────────────────────────────────────────────────────

function pushToApp() {
  var ics = buildIcsFromSheet();
  if (!ics) {
    Logger.log("No ICS data generated — sheet may be empty");
    return;
  }

  var url = APP_URL + "/api/pto/upload_ics?name=" + encodeURIComponent(CALENDAR_NAME);
  var options = {
    method: "post",
    contentType: "text/calendar",
    payload: ics,
    muteHttpExceptions: true
  };
  if (API_KEY) {
    options.headers = { "X-Api-Key": API_KEY };
  }

  var response = UrlFetchApp.fetch(url, options);
  var code = response.getResponseCode();
  var body = response.getContentText();
  Logger.log("PTO sync response (" + code + "): " + body);

  if (code !== 200) {
    throw new Error("PTO sync failed (" + code + "): " + body);
  }
}
