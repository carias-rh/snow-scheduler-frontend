import json
import logging
import uuid
from datetime import datetime, timezone, timedelta, time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from croniter import croniter
from flask import Flask, jsonify, redirect, render_template, request, url_for
from zoneinfo import ZoneInfo

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BASE_DIR = Path(__file__).parent
DATA_FILE = BASE_DIR / "data" / "state.json"

# Common timezone abbreviation aliases to canonical IANA zones
TZ_ALIASES: Dict[str, str] = {
    "UTC": "UTC",
    "GMT": "Etc/GMT",
    "BST": "Europe/London",           # British Summer Time
    "CET": "Europe/Berlin",
    "CEST": "Europe/Berlin",
    "EET": "Europe/Bucharest",
    "EEST": "Europe/Bucharest",
    "WET": "Europe/Lisbon",
    "WEST": "Europe/Lisbon",
    "IST": "Asia/Kolkata",            # India Standard Time
    "PKT": "Asia/Karachi",
    "JST": "Asia/Tokyo",
    "KST": "Asia/Seoul",
    "AEST": "Australia/Sydney",
    "AEDT": "Australia/Sydney",
    "NZST": "Pacific/Auckland",
    "NZDT": "Pacific/Auckland",
    # North America
    "EST": "America/New_York",
    "EDT": "America/New_York",
    "CST": "America/Chicago",
    "CDT": "America/Chicago",
    "MST": "America/Denver",
    "MDT": "America/Denver",
    "PST": "America/Los_Angeles",
    "PDT": "America/Los_Angeles",
}


def ensure_data_file() -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not DATA_FILE.exists():
        initial_state = {
            "members": [
                {"id": str(uuid.uuid4()), "name": "Alice"},
                {"id": str(uuid.uuid4()), "name": "Bob"},
                {"id": str(uuid.uuid4()), "name": "Charlie"},
            ],
            "schedules": [],
        }
        DATA_FILE.write_text(json.dumps(initial_state, indent=2))


def load_state() -> Dict[str, List[Dict]]:
    ensure_data_file()
    return json.loads(DATA_FILE.read_text())


def save_state(state: Dict[str, List[Dict]]) -> None:
    DATA_FILE.write_text(json.dumps(state, indent=2))


def get_member_map(state: Dict[str, List[Dict]]) -> Dict[str, Dict]:
    return {m["id"]: m for m in state.get("members", [])}


def canonicalize_timezone_name(tz_name: str) -> str:
    name = (tz_name or "").strip()
    if not name:
        raise ValueError("Timezone required")

    # Direct IANA name
    try:
        ZoneInfo(name)
        return name
    except Exception:
        pass

    # Abbreviation alias
    alias = name.upper()
    if alias in TZ_ALIASES:
        # Validate mapped IANA
        ZoneInfo(TZ_ALIASES[alias])
        return TZ_ALIASES[alias]

    raise ValueError(
        f"Unknown timezone '{tz_name}'. Use IANA (e.g., 'Europe/Berlin', 'America/New_York') "
        f"or a supported abbreviation: {', '.join(sorted(TZ_ALIASES.keys()))}"
    )


def get_now_utc() -> datetime:
    return datetime.now(timezone.utc)


def last_fire_utc(cron_expr: str, tz_name: str, now_utc: datetime) -> Optional[datetime]:
    tz = ZoneInfo(canonicalize_timezone_name(tz_name))
    now_local = now_utc.astimezone(tz)
    itr = croniter(cron_expr, now_local)
    last_local = itr.get_prev(datetime)
    return last_local.astimezone(timezone.utc)


def next_fire_utc(cron_expr: str, tz_name: str, now_utc: datetime) -> Optional[datetime]:
    tz = ZoneInfo(canonicalize_timezone_name(tz_name))
    now_local = now_utc.astimezone(tz)
    itr = croniter(cron_expr, now_local)
    next_local = itr.get_next(datetime)
    return next_local.astimezone(timezone.utc)


def _parse_time_of_day(hhmm: str) -> time:
    hhmm = (hhmm or "").strip()
    if not hhmm:
        raise ValueError("Time value required")
    parts = hhmm.split(":")
    if len(parts) != 2:
        raise ValueError("Time must be HH:MM")
    h = int(parts[0])
    m = int(parts[1])
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError("Time must be HH:MM (00:00-23:59)")
    return time(hour=h, minute=m)


def _is_range_schedule(s: Dict) -> bool:
    return "start_time" in s and s.get("start_time") is not None


def _generate_events_for_schedule(s: Dict, window_start_utc: datetime, window_end_utc: datetime) -> List[Tuple[datetime, str, Dict]]:
    events: List[Tuple[datetime, str, Dict]] = []
    if not s.get("active", True):
        return events

    # Legacy cron-only schedule: only start events; shift ends on next start of any schedule
    if "cron" in s and s.get("cron"):
        try:
            tz = ZoneInfo(canonicalize_timezone_name(s["timezone"]))
        except Exception:
            return events
        # Start iteration a bit before window start to capture an event that may affect active state
        start_minus = window_start_utc - timedelta(days=2)
        base_local = start_minus.astimezone(tz)
        itr = croniter(s["cron"], base_local)
        for _ in range(1000):
            try:
                next_local = itr.get_next(datetime)
            except Exception:
                break
            next_utc = next_local.astimezone(timezone.utc)
            if next_utc >= window_end_utc:
                break
            events.append((next_utc, "start", s))
        return events

    # Range-based schedule: start_time, optional end_time, days list, timezone
    if _is_range_schedule(s):
        try:
            tz = ZoneInfo(canonicalize_timezone_name(s["timezone"]))
        except Exception:
            return events
        try:
            start_t = _parse_time_of_day(s["start_time"])  # required
        except Exception:
            return events
        end_t: Optional[time] = None
        if s.get("end_time"):
            try:
                end_t = _parse_time_of_day(s["end_time"])  # optional
            except Exception:
                end_t = None
        # Days are integers Monday=0 .. Sunday=6
        try:
            days = [int(d) for d in (s.get("days") or [])]
        except Exception:
            days = []

        # Determine local date range to iterate
        local_start = (window_start_utc - timedelta(days=2)).astimezone(tz)
        local_end = (window_end_utc + timedelta(days=1)).astimezone(tz)
        cur_date = datetime(local_start.year, local_start.month, local_start.day, 0, 0, 0, tzinfo=tz)
        while cur_date < local_end:
            if cur_date.weekday() in days:
                start_local = datetime(cur_date.year, cur_date.month, cur_date.day, start_t.hour, start_t.minute, tzinfo=tz)
                start_utc = start_local.astimezone(timezone.utc)
                if start_utc < window_end_utc:
                    events.append((start_utc, "start", s))
                if end_t is not None:
                    # If end before start, rolls over to next day
                    end_day = cur_date
                    if (end_t.hour, end_t.minute) <= (start_t.hour, start_t.minute):
                        end_day = cur_date + timedelta(days=1)
                    end_local = datetime(end_day.year, end_day.month, end_day.day, end_t.hour, end_t.minute, tzinfo=tz)
                    end_utc = end_local.astimezone(timezone.utc)
                    if end_utc > window_start_utc and end_utc < window_end_utc + timedelta(days=1):
                        events.append((end_utc, "end", s))
            cur_date = cur_date + timedelta(days=1)
        return events

    return events


def _generate_all_events(state: Dict[str, List[Dict]], window_start_utc: datetime, window_end_utc: datetime) -> List[Tuple[datetime, str, Dict]]:
    events: List[Tuple[datetime, str, Dict]] = []
    for s in state.get("schedules", []):
        try:
            events.extend(_generate_events_for_schedule(s, window_start_utc, window_end_utc))
        except Exception as e:
            logging.warning("Failed to generate events for schedule %s: %s", s.get("id"), e)
    events.sort(key=lambda e: e[0])
    return events


def _determine_active_at(state: Dict[str, List[Dict]], at_utc: datetime) -> Tuple[Optional[Dict], Optional[datetime]]:
    # Generate events around the timestamp and simulate to find the active schedule and its start time
    window_start = at_utc - timedelta(days=2)
    window_end = at_utc + timedelta(seconds=1)
    events = _generate_all_events(state, window_start, window_end)
    active: Optional[Dict] = None
    active_started: Optional[datetime] = None
    for ts, kind, sched in events:
        if ts > at_utc:
            break
        if kind == "start":
            active = sched
            active_started = ts
        elif kind == "end":
            # Only end the schedule if it is currently active
            if active and active.get("id") == sched.get("id"):
                active = None
                active_started = None
    return active, active_started


def _determine_all_active_at(state: Dict[str, List[Dict]], at_utc: datetime) -> Tuple[List[Dict], Optional[datetime]]:
    """Determine all schedules active at a specific UTC time, allowing overlaps.

    Returns a tuple of (list_of_active_schedules, active_set_started_utc), where
    active_set_started_utc is when the current composition of the active set last changed.
    """
    # Look slightly behind to capture state transitions leading up to this time
    window_start = at_utc - timedelta(days=2)
    window_end = at_utc + timedelta(seconds=1)
    events = _generate_all_events(state, window_start, window_end)

    active_by_id: Dict[str, Dict] = {}
    last_change: Optional[datetime] = None

    def is_cron_schedule(s: Dict) -> bool:
        return bool(s.get("cron")) and not _is_range_schedule(s)

    for ts, kind, sched in events:
        if ts > at_utc:
            break
        sid = sched.get("id")
        if kind == "start":
            if is_cron_schedule(sched):
                # Cron-only model: replaces any currently active schedules
                active_by_id = {sid: sched}
                last_change = ts
            else:
                if sid not in active_by_id:
                    active_by_id[sid] = sched
                    last_change = ts
        elif kind == "end":
            if sid in active_by_id:
                del active_by_id[sid]
                last_change = ts

    active_list = list(active_by_id.values())
    # Provide deterministic ordering by member name for stable UI and round-robin
    member_map = get_member_map(state)
    active_list.sort(key=lambda s: (member_map.get(s.get("member_id"), {}).get("name", ""), s.get("id")))
    return active_list, last_change


def _find_next_start_after(state: Dict[str, List[Dict]], after_utc: datetime) -> Tuple[Optional[Dict], Optional[datetime]]:
    window_start = after_utc
    window_end = after_utc + timedelta(days=7)
    events = _generate_all_events(state, window_start, window_end)
    for ts, kind, sched in events:
        if kind == "start" and ts > after_utc:
            return sched, ts
    return None, None


def compute_current_shift(state: Dict[str, List[Dict]], now_utc: Optional[datetime] = None) -> Tuple[Optional[Dict], Optional[datetime], Optional[Dict], Optional[datetime]]:
    if now_utc is None:
        now_utc = get_now_utc()

    schedules: List[Dict] = [s for s in state.get("schedules", []) if s.get("active", True)]
    if not schedules:
        return None, None, None, None

    current_schedule, current_started_utc = _determine_active_at(state, now_utc)
    next_schedule, next_start_utc = _find_next_start_after(state, now_utc)
    return current_schedule, current_started_utc, next_schedule, next_start_utc


def compute_current_overlaps(state: Dict[str, List[Dict]], now_utc: Optional[datetime] = None) -> Tuple[List[Dict], Optional[datetime]]:
    if now_utc is None:
        now_utc = get_now_utc()
    active_schedules, active_started = _determine_all_active_at(state, now_utc)
    return active_schedules, active_started


def compute_timeline_segments(state: Dict[str, List[Dict]], window_start_utc: datetime, window_end_utc: datetime) -> List[Dict]:
    """Compute continuous segments across the window with possibly multiple active schedules.

    Each returned segment is a dict: { start_utc, end_utc, schedules: [schedule, ...] }.
    """
    schedules: List[Dict] = [s for s in state.get("schedules", []) if s.get("active", True)]
    if not schedules:
        return []

    # Fetch events and establish initial active set at window start
    events = _generate_all_events(state, window_start_utc - timedelta(days=2), window_end_utc)

    active_by_id: Dict[str, Dict] = {}
    def is_cron_schedule(s: Dict) -> bool:
        return bool(s.get("cron")) and not _is_range_schedule(s)

    for ts, kind, sched in events:
        if ts >= window_start_utc:
            break
        sid = sched.get("id")
        if kind == "start":
            if is_cron_schedule(sched):
                active_by_id = {sid: sched}
            else:
                active_by_id[sid] = sched
        elif kind == "end":
            if sid in active_by_id:
                del active_by_id[sid]

    segments: List[Dict] = []
    prev_time = window_start_utc
    for ts, kind, sched in events:
        if ts < window_start_utc:
            continue
        if ts >= window_end_utc:
            break
        if prev_time < ts:
            segments.append({
                "start_utc": prev_time,
                "end_utc": ts,
                "schedules": list(active_by_id.values()),
            })
        sid = sched.get("id")
        if kind == "start":
            if is_cron_schedule(sched):
                active_by_id = {sid: sched}
            else:
                active_by_id[sid] = sched
        elif kind == "end":
            if sid in active_by_id:
                del active_by_id[sid]
        prev_time = ts

    if prev_time < window_end_utc:
        segments.append({
            "start_utc": prev_time,
            "end_utc": window_end_utc,
            "schedules": list(active_by_id.values()),
        })

    # Sort schedules within each segment deterministically by member name
    member_map = get_member_map(state)
    for seg in segments:
        seg["schedules"].sort(key=lambda s: (member_map.get(s.get("member_id"), {}).get("name", ""), s.get("id")))
    return segments


@app.route("/")
def index():
    state = load_state()
    members = state.get("members", [])
    schedules = state.get("schedules", [])

    # Overlapping-aware current members
    current_schedules, current_started_utc = compute_current_overlaps(state)
    current_schedule, _single_started, next_schedule, next_start_utc = compute_current_shift(state)
    member_map = get_member_map(state)

    current_members = [member_map.get(s.get("member_id")) for s in current_schedules]
    current_member = member_map.get(current_schedule["member_id"]) if current_schedule else None
    next_member = member_map.get(next_schedule["member_id"]) if next_schedule else None

    return render_template(
        "index.html",
        members=members,
        schedules=schedules,
        current_member=current_member,
        current_members=current_members,
        current_started_utc=current_started_utc,
        next_member=next_member,
        next_start_utc=next_start_utc,
        new_member_id=request.args.get("new_member_id"),
    )


@app.route("/api/current_shift", methods=["GET"])
def api_current_shift():
    state = load_state()
    current_schedules, current_started_utc = compute_current_overlaps(state)
    current_schedule, _single_started_utc, next_schedule, next_start_utc = compute_current_shift(state)
    member_map = get_member_map(state)
    current_member = member_map.get(current_schedule["member_id"]) if current_schedule else None
    current_members = [member_map.get(s.get("member_id")) for s in current_schedules]
    next_member = member_map.get(next_schedule["member_id"]) if next_schedule else None

    return jsonify({
        "current": {
            "member": current_member,
            "members": current_members,
            "started_utc": current_started_utc.isoformat() if current_started_utc else None
        },
        "next": {
            "member": next_member,
            "start_utc": next_start_utc.isoformat() if next_start_utc else None
        }
    })


@app.route("/members/add", methods=["POST"])
def add_member():
    state = load_state()
    name = request.form.get("name", "").strip()
    if not name:
        return "Name required", 400
    new_member = {"id": str(uuid.uuid4()), "name": name}
    state["members"].append(new_member)
    state["members"] = sorted(state["members"], key=lambda m: m["name"].lower())
    save_state(state)
    logging.info("Added member: %s", name)
    # Redirect with hint to preselect in Add Schedule form
    return redirect(url_for("index", new_member_id=new_member["id"]))


@app.route("/members/delete/<member_id>", methods=["POST"])
def delete_member(member_id: str):
    state = load_state()
    state["members"] = [m for m in state["members"] if m["id"] != member_id]
    state["schedules"] = [s for s in state["schedules"] if s["member_id"] != member_id]
    save_state(state)
    logging.info("Deleted member: %s", member_id)
    return redirect(url_for("index"))


@app.route("/schedule/add", methods=["POST"])
def add_schedule():
    state = load_state()
    timezone_name = request.form.get("timezone", "UTC").strip() or "UTC"
    member_id = request.form.get("member_id", "").strip()

    if not member_id:
        return "member_id required", 400

    try:
        canonical_tz = canonicalize_timezone_name(timezone_name)
    except Exception as e:
        return f"Invalid timezone: {e}", 400

    # New range-based inputs
    start_time = (request.form.get("start_time") or "").strip()
    end_time = (request.form.get("end_time") or "").strip()
    days = request.form.getlist("days")  # list of strings like ["0", "1", ...]

    if start_time:
        # Range-based schedule
        try:
            _ = _parse_time_of_day(start_time)
            if end_time:
                _ = _parse_time_of_day(end_time)
        except Exception as e:
            return f"Invalid time: {e}", 400
        try:
            days_int = [int(d) for d in days]
            for d in days_int:
                if d < 0 or d > 6:
                    raise ValueError("day out of range")
        except Exception:
            return "Invalid days; must be integers 0=Mon .. 6=Sun", 400

        new_schedule = {
            "id": str(uuid.uuid4()),
            "member_id": member_id,
            "start_time": start_time,
            "end_time": end_time or None,
            "days": days_int,
            "timezone": canonical_tz,
            "active": True,
        }
        state["schedules"].append(new_schedule)
        save_state(state)
        logging.info("Added range schedule: %s %s-%s (%s) days=%s", member_id, start_time, end_time or "", canonical_tz, days_int)
        return redirect(url_for("index"))

    # Fallback for legacy cron input (still supported if provided by API or older UI)
    cron = request.form.get("cron", "").strip()
    if not cron:
        return "start_time or cron required", 400
    try:
        _ = next_fire_utc(cron, canonical_tz, get_now_utc())
    except Exception as e:
        return f"Invalid cron: {e}", 400

    new_schedule = {
        "id": str(uuid.uuid4()),
        "member_id": member_id,
        "cron": cron,
        "timezone": canonical_tz,
        "active": True,
    }
    state["schedules"].append(new_schedule)
    save_state(state)
    logging.info("Added cron schedule: %s (%s)", cron, canonical_tz)
    return redirect(url_for("index"))


@app.route("/schedule/delete/<schedule_id>", methods=["POST"])
def delete_schedule(schedule_id: str):
    state = load_state()
    state["schedules"] = [s for s in state["schedules"] if s["id"] != schedule_id]
    save_state(state)
    logging.info("Deleted schedule: %s", schedule_id)
    return redirect(url_for("index"))


@app.route("/schedule/delete", methods=["POST"])
def delete_schedules_bulk():
    """Bulk delete schedules from a list of ids in form field 'schedule_ids'."""
    state = load_state()
    ids = request.form.getlist("schedule_ids")
    if not ids:
        return redirect(url_for("index"))
    before = len(state.get("schedules", []))
    state["schedules"] = [s for s in state.get("schedules", []) if s.get("id") not in ids]
    save_state(state)
    after = len(state.get("schedules", []))
    logging.info("Bulk deleted %d schedules", before - after)
    return redirect(url_for("index"))


@app.route("/schedule/set_active/<schedule_id>", methods=["POST"])
def set_schedule_active(schedule_id: str):
    """Set a schedule's active flag from form field 'active' ("true"/"false" or on/off).
    Returns JSON for fetch-based UI or redirects for graceful fallback.
    """
    state = load_state()
    active_param = (request.form.get("active") or request.args.get("active") or "").strip().lower()
    active_value = active_param in ("1", "true", "on", "yes")

    updated = False
    for s in state.get("schedules", []):
        if s.get("id") == schedule_id:
            s["active"] = active_value
            updated = True
            break
    if updated:
        save_state(state)
        logging.info("Set schedule %s active=%s", schedule_id, active_value)

    # If request prefers JSON (fetch), respond JSON; else redirect
    if request.accept_mimetypes.best == "application/json" or request.headers.get("X-Requested-With") == "fetch":
        return jsonify({"ok": updated, "schedule_id": schedule_id, "active": active_value})
    return redirect(url_for("index"))

@app.route("/api/shift", methods=["GET"])
def api_shift():
    state = load_state()
    now_utc = get_now_utc()
    active_schedules, active_set_started = _determine_all_active_at(state, now_utc)
    member_map = get_member_map(state)
    if not active_schedules:
        return jsonify({
            "id": None,
            "name": None,
            "on_shift": False,
            "round_robin": False,
        })

    if len(active_schedules) == 1:
        only = active_schedules[0]
        member = member_map.get(only.get("member_id"))
        return jsonify({
            "id": member.get("id") if member else None,
            "name": member.get("name") if member else None,
            "on_shift": True,
            "round_robin": False,
        })

    # Round-robin over the overlapping active schedules
    # Build stable ordering (already sorted by member name in _determine_all_active_at)
    group_key_part = "|".join([s.get("id") for s in active_schedules])
    group_time = (active_set_started.isoformat() if active_set_started else "")
    group_key = f"{group_time}|{group_key_part}"

    rr_map = state.get("rr", {})
    prev_index = rr_map.get(group_key, -1)
    next_index = (prev_index + 1) % len(active_schedules)
    rr_map[group_key] = next_index
    state["rr"] = rr_map
    save_state(state)

    selected = active_schedules[next_index]
    member = member_map.get(selected.get("member_id"))
    return jsonify({
        "id": member.get("id") if member else None,
        "name": member.get("name") if member else None,
        "on_shift": True,
        "round_robin": True,
    })


@app.route("/members/delete", methods=["POST"])
def delete_members_bulk():
    """Bulk delete members and their schedules. Form field 'member_ids'."""
    state = load_state()
    ids = set(request.form.getlist("member_ids"))
    if not ids:
        return redirect(url_for("index"))
    before_m = len(state.get("members", []))
    state["members"] = [m for m in state.get("members", []) if m.get("id") not in ids]
    # Remove schedules belonging to deleted members
    state["schedules"] = [s for s in state.get("schedules", []) if s.get("member_id") not in ids]
    save_state(state)
    after_m = len(state.get("members", []))
    logging.info("Bulk deleted %d members and their schedules", before_m - after_m)
    return redirect(url_for("index"))


@app.route("/api/timeline", methods=["GET"])
def api_timeline():
    """Return 24h timeline segments for a given timezone (default UTC) and date.

    Query params:
      - tz: IANA timezone or supported abbreviation, default 'UTC'
      - date: YYYY-MM-DD in the provided timezone; defaults to today in tz
    """
    state = load_state()
    tz_param = request.args.get("tz", "UTC").strip() or "UTC"
    tz_name = canonicalize_timezone_name(tz_param)
    tz = ZoneInfo(tz_name)

    # Determine local day
    date_param = request.args.get("date")
    if date_param:
        try:
            year, month, day = [int(x) for x in date_param.split("-")]
            local_start = datetime(year, month, day, 0, 0, 0, tzinfo=tz)
        except Exception:
            return jsonify({"error": "Invalid date. Use YYYY-MM-DD."}), 400
    else:
        now_local = get_now_utc().astimezone(tz)
        local_start = datetime(now_local.year, now_local.month, now_local.day, 0, 0, 0, tzinfo=tz)

    local_end = local_start + timedelta(days=1)
    window_start_utc = local_start.astimezone(timezone.utc)
    window_end_utc = local_end.astimezone(timezone.utc)

    segments = compute_timeline_segments(state, window_start_utc, window_end_utc)
    member_map = get_member_map(state)

    def seg_to_json(seg: Dict) -> Dict:
        schedules_json: List[Dict] = []
        for s in seg.get("schedules", []):
            schedules_json.append({
                "id": s.get("id"),
                "member": member_map.get(s.get("member_id")),
            })
        return {
            "start_utc": seg["start_utc"].isoformat(),
            "end_utc": seg["end_utc"].isoformat(),
            "schedules": schedules_json,
        }

    return jsonify({
        "window": {
            "tz": tz_name,
            "start_utc": window_start_utc.isoformat(),
            "end_utc": window_end_utc.isoformat(),
        },
        "segments": [seg_to_json(seg) for seg in segments]
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
