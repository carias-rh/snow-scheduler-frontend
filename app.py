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


def compute_current_shift(state: Dict[str, List[Dict]], now_utc: Optional[datetime] = None) -> Tuple[Optional[Dict], Optional[datetime], Optional[Dict], Optional[datetime]]:
    if now_utc is None:
        now_utc = get_now_utc()

    schedules: List[Dict] = state.get("schedules", [])
    if not schedules:
        return None, None, None, None

    latest_last: Tuple[Optional[Dict], Optional[datetime]] = (None, None)
    for s in schedules:
        try:
            lf = last_fire_utc(s["cron"], s["timezone"], now_utc)
        except Exception as e:
            logging.warning("Skipping schedule %s due to timezone/cron error: %s", s.get("id"), e)
            continue
        if lf and (latest_last[1] is None or lf > latest_last[1]):
            latest_last = (s, lf)

    current_schedule, current_started_utc = latest_last

    soonest_next: Tuple[Optional[Dict], Optional[datetime]] = (None, None)
    for s in schedules:
        try:
            nf = next_fire_utc(s["cron"], s["timezone"], now_utc)
        except Exception as e:
            logging.warning("Skipping schedule %s due to timezone/cron error: %s", s.get("id"), e)
            continue
        if nf and (soonest_next[1] is None or nf < soonest_next[1]):
            soonest_next = (s, nf)

    next_schedule, next_start_utc = soonest_next
    return current_schedule, current_started_utc, next_schedule, next_start_utc


def compute_timeline_segments(state: Dict[str, List[Dict]], window_start_utc: datetime, window_end_utc: datetime) -> List[Dict]:
    """Compute continuous segments across the window where the active schedule/member is in effect.

    Rule: The schedule that last fired before a point in time remains active until the next schedule fires.
    """
    schedules: List[Dict] = state.get("schedules", [])
    if not schedules:
        return []

    # Find which schedule is active at the window start
    latest_last: Tuple[Optional[Dict], Optional[datetime]] = (None, None)
    for s in schedules:
        try:
            lf = last_fire_utc(s["cron"], s["timezone"], window_start_utc)
        except Exception:
            continue
        if lf and (latest_last[1] is None or lf > latest_last[1]):
            latest_last = (s, lf)
    active_schedule: Optional[Dict] = latest_last[0]

    # Gather all fire events within the window [start, end)
    events: List[Tuple[datetime, Dict]] = []
    for s in schedules:
        try:
            tz = ZoneInfo(canonicalize_timezone_name(s["timezone"]))
        except Exception:
            continue
        # Start cron iteration from the window start in the schedule's local time
        base_local = window_start_utc.astimezone(tz)
        itr = croniter(s["cron"], base_local)
        # Iterate forward until we pass the window end
        # Guard against excessive loops
        for _ in range(500):
            try:
                next_local = itr.get_next(datetime)
            except Exception:
                break
            next_utc = next_local.astimezone(timezone.utc)
            if next_utc >= window_end_utc:
                break
            if next_utc < window_start_utc:
                # Just in case of TZ/offset peculiarities
                continue
            events.append((next_utc, s))

    # Sort events by time
    events.sort(key=lambda e: e[0])

    segments: List[Dict] = []
    prev_time = window_start_utc
    for event_time, schedule in events:
        if prev_time < event_time:
            segments.append({
                "start_utc": prev_time,
                "end_utc": event_time,
                "schedule": active_schedule,
            })
        active_schedule = schedule
        prev_time = event_time

    # Tail segment to window end
    if prev_time < window_end_utc:
        segments.append({
            "start_utc": prev_time,
            "end_utc": window_end_utc,
            "schedule": active_schedule,
        })

    return segments


@app.route("/")
def index():
    state = load_state()
    members = state.get("members", [])
    schedules = state.get("schedules", [])

    current_schedule, current_started_utc, next_schedule, next_start_utc = compute_current_shift(state)
    member_map = get_member_map(state)

    current_member = member_map.get(current_schedule["member_id"]) if current_schedule else None
    next_member = member_map.get(next_schedule["member_id"]) if next_schedule else None

    return render_template(
        "index.html",
        members=members,
        schedules=schedules,
        current_member=current_member,
        current_started_utc=current_started_utc,
        next_member=next_member,
        next_start_utc=next_start_utc,
    )


@app.route("/api/current_shift", methods=["GET"])
def api_current_shift():
    state = load_state()
    current_schedule, current_started_utc, next_schedule, next_start_utc = compute_current_shift(state)
    member_map = get_member_map(state)
    current_member = member_map.get(current_schedule["member_id"]) if current_schedule else None
    next_member = member_map.get(next_schedule["member_id"]) if next_schedule else None

    return jsonify({
        "current": {
            "member": current_member,
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
    return redirect(url_for("index"))


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
    cron = request.form.get("cron", "").strip()
    timezone_name = request.form.get("timezone", "UTC").strip() or "UTC"
    member_id = request.form.get("member_id", "").strip()
    description = request.form.get("description", "").strip()

    if not cron or not member_id:
        return "cron and member_id required", 400

    try:
        canonical_tz = canonicalize_timezone_name(timezone_name)
        _ = next_fire_utc(cron, canonical_tz, get_now_utc())
    except Exception as e:
        return f"Invalid cron/timezone: {e}", 400

    new_schedule = {
        "id": str(uuid.uuid4()),
        "member_id": member_id,
        "cron": cron,
        "timezone": canonical_tz,
        "description": description,
    }
    state["schedules"].append(new_schedule)
    save_state(state)
    logging.info("Added schedule: %s (%s)", cron, canonical_tz)
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


@app.route("/api/schedule", methods=["GET"])
def list_schedule():
    state = load_state()
    return jsonify(state.get("schedules", []))


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
        s = seg["schedule"]
        sched_id = s.get("id") if s else None
        member = member_map.get(s.get("member_id")) if s else None
        return {
            "start_utc": seg["start_utc"].isoformat(),
            "end_utc": seg["end_utc"].isoformat(),
            "schedule": {
                "id": sched_id,
                "description": s.get("description") if s else None,
                "member": member,
            }
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
