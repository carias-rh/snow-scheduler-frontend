import json
import logging
import uuid
from datetime import datetime, timezone
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


@app.route("/api/schedule", methods=["GET"])
def list_schedule():
    state = load_state()
    return jsonify(state.get("schedules", []))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
