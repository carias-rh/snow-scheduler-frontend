import copy
import json
import logging
import os
import threading
import uuid
from datetime import date, datetime, timezone, timedelta, time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests as http_requests
from croniter import croniter
from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, render_template, request, url_for
from icalendar import Calendar as ICalCalendar
from zoneinfo import ZoneInfo

# Load .env from the app directory first, then walk up to find a project-root .env.
# In OpenShift the env vars are injected directly so load_dotenv is a no-op.
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent.parent / ".env")

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

_TEAM = os.environ.get("TEAM", "").strip()
SITE_TITLE = f"{_TEAM.upper()} - Shift Scheduler" if _TEAM else "Shift Scheduler"

BASE_DIR = Path(__file__).parent
DATA_FILE = BASE_DIR / "data" / "state.json"

# Common timezone abbreviation aliases to canonical IANA zones
TZ_ALIASES: Dict[str, str] = {
    "UTC": "UTC",
    "GMT": "Etc/GMT",
    # Europe
    "BST": "Europe/London",           # British Summer Time
    "CET": "Europe/Berlin",
    "CEST": "Europe/Berlin",
    "EET": "Europe/Bucharest",
    "EEST": "Europe/Bucharest",
    "WET": "Europe/Lisbon",
    "WEST": "Europe/Lisbon",
    "MSK": "Europe/Moscow",
    "TRT": "Europe/Istanbul",         # Turkey Time
    # Asia
    "IST": "Asia/Kolkata",            # India Standard Time
    "PKT": "Asia/Karachi",
    "CT": "Asia/Shanghai",            # China Time (common shorthand)
    "CCT": "Asia/Shanghai",           # China Coast Time
    "HKT": "Asia/Hong_Kong",
    "SGT": "Asia/Singapore",
    "MYT": "Asia/Kuala_Lumpur",       # Malaysia Time
    "PHT": "Asia/Manila",             # Philippine Time
    "WIB": "Asia/Jakarta",            # Western Indonesia
    "ICT": "Asia/Bangkok",            # Indochina Time (Thailand, Vietnam)
    "MMT": "Asia/Yangon",             # Myanmar Time
    "NPT": "Asia/Kathmandu",
    "BDT": "Asia/Dhaka",              # Bangladesh Time
    "JST": "Asia/Tokyo",
    "KST": "Asia/Seoul",
    "TWT": "Asia/Taipei",             # Taiwan Time
    # Oceania
    "AEST": "Australia/Sydney",
    "AEDT": "Australia/Sydney",
    "ACST": "Australia/Adelaide",
    "AWST": "Australia/Perth",
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
    "AKST": "America/Anchorage",
    "HST": "Pacific/Honolulu",
    # Latin America
    "BRT": "America/Sao_Paulo",       # Brasilia Time
    "ART": "America/Argentina/Buenos_Aires",
    "COT": "America/Bogota",          # Colombia Time
    "PET": "America/Lima",            # Peru Time
    "CLT": "America/Santiago",        # Chile Time
    "VET": "America/Caracas",         # Venezuela Time
    "ECT": "America/Guayaquil",       # Ecuador Time
    "MXT": "America/Mexico_City",
}


def ensure_data_file() -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not DATA_FILE.exists():
        initial_state = {
            "zones": [],
            "groups": [],
            "members": [
                {"id": str(uuid.uuid4()), "name": "Alice"},
                {"id": str(uuid.uuid4()), "name": "Bob"},
                {"id": str(uuid.uuid4()), "name": "Charlie"},
            ],
            "schedules": [],
            "rr": {},
        }
        DATA_FILE.write_text(json.dumps(initial_state, indent=2))


def load_state() -> Dict[str, List[Dict]]:
    ensure_data_file()
    state = json.loads(DATA_FILE.read_text())
    if _migrate_timezone_abbreviations(state):
        DATA_FILE.write_text(json.dumps(state, indent=2))
    return state


def save_state(state: Dict[str, List[Dict]]) -> None:
    DATA_FILE.write_text(json.dumps(state, indent=2))


def _migrate_timezone_abbreviations(state: Dict[str, List[Dict]]) -> bool:
    """Rewrite legacy abbreviations (EST, CST, …) to IANA names in-place.

    Returns True if any schedule was changed."""
    changed = False
    for s in state.get("schedules", []):
        tz = s.get("timezone", "")
        try:
            canonical = canonicalize_timezone_name(tz)
        except Exception:
            continue
        if canonical != tz:
            s["timezone"] = canonical
            changed = True
    return changed



def sync_groups_from_config() -> None:
    """Seed zones and groups into state.json from the SNOW_GROUPS_CONFIG env var.

    Called once at startup.  Preserves members, schedules, and round-robin state.
    """
    raw = os.environ.get("SNOW_GROUPS_CONFIG", "").strip()
    if not raw:
        return
    try:
        cfg = json.loads(raw)
    except json.JSONDecodeError:
        logging.error("SNOW_GROUPS_CONFIG is not valid JSON – skipping group sync")
        return

    state = load_state()
    state["zones"] = cfg.get("zones", [])
    state["groups"] = cfg.get("groups", [])
    save_state(state)
    logging.info(
        "Synced %d zone(s) and %d group(s) from SNOW_GROUPS_CONFIG",
        len(state["zones"]),
        len(state["groups"]),
    )


sync_groups_from_config()


def get_member_map(state: Dict[str, List[Dict]]) -> Dict[str, Dict]:
    return {m["id"]: m for m in state.get("members", [])}


def _redirect_back():
    """Redirect to the referrer or index, preserving the sidebar tab via the ``_tab`` form field."""
    tab = (request.form.get("_tab") or "").strip()
    base = request.referrer or url_for("index")
    # Strip any existing fragment from the base URL
    base = base.split("#")[0]
    if tab:
        return redirect(f"{base}#{tab}")
    return redirect(base)


def get_group_map(state: Dict[str, List[Dict]]) -> Dict[str, Dict]:
    return {g["id"]: g for g in state.get("groups", [])}


def filter_state(state: Dict, group: Optional[str] = None, zone: Optional[str] = None) -> Dict:
    """Return a shallow copy of state with schedules filtered by group and/or zone.

    Members, zones, groups, and rr are preserved unchanged so that member lookups
    and round-robin persistence work correctly on the original state.
    """
    if not group and not zone:
        return state

    if group:
        filtered = [s for s in state.get("schedules", []) if s.get("group") == group]
    else:
        zone_group_ids = {g["id"] for g in state.get("groups", []) if g.get("zone_id") == zone}
        filtered = [s for s in state.get("schedules", []) if s.get("group") in zone_group_ids]

    return {**state, "schedules": filtered}


def canonicalize_timezone_name(tz_name: str) -> str:
    name = (tz_name or "").strip()
    if not name:
        raise ValueError("Timezone required")

    # Check abbreviation aliases FIRST so that ambiguous names like "EST"
    # (a valid but fixed-offset IANA zone with no DST) get mapped to the
    # DST-aware IANA zone (e.g. "America/New_York").
    alias = name.upper()
    if alias in TZ_ALIASES:
        ZoneInfo(TZ_ALIASES[alias])
        return TZ_ALIASES[alias]

    # Direct IANA name
    try:
        ZoneInfo(name)
        return name
    except Exception:
        pass

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


def _is_bounded_range_schedule(s: Dict) -> bool:
    return _is_range_schedule(s) and bool(s.get("end_time"))


def _is_open_range_schedule(s: Dict) -> bool:
    return _is_range_schedule(s) and not bool(s.get("end_time"))


def _generate_events_for_schedule(s: Dict, window_start_utc: datetime, window_end_utc: datetime) -> List[Tuple[datetime, str, Dict]]:
    events: List[Tuple[datetime, str, Dict]] = []
    if not s.get("active", True):
        return events

    if "cron" in s and s.get("cron"):
        try:
            tz = ZoneInfo(canonicalize_timezone_name(s["timezone"]))
        except Exception:
            return events
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

    if _is_range_schedule(s):
        try:
            tz = ZoneInfo(canonicalize_timezone_name(s["timezone"]))
        except Exception:
            return events
        try:
            start_t = _parse_time_of_day(s["start_time"])
        except Exception:
            return events
        end_t: Optional[time] = None
        if s.get("end_time"):
            try:
                end_t = _parse_time_of_day(s["end_time"])
            except Exception:
                end_t = None
        try:
            days = [int(d) for d in (s.get("days") or [])]
        except Exception:
            days = []

        local_start = (window_start_utc - timedelta(days=2)).astimezone(tz)
        local_end = (window_end_utc + timedelta(days=1)).astimezone(tz)
        iteration_floor_utc = datetime(local_start.year, local_start.month, local_start.day, 0, 0, 0, tzinfo=tz).astimezone(timezone.utc)
        is_overnight = end_t is not None and (end_t.hour, end_t.minute) <= (start_t.hour, start_t.minute)
        cur_date = datetime(local_start.year, local_start.month, local_start.day, 0, 0, 0, tzinfo=tz)
        while cur_date < local_end:
            if cur_date.weekday() in days:
                start_local = datetime(cur_date.year, cur_date.month, cur_date.day, start_t.hour, start_t.minute, tzinfo=tz)
                start_utc = start_local.astimezone(timezone.utc)
                if start_utc < window_end_utc:
                    events.append((start_utc, "start", s))
                if end_t is not None:
                    end_day = cur_date + timedelta(days=1) if is_overnight else cur_date
                    end_local = datetime(end_day.year, end_day.month, end_day.day, end_t.hour, end_t.minute, tzinfo=tz)
                    end_utc = end_local.astimezone(timezone.utc)
                    if end_utc > iteration_floor_utc and end_utc < window_end_utc + timedelta(days=1):
                        events.append((end_utc, "end", s))

                # Overnight bleed-in: if previous day is NOT a schedule day,
                # generate a synthetic start+end so the overnight portion
                # (00:00 to end_time) still shows on this day's timeline.
                if is_overnight:
                    prev_day = cur_date - timedelta(days=1)
                    if prev_day.weekday() not in days:
                        syn_start_local = datetime(prev_day.year, prev_day.month, prev_day.day, start_t.hour, start_t.minute, tzinfo=tz)
                        syn_start_utc = syn_start_local.astimezone(timezone.utc)
                        syn_end_local = datetime(cur_date.year, cur_date.month, cur_date.day, end_t.hour, end_t.minute, tzinfo=tz)
                        syn_end_utc = syn_end_local.astimezone(timezone.utc)
                        if syn_start_utc < window_end_utc:
                            events.append((syn_start_utc, "start", s))
                        if syn_end_utc > iteration_floor_utc and syn_end_utc < window_end_utc + timedelta(days=1):
                            events.append((syn_end_utc, "end", s))

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
            if active and active.get("id") == sched.get("id"):
                active = None
                active_started = None
    return active, active_started


def _determine_all_active_at(state: Dict[str, List[Dict]], at_utc: datetime) -> Tuple[List[Dict], Optional[datetime]]:
    """Determine all schedules active at a specific UTC time, allowing overlaps."""
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
            if is_cron_schedule(sched) or _is_open_range_schedule(sched):
                if not (len(active_by_id) == 1 and sid in active_by_id):
                    active_by_id = {sid: sched}
                    last_change = ts
            elif _is_bounded_range_schedule(sched):
                removed_any = False
                to_remove = [aid for aid, a in active_by_id.items() if _is_open_range_schedule(a)]
                for rid in to_remove:
                    del active_by_id[rid]
                    removed_any = True
                if sid not in active_by_id:
                    active_by_id[sid] = sched
                    removed_any = True
                if removed_any:
                    last_change = ts
        elif kind == "end":
            if sid in active_by_id:
                del active_by_id[sid]
                last_change = ts

    active_list = list(active_by_id.values())
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
    active_schedules, _composition_changed = _determine_all_active_at(state, now_utc)

    # Compute when any of the currently active schedules started TODAY
    # (not when the composition historically changed, which can be days ago)
    today_start = None
    if active_schedules:
        day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        events = _generate_all_events(state, day_start, now_utc + timedelta(seconds=1))
        active_ids = {s.get("id") for s in active_schedules}
        for ts, kind, sched in events:
            if ts > now_utc:
                break
            if kind == "start" and sched.get("id") in active_ids:
                if today_start is None or ts < today_start:
                    today_start = ts

    return active_schedules, today_start


def compute_timeline_segments(state: Dict[str, List[Dict]], window_start_utc: datetime, window_end_utc: datetime) -> List[Dict]:
    """Compute continuous segments across the window with possibly multiple active schedules."""
    schedules: List[Dict] = [s for s in state.get("schedules", []) if s.get("active", True)]
    if not schedules:
        return []

    events = _generate_all_events(state, window_start_utc - timedelta(days=2), window_end_utc)

    active_by_id: Dict[str, Dict] = {}
    def is_cron_schedule(s: Dict) -> bool:
        return bool(s.get("cron")) and not _is_range_schedule(s)

    for ts, kind, sched in events:
        if ts >= window_start_utc:
            break
        sid = sched.get("id")
        if kind == "start":
            if is_cron_schedule(sched) or _is_open_range_schedule(sched):
                active_by_id = {sid: sched}
            elif _is_bounded_range_schedule(sched):
                to_remove = [aid for aid, a in active_by_id.items() if _is_open_range_schedule(a)]
                for rid in to_remove:
                    del active_by_id[rid]
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
            if is_cron_schedule(sched) or _is_open_range_schedule(sched):
                active_by_id = {sid: sched}
            elif _is_bounded_range_schedule(sched):
                to_remove = [aid for aid, a in active_by_id.items() if _is_open_range_schedule(a)]
                for rid in to_remove:
                    del active_by_id[rid]
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

    member_map = get_member_map(state)
    for seg in segments:
        seg["schedules"].sort(key=lambda s: (member_map.get(s.get("member_id"), {}).get("name", ""), s.get("id")))
    return segments


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    state = load_state()
    zones = state.get("zones", [])
    groups = state.get("groups", [])
    members = state.get("members", [])

    selected_zone = request.args.get("zone")
    selected_group = request.args.get("group")

    # When zones are configured, default to the first zone (no "Global" view)
    if zones and not selected_zone and not selected_group:
        selected_zone = zones[0]["id"]

    eval_state = filter_state(state, group=selected_group, zone=selected_zone)
    schedules = sorted(eval_state.get("schedules", []),
                       key=lambda s: (s.get("priority") or float("inf")))

    current_schedules, current_started_utc = compute_current_overlaps(eval_state)
    current_schedule, _single_started, next_schedule, next_start_utc = compute_current_shift(eval_state)
    member_map = get_member_map(state)

    current_members = [member_map.get(s.get("member_id")) for s in current_schedules]
    current_member = member_map.get(current_schedule["member_id"]) if current_schedule else None
    next_member = member_map.get(next_schedule["member_id"]) if next_schedule else None

    zone_groups = [g for g in groups if g.get("zone_id") == selected_zone] if selected_zone else groups

    members_on_pto = set(state.get("pto", []))
    pto_calendars = state.get("pto_calendars", [])
    pto_auto_enabled = state.get("pto_auto_enabled", False)
    pto_auto_members = set(state.get("pto_auto", []))

    return render_template(
        "index.html",
        site_title=SITE_TITLE,
        members=members,
        members_on_pto=members_on_pto,
        pto_calendars=pto_calendars,
        pto_auto_enabled=pto_auto_enabled,
        pto_auto_members=pto_auto_members,
        schedules=schedules,
        zones=zones,
        groups=groups,
        zone_groups=zone_groups,
        selected_zone=selected_zone,
        selected_group=selected_group,
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
    group_filter = request.args.get("group")
    zone_filter = request.args.get("zone")
    eval_state = filter_state(state, group=group_filter, zone=zone_filter)

    current_schedules, current_started_utc = compute_current_overlaps(eval_state)
    current_schedule, _single_started_utc, next_schedule, next_start_utc = compute_current_shift(eval_state)
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


# ---------------------------------------------------------------------------
# Member CRUD
# ---------------------------------------------------------------------------

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
    tab = (request.form.get("_tab") or "").strip()
    target = url_for("index", new_member_id=new_member["id"])
    if tab:
        target += f"#{tab}"
    return redirect(target)


@app.route("/members/edit/<member_id>", methods=["POST"])
def edit_member(member_id: str):
    """Rename an existing member."""
    state = load_state()
    name = request.form.get("name", "").strip()
    if not name:
        return "Name required", 400

    updated = False
    for m in state.get("members", []):
        if m.get("id") == member_id:
            m["name"] = name
            updated = True
            break
    if not updated:
        return "Member not found", 404

    state["members"] = sorted(state["members"], key=lambda m: m["name"].lower())
    save_state(state)
    logging.info("Renamed member %s to: %s", member_id, name)
    return _redirect_back()


@app.route("/members/delete/<member_id>", methods=["POST"])
def delete_member(member_id: str):
    state = load_state()
    state["members"] = [m for m in state["members"] if m["id"] != member_id]
    state["schedules"] = [s for s in state["schedules"] if s["member_id"] != member_id]
    save_state(state)
    logging.info("Deleted member: %s", member_id)
    return _redirect_back()


@app.route("/members/delete", methods=["POST"])
def delete_members_bulk():
    """Bulk delete members and their schedules."""
    state = load_state()
    ids = set(request.form.getlist("member_ids"))
    if not ids:
        return _redirect_back()
    before_m = len(state.get("members", []))
    state["members"] = [m for m in state.get("members", []) if m.get("id") not in ids]
    state["schedules"] = [s for s in state.get("schedules", []) if s.get("member_id") not in ids]
    save_state(state)
    after_m = len(state.get("members", []))
    logging.info("Bulk deleted %d members and their schedules", before_m - after_m)
    return _redirect_back()


def _parse_default_schedule_from_form() -> Tuple[Optional[Dict], Optional[str]]:
    """Parse range schedule fields from POST form. Returns (payload, error_message)."""
    start_time = (request.form.get("start_time") or "").strip()
    end_time = (request.form.get("end_time") or "").strip()
    days = request.form.getlist("days")
    timezone_name = (request.form.get("timezone") or "UTC").strip() or "UTC"

    if not start_time:
        return None, "Start time is required for a default schedule."

    try:
        _ = _parse_time_of_day(start_time)
        if end_time:
            _ = _parse_time_of_day(end_time)
    except Exception as e:
        return None, f"Invalid time: {e}"

    try:
        days_int = [int(d) for d in days]
        for d in days_int:
            if d < 0 or d > 6:
                raise ValueError("day out of range")
    except Exception:
        return None, "Select at least one weekday; days must be 0=Mon .. 6=Sun."

    if not days_int:
        return None, "Select at least one day of the week."

    try:
        canonical_tz = canonicalize_timezone_name(timezone_name)
    except Exception as e:
        return None, f"Invalid timezone: {e}"

    return {
        "start_time": start_time,
        "end_time": end_time or None,
        "days": days_int,
        "timezone": canonical_tz,
    }, None


def _apply_default_schedule_to_member_schedules(state: Dict, member_id: str, ds: Dict) -> int:
    """Copy default range fields onto every range-based schedule for the member. Returns count updated."""
    n = 0
    for s in state.get("schedules", []):
        if s.get("member_id") != member_id:
            continue
        if not _is_range_schedule(s):
            continue
        s["start_time"] = ds["start_time"]
        s["end_time"] = ds.get("end_time")
        s["days"] = list(ds["days"])
        s["timezone"] = ds["timezone"]
        s.pop("cron", None)
        n += 1
    return n


@app.route("/members/default_schedule/<member_id>", methods=["POST"])
def set_member_default_schedule(member_id: str):
    """Optional per-member default shift template; can be applied to all range schedules at once."""
    state = load_state()
    member_map = get_member_map(state)
    if member_id not in member_map:
        return "Member not found", 404

    action = (request.form.get("action") or "save").strip().lower()

    if action == "clear":
        for m in state.get("members", []):
            if m.get("id") == member_id:
                m.pop("default_schedule", None)
                break
        save_state(state)
        logging.info("Cleared default schedule template for member %s", member_id)
        return _redirect_back()

    ds, err = _parse_default_schedule_from_form()
    if err or not ds:
        return err or "Invalid default schedule", 400

    for m in state.get("members", []):
        if m.get("id") == member_id:
            m["default_schedule"] = ds
            break

    if action == "save_and_apply":
        n = _apply_default_schedule_to_member_schedules(state, member_id, ds)
        save_state(state)
        logging.info(
            "Updated default schedule for member %s and applied to %d range schedule(s)",
            member_id,
            n,
        )
        return _redirect_back()

    if action == "save":
        save_state(state)
        logging.info("Saved default schedule template for member %s", member_id)
        return _redirect_back()

    return "Unknown action", 400


def _toggle_pto(state: Dict, member_id: str, going_on_pto: bool,
                group_filter: Optional[str] = None) -> Dict:
    """Core PTO toggle logic — modifies *state* in-place, returns stats.

    Does NOT call ``save_state()``; the caller is responsible for persisting.

    Priority cascade logic:
      - **PTO On**: deactivates all of the member's schedules, then for each
        affected group promotes the next-priority member who is NOT on PTO.
      - **PTO Off**: reactivates schedules only where the member is the
        highest-priority non-PTO person, then demotes lower-priority members
        that were covering.
    """
    all_schedules = state.get("schedules", [])
    pto_set = set(state.get("pto", []))
    member_map = get_member_map(state)
    active_value = not going_on_pto

    if going_on_pto:
        pto_set.add(member_id)
    else:
        pto_set.discard(member_id)
    state["pto"] = list(pto_set)

    toggled = 0
    skipped = 0
    promoted = 0
    demoted = 0
    affected_groups: set = set()

    for s in all_schedules:
        if s.get("member_id") != member_id:
            continue
        if group_filter and s.get("group") != group_filter:
            continue

        if not active_value:
            s["active"] = False
            toggled += 1
            if s.get("group"):
                affected_groups.add(s["group"])
        else:
            my_priority = s.get("priority")
            if my_priority is None:
                s["active"] = True
                toggled += 1
                continue

            grp = s.get("group")
            higher_active = any(
                other.get("active")
                and other.get("group") == grp
                and other.get("member_id") != member_id
                and other.get("member_id") not in pto_set
                and (other.get("priority") or 1) < my_priority
                for other in all_schedules
            )
            if higher_active:
                skipped += 1
            else:
                s["active"] = True
                toggled += 1
                if grp:
                    affected_groups.add(grp)

    for grp in affected_groups:
        grp_schedules = [s for s in all_schedules
                         if s.get("group") == grp and s.get("priority") is not None]
        if not grp_schedules:
            continue

        if not active_value:
            non_pto_candidates = [
                s for s in grp_schedules
                if s.get("member_id") != member_id
                and s.get("member_id") not in pto_set
            ]
            non_pto_candidates.sort(key=lambda s: s.get("priority") or 1)
            already_active = any(s.get("active") for s in non_pto_candidates)
            if not already_active and non_pto_candidates:
                next_priority = non_pto_candidates[0].get("priority") or 1
                for s in non_pto_candidates:
                    if (s.get("priority") or 1) == next_priority:
                        s["active"] = True
                        promoted += 1
                        logging.info(
                            "Cascade: promoted %s (p%s) in group %s",
                            member_map.get(s["member_id"], {}).get("name", s["member_id"]),
                            s.get("priority"), grp,
                        )
        else:
            my_schedules_in_grp = [
                s for s in grp_schedules
                if s.get("member_id") == member_id and s.get("active")
            ]
            if not my_schedules_in_grp:
                continue
            my_best = min((s.get("priority") or 1) for s in my_schedules_in_grp)
            for s in grp_schedules:
                if s.get("member_id") == member_id:
                    continue
                if s.get("member_id") in pto_set:
                    continue
                if s.get("active") and (s.get("priority") or 1) > my_best:
                    s["active"] = False
                    demoted += 1
                    logging.info(
                        "Cascade: demoted %s (p%s) in group %s",
                        member_map.get(s["member_id"], {}).get("name", s["member_id"]),
                        s.get("priority"), grp,
                    )

    return {"toggled": toggled, "skipped": skipped, "promoted": promoted, "demoted": demoted}


@app.route("/members/toggle_schedules/<member_id>", methods=["POST"])
def toggle_member_schedules(member_id: str):
    """Activate or deactivate all schedules for a member (PTO management)."""
    state = load_state()
    active_param = (request.form.get("active") or "").strip().lower()
    active_value = active_param in ("1", "true", "on", "yes")
    group_filter = request.form.get("group", "").strip() or None
    going_on_pto = not active_value

    member_map = get_member_map(state)
    member = member_map.get(member_id, {})
    member_name = member.get("name", member_id)

    stats = _toggle_pto(state, member_id, going_on_pto, group_filter)

    # Manual PTO-off also clears the auto-managed flag
    if not going_on_pto:
        pto_auto = set(state.get("pto_auto", []))
        pto_auto.discard(member_id)
        state["pto_auto"] = list(pto_auto)

    save_state(state)

    action = "deactivated" if going_on_pto else "activated"
    logging.info("PTO %s for %s: %d %s, %d skipped, %d promoted, %d demoted (group=%s)",
                 "On" if going_on_pto else "Off", member_name,
                 stats["toggled"], action, stats["skipped"],
                 stats["promoted"], stats["demoted"], group_filter)

    if request.accept_mimetypes.best == "application/json" or request.headers.get("X-Requested-With") == "fetch":
        return jsonify({
            "ok": True,
            "member_id": member_id,
            "member_name": member_name,
            "active": active_value,
            "count": stats["toggled"],
            "skipped": stats["skipped"],
            "promoted": stats["promoted"],
            "demoted": stats["demoted"],
        })
    return _redirect_back()


# ---------------------------------------------------------------------------
# Schedule CRUD
# ---------------------------------------------------------------------------

def _should_new_schedule_be_active(state: Dict, member_id: str, group: Optional[str],
                                   priority: Optional[int]) -> bool:
    """Decide whether a newly added schedule should start active.

    If the schedule has a priority and there is already an active schedule in the
    same group held by a different (non-PTO) member at a higher (lower number)
    priority, the new schedule starts inactive so it doesn't compete.
    """
    if priority is None or group is None:
        return True
    pto_set = set(state.get("pto", []))
    if member_id in pto_set:
        return False
    for s in state.get("schedules", []):
        if not s.get("active"):
            continue
        if s.get("group") != group:
            continue
        if s.get("member_id") == member_id:
            continue
        if s.get("member_id") in pto_set:
            continue
        other_p = s.get("priority")
        if other_p is not None and other_p < priority:
            return False
    return True


@app.route("/schedule/add", methods=["POST"])
def add_schedule():
    state = load_state()
    timezone_name = request.form.get("timezone", "UTC").strip() or "UTC"
    member_id = request.form.get("member_id", "").strip()
    priority_raw = request.form.get("priority", "").strip()
    priority = int(priority_raw) if priority_raw else None

    groups_raw = request.form.getlist("group")
    groups_list = list(dict.fromkeys(g.strip() for g in groups_raw if g.strip()))
    if not groups_list:
        groups_list = [None]

    if not member_id:
        return "member_id required", 400

    try:
        canonical_tz = canonicalize_timezone_name(timezone_name)
    except Exception as e:
        return f"Invalid timezone: {e}", 400

    start_time = (request.form.get("start_time") or "").strip()
    end_time = (request.form.get("end_time") or "").strip()
    days = request.form.getlist("days")

    if start_time:
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

        for group in groups_list:
            active = _should_new_schedule_be_active(state, member_id, group, priority)
            new_schedule = {
                "id": str(uuid.uuid4()),
                "member_id": member_id,
                "start_time": start_time,
                "end_time": end_time or None,
                "days": days_int,
                "timezone": canonical_tz,
                "active": active,
                "group": group,
                "priority": priority,
            }
            state["schedules"].append(new_schedule)
        save_state(state)
        logging.info("Added %d range schedule(s): %s %s-%s (%s) days=%s groups=%s p=%s active=%s", len(groups_list), member_id, start_time, end_time or "", canonical_tz, days_int, groups_list, priority, active)
        return _redirect_back()

    cron = request.form.get("cron", "").strip()
    if not cron:
        return "start_time or cron required", 400
    try:
        _ = next_fire_utc(cron, canonical_tz, get_now_utc())
    except Exception as e:
        return f"Invalid cron: {e}", 400

    for group in groups_list:
        active = _should_new_schedule_be_active(state, member_id, group, priority)
        new_schedule = {
            "id": str(uuid.uuid4()),
            "member_id": member_id,
            "cron": cron,
            "timezone": canonical_tz,
            "active": active,
            "group": group,
            "priority": priority,
        }
        state["schedules"].append(new_schedule)
    save_state(state)
    logging.info("Added %d cron schedule(s): %s (%s) groups=%s p=%s active=%s", len(groups_list), cron, canonical_tz, groups_list, priority, active)
    return _redirect_back()


@app.route("/schedule/edit/<schedule_id>", methods=["POST"])
def edit_schedule(schedule_id: str):
    """Update an existing schedule in-place, preserving its id and active state."""
    state = load_state()

    target = None
    for s in state.get("schedules", []):
        if s.get("id") == schedule_id:
            target = s
            break
    if target is None:
        return "Schedule not found", 404

    timezone_name = request.form.get("timezone", "UTC").strip() or "UTC"
    member_id = request.form.get("member_id", "").strip()
    group = request.form.get("group", "").strip() or None
    priority_raw = request.form.get("priority", "").strip()
    priority = int(priority_raw) if priority_raw else None

    if not member_id:
        return "member_id required", 400

    try:
        canonical_tz = canonicalize_timezone_name(timezone_name)
    except Exception as e:
        return f"Invalid timezone: {e}", 400

    start_time = (request.form.get("start_time") or "").strip()
    end_time = (request.form.get("end_time") or "").strip()
    days = request.form.getlist("days")

    if start_time:
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

        target["member_id"] = member_id
        target["start_time"] = start_time
        target["end_time"] = end_time or None
        target["days"] = days_int
        target["timezone"] = canonical_tz
        target["group"] = group
        target["priority"] = priority
        target.pop("cron", None)
        save_state(state)
        logging.info("Edited schedule %s: range %s-%s (%s) days=%s group=%s p=%s",
                     schedule_id, start_time, end_time or "", canonical_tz, days_int, group, priority)
        return _redirect_back()

    cron = request.form.get("cron", "").strip()
    if not cron:
        return "start_time or cron required", 400
    try:
        _ = next_fire_utc(cron, canonical_tz, get_now_utc())
    except Exception as e:
        return f"Invalid cron: {e}", 400

    target["member_id"] = member_id
    target["cron"] = cron
    target["timezone"] = canonical_tz
    target["group"] = group
    target["priority"] = priority
    target.pop("start_time", None)
    target.pop("end_time", None)
    target.pop("days", None)
    save_state(state)
    logging.info("Edited schedule %s: cron %s (%s) group=%s p=%s",
                 schedule_id, cron, canonical_tz, group, priority)
    return _redirect_back()


@app.route("/schedule/duplicate/<schedule_id>", methods=["POST"])
def duplicate_schedule(schedule_id: str):
    """Create a copy of an existing schedule with a new id."""
    state = load_state()
    source = None
    for s in state.get("schedules", []):
        if s.get("id") == schedule_id:
            source = s
            break
    if source is None:
        return "Schedule not found", 404

    new_schedule = copy.deepcopy(source)
    new_schedule["id"] = str(uuid.uuid4())
    state["schedules"].append(new_schedule)
    save_state(state)
    logging.info("Duplicated schedule %s → %s", schedule_id, new_schedule["id"])
    return _redirect_back()


@app.route("/schedule/delete/<schedule_id>", methods=["POST"])
def delete_schedule(schedule_id: str):
    state = load_state()
    state["schedules"] = [s for s in state["schedules"] if s["id"] != schedule_id]
    save_state(state)
    logging.info("Deleted schedule: %s", schedule_id)
    return _redirect_back()


@app.route("/schedule/delete", methods=["POST"])
def delete_schedules_bulk():
    """Bulk delete schedules from a list of ids in form field 'schedule_ids'."""
    state = load_state()
    ids = request.form.getlist("schedule_ids")
    if not ids:
        return _redirect_back()
    before = len(state.get("schedules", []))
    state["schedules"] = [s for s in state.get("schedules", []) if s.get("id") not in ids]
    save_state(state)
    after = len(state.get("schedules", []))
    logging.info("Bulk deleted %d schedules", before - after)
    return _redirect_back()


@app.route("/schedule/set_active/<schedule_id>", methods=["POST"])
def set_schedule_active(schedule_id: str):
    """Set a schedule's active flag."""
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

    if request.accept_mimetypes.best == "application/json" or request.headers.get("X-Requested-With") == "fetch":
        return jsonify({"ok": updated, "schedule_id": schedule_id, "active": active_value})
    return _redirect_back()


# ---------------------------------------------------------------------------
# Shift API (consumed by servicenow_autoassign)
# ---------------------------------------------------------------------------

@app.route("/api/shift", methods=["GET"])
def api_shift():
    state = load_state()
    group_filter = request.args.get("group")
    zone_filter = request.args.get("zone")
    eval_state = filter_state(state, group=group_filter, zone=zone_filter)

    now_utc = get_now_utc()
    active_schedules, active_set_started = _determine_all_active_at(eval_state, now_utc)
    member_map = get_member_map(state)

    filter_label = f"group={group_filter}" if group_filter else (f"zone={zone_filter}" if zone_filter else "all")

    if not active_schedules:
        logging.info(
            "/api/shift [%s] No active schedules at %s — nobody on shift",
            filter_label, now_utc.strftime("%Y-%m-%d %H:%M UTC"),
        )
        return jsonify({
            "id": None,
            "name": None,
            "on_shift": False,
            "round_robin": False,
        })

    active_names = [
        member_map.get(s.get("member_id"), {}).get("name", "?") for s in active_schedules
    ]

    if len(active_schedules) == 1:
        only = active_schedules[0]
        member = member_map.get(only.get("member_id"))
        name = member.get("name") if member else None
        logging.info(
            "/api/shift [%s] Single active schedule → %s (no round-robin)",
            filter_label, name,
        )
        return jsonify({
            "id": member.get("id") if member else None,
            "name": name,
            "on_shift": True,
            "round_robin": False,
        })

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
    name = member.get("name") if member else None
    logging.info(
        "/api/shift [%s] Round-robin active: pool=%s | prev_idx=%d → next_idx=%d → selected: %s",
        filter_label, active_names, prev_index, next_index, name,
    )
    return jsonify({
        "id": member.get("id") if member else None,
        "name": name,
        "on_shift": True,
        "round_robin": True,
    })


# ---------------------------------------------------------------------------
# Timeline API
# ---------------------------------------------------------------------------

@app.route("/api/timeline", methods=["GET"])
def api_timeline():
    """Return 24h timeline segments, optionally filtered by group or zone."""
    state = load_state()
    group_filter = request.args.get("group")
    zone_filter = request.args.get("zone")
    eval_state = filter_state(state, group=group_filter, zone=zone_filter)

    tz_param = request.args.get("tz", "UTC").strip() or "UTC"
    tz_name = canonicalize_timezone_name(tz_param)
    tz = ZoneInfo(tz_name)

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

    segments = compute_timeline_segments(eval_state, window_start_utc, window_end_utc)
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


# ---------------------------------------------------------------------------
# Data API (for external integrations)
# ---------------------------------------------------------------------------

@app.route("/api/zones", methods=["GET"])
def api_zones():
    state = load_state()
    return jsonify(state.get("zones", []))


@app.route("/api/groups", methods=["GET"])
def api_groups():
    state = load_state()
    zone = request.args.get("zone")
    groups = state.get("groups", [])
    if zone:
        groups = [g for g in groups if g.get("zone_id") == zone]
    return jsonify(groups)


# ---------------------------------------------------------------------------
# Leave Calendar CRUD (built-in leave events stored in state.json)
# ---------------------------------------------------------------------------

@app.route("/api/leave_events", methods=["GET"])
def api_leave_events():
    """Return leave events for FullCalendar, optionally filtered by date range."""
    state = load_state()
    events = state.get("leave_events", [])
    member_map = get_member_map(state)

    start_param = request.args.get("start", "")
    end_param = request.args.get("end", "")

    fc_events = []
    for ev in events:
        if start_param and ev.get("end", "") < start_param:
            continue
        if end_param and ev.get("start", "") >= end_param:
            continue
        member = member_map.get(ev.get("member_id"), {})
        fc_events.append({
            "id": ev["id"],
            "title": member.get("name", "Unknown"),
            "start": ev["start"],
            "end": ev["end"],
            "allDay": True,
            "extendedProps": {
                "member_id": ev.get("member_id"),
                "leave_type": ev.get("leave_type", "Leave"),
            },
        })
    return jsonify(fc_events)


@app.route("/api/leave_events", methods=["POST"])
def add_leave_event():
    """Create a leave event."""
    data = request.get_json(force=True)
    member_id = data.get("member_id", "").strip()
    start = data.get("start", "").strip()
    end = data.get("end", "").strip()
    leave_type = data.get("leave_type", "Leave").strip() or "Leave"

    if not member_id or not start:
        return jsonify({"ok": False, "error": "member_id and start required"}), 400
    if not end:
        end_date = datetime.strptime(start, "%Y-%m-%d").date() + timedelta(days=1)
        end = end_date.isoformat()

    state = load_state()
    event = {
        "id": str(uuid.uuid4()),
        "member_id": member_id,
        "start": start,
        "end": end,
        "leave_type": leave_type,
    }
    state.setdefault("leave_events", []).append(event)
    save_state(state)

    member_map = get_member_map(state)
    logging.info("Added leave event: %s %s–%s (%s)",
                 member_map.get(member_id, {}).get("name", member_id), start, end, leave_type)

    sync_result = None
    try:
        sync_result = sync_pto_calendars()
    except Exception as exc:
        logging.warning("PTO sync after leave add failed: %s", exc)

    payload: Dict = {"ok": True, "event": event}
    if sync_result is not None:
        payload["sync"] = sync_result
    return jsonify(payload)


@app.route("/api/leave_events/<event_id>", methods=["DELETE"])
def delete_leave_event(event_id: str):
    """Delete a leave event."""
    state = load_state()
    before = len(state.get("leave_events", []))
    state["leave_events"] = [e for e in state.get("leave_events", []) if e["id"] != event_id]
    if len(state.get("leave_events", [])) == before:
        return jsonify({"ok": False, "error": "Not found"}), 404
    save_state(state)
    logging.info("Deleted leave event: %s", event_id)

    sync_result = None
    try:
        sync_result = sync_pto_calendars()
    except Exception as exc:
        logging.warning("PTO sync after leave delete failed: %s", exc)

    payload: Dict = {"ok": True}
    if sync_result is not None:
        payload["sync"] = sync_result
    return jsonify(payload)


# ---------------------------------------------------------------------------
# PTO Calendar (ICS Feed) Sync
# ---------------------------------------------------------------------------

def _read_ics_source(ics_url: str) -> bytes:
    """Read ICS data from a URL (http/https) or a local file path."""
    stripped = ics_url.strip()
    if stripped.startswith("file://"):
        return Path(stripped[7:]).read_bytes()
    if stripped.startswith("/") or (len(stripped) > 1 and stripped[1] == ":"):
        return Path(stripped).read_bytes()
    resp = http_requests.get(stripped, timeout=30)
    resp.raise_for_status()
    content_type = resp.headers.get("Content-Type", "")
    if "text/html" in content_type:
        raise ValueError(
            "URL returned HTML instead of ICS data. "
            "If using Google Calendar, ensure the calendar is set to public "
            "(Settings → Access permissions → Make available to public) "
            "and use the ICS link from 'Integrate calendar'."
        )
    return resp.content


def _fetch_active_pto_events(ics_url: str, now_utc: datetime) -> List[Dict]:
    """Fetch an ICS feed (URL or local path) and return VEVENTs active at *now_utc*.

    For all-day events the UTC check window is widened to cover all timezone
    offsets (UTC-12 … UTC+14) so that events are returned whenever the
    calendar date could be "today" in *any* member timezone.  The original
    dates are stored in ``all_day_start`` / ``all_day_end`` so the caller can
    do a precise per-member-timezone check.
    """
    cal = ICalCalendar.from_ical(_read_ics_source(ics_url))

    active: List[Dict] = []
    for comp in cal.walk():
        if comp.name != "VEVENT":
            continue

        dtstart = comp.get("DTSTART")
        if not dtstart:
            continue
        start_val = dtstart.dt
        dtend = comp.get("DTEND")
        all_day_start: Optional[date] = None
        all_day_end: Optional[date] = None

        if isinstance(start_val, date) and not isinstance(start_val, datetime):
            all_day_start = start_val
            if dtend:
                end_val = dtend.dt
                if isinstance(end_val, date) and not isinstance(end_val, datetime):
                    all_day_end = end_val
                else:
                    all_day_end = start_val + timedelta(days=1)
            else:
                all_day_end = start_val + timedelta(days=1)

            # Widen UTC window so the event is fetched for members whose
            # local clock is already inside the PTO date.
            # UTC+14 → date starts 14 h before midnight UTC
            # UTC-12 → date ends  12 h after  midnight UTC
            ev_start = datetime(all_day_start.year, all_day_start.month,
                                all_day_start.day, tzinfo=timezone.utc) - timedelta(hours=14)
            ev_end = datetime(all_day_end.year, all_day_end.month,
                              all_day_end.day, tzinfo=timezone.utc) + timedelta(hours=12)
        else:
            if start_val.tzinfo is None:
                start_val = start_val.replace(tzinfo=timezone.utc)
            ev_start = start_val.astimezone(timezone.utc)
            if dtend:
                end_val = dtend.dt
                if isinstance(end_val, date) and not isinstance(end_val, datetime):
                    ev_end = datetime(end_val.year, end_val.month, end_val.day,
                                      tzinfo=timezone.utc)
                elif end_val.tzinfo is None:
                    ev_end = end_val.replace(tzinfo=timezone.utc)
                else:
                    ev_end = end_val.astimezone(timezone.utc)
            else:
                dur = comp.get("DURATION")
                ev_end = (ev_start + dur.dt) if dur else (ev_start + timedelta(hours=8))

        if ev_start <= now_utc < ev_end:
            summary = str(comp.get("SUMMARY", ""))
            attendees = comp.get("ATTENDEE")
            if attendees is None:
                attendees = []
            elif not isinstance(attendees, list):
                attendees = [attendees]
            event: Dict = {
                "summary": summary,
                "start": ev_start.isoformat(),
                "end": ev_end.isoformat(),
                "attendees": [
                    str(a).replace("mailto:", "").strip().lower()
                    for a in attendees if a
                ],
            }
            if all_day_start is not None:
                event["all_day_start"] = all_day_start.isoformat()
                event["all_day_end"] = all_day_end.isoformat()
            active.append(event)

    return active


def _match_event_to_members(event: Dict, members: List[Dict],
                            match_by: str) -> List[Dict]:
    """Match an ICS event to members by summary text or attendee e-mail."""
    matched: List[Dict] = []
    if match_by == "summary":
        summary_lower = event["summary"].lower()
        for m in members:
            if m["name"].lower() in summary_lower:
                matched.append(m)
    elif match_by == "email":
        event_emails = set(event.get("attendees", []))
        for m in members:
            member_email = m.get("email", "").strip().lower()
            if member_email and member_email in event_emails:
                matched.append(m)
    return matched


def _get_member_timezones(state: Dict, member_id: str) -> List[str]:
    """Return the unique canonical timezones associated with a member.

    Checks the member's ``default_schedule`` first, then collects timezones
    from all of their schedules.  Falls back to ``["UTC"]`` if nothing is
    configured.
    """
    tzs: set = set()
    for m in state.get("members", []):
        if m["id"] == member_id:
            ds_tz = m.get("default_schedule", {}).get("timezone")
            if ds_tz:
                try:
                    tzs.add(canonicalize_timezone_name(ds_tz))
                except Exception:
                    pass
            break

    for s in state.get("schedules", []):
        if s.get("member_id") == member_id and s.get("timezone"):
            try:
                tzs.add(canonicalize_timezone_name(s["timezone"]))
            except Exception:
                pass

    return list(tzs) if tzs else ["UTC"]


def _is_pto_active_for_member(event: Dict, now_utc: datetime,
                               member_timezones: List[str]) -> bool:
    """Check whether *event* is currently active for a member.

    For timed events the caller already filtered by UTC window, so this
    always returns ``True``.

    For all-day events the check is timezone-aware: the event is active if
    the member's local date (in **any** of their schedule timezones) falls
    within ``[all_day_start, all_day_end)``.
    """
    if "all_day_start" not in event:
        return True

    ev_start_date = date.fromisoformat(event["all_day_start"])
    ev_end_date = date.fromisoformat(event["all_day_end"])

    for tz_name in member_timezones:
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            continue
        local_date = now_utc.astimezone(tz).date()
        if ev_start_date <= local_date < ev_end_date:
            return True

    return False


def sync_pto_calendars() -> Dict:
    """Sync all enabled PTO calendars and auto-toggle PTO.

    Returns a summary dict with counts of what changed.
    """
    state = load_state()
    calendars = state.get("pto_calendars", [])
    enabled_cals = [c for c in calendars if c.get("enabled", True)]

    # Always run the full sync through PTO reconciliation. Do not return early when
    # there are no ICS feeds and no leave events: deleting the last leave entry must
    # still clear pto_auto members who were only on PTO from the Leave Calendar.

    now_utc = get_now_utc()
    members = state.get("members", [])
    pto_auto = set(state.get("pto_auto", []))
    should_be_on_pto: set = set()
    errors: List[Dict] = []
    events_found = 0

    # Pre-compute each member's timezones once for all-day-event checks.
    member_tz_cache: Dict[str, List[str]] = {}

    for cal_cfg in enabled_cals:
        cal_id = cal_cfg["id"]
        ics_url = cal_cfg.get("ics_url", "")
        match_by = cal_cfg.get("match_by", "summary")
        try:
            active_events = _fetch_active_pto_events(ics_url, now_utc)
            events_found += len(active_events)
            for ev in active_events:
                for m in _match_event_to_members(ev, members, match_by):
                    mid = m["id"]
                    if mid not in member_tz_cache:
                        member_tz_cache[mid] = _get_member_timezones(state, mid)
                    if _is_pto_active_for_member(ev, now_utc, member_tz_cache[mid]):
                        should_be_on_pto.add(mid)
            cal_cfg["last_sync"] = now_utc.isoformat()
            cal_cfg["last_sync_status"] = "ok"
            cal_cfg["last_sync_events"] = len(active_events)
        except Exception as exc:
            logging.warning("Failed to sync PTO calendar %s: %s",
                            cal_cfg.get("name", cal_id), exc)
            cal_cfg["last_sync"] = now_utc.isoformat()
            cal_cfg["last_sync_status"] = f"error: {exc}"
            cal_cfg["last_sync_events"] = 0
            errors.append({"calendar": cal_cfg.get("name", cal_id), "error": str(exc)})

    # Also check built-in leave events from the Leave Calendar.
    # Use each member's local date (from their schedule timezones) so that
    # PTO triggers at the right time regardless of UTC offset.
    for ev in state.get("leave_events", []):
        mid = ev.get("member_id")
        if not mid:
            continue
        if mid not in member_tz_cache:
            member_tz_cache[mid] = _get_member_timezones(state, mid)
        tzs = member_tz_cache[mid] or ["UTC"]
        is_active = False
        for tz_name in tzs:
            try:
                local_date = now_utc.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d")
            except Exception:
                local_date = now_utc.strftime("%Y-%m-%d")
            if ev.get("start", "") <= local_date < ev.get("end", ""):
                is_active = True
                break
        if is_active:
            should_be_on_pto.add(mid)
            events_found += 1

    pto_set = set(state.get("pto", []))
    toggled_on: List[str] = []
    toggled_off: List[str] = []

    for mid in should_be_on_pto:
        if mid not in pto_set:
            _toggle_pto(state, mid, going_on_pto=True)
            pto_auto.add(mid)
            toggled_on.append(mid)

    for mid in list(pto_auto):
        if mid not in should_be_on_pto:
            _toggle_pto(state, mid, going_on_pto=False)
            pto_auto.discard(mid)
            toggled_off.append(mid)

    state["pto_auto"] = list(pto_auto)
    save_state(state)

    if toggled_on or toggled_off:
        member_map = get_member_map(state)
        on_names = [member_map.get(mid, {}).get("name", mid) for mid in toggled_on]
        off_names = [member_map.get(mid, {}).get("name", mid) for mid in toggled_off]
        if on_names:
            logging.info("PTO Auto-Sync: set PTO ON for: %s", ", ".join(on_names))
        if off_names:
            logging.info("PTO Auto-Sync: set PTO OFF for: %s", ", ".join(off_names))

    return {
        "synced": len(enabled_cals),
        "events_found": events_found,
        "toggled_on": toggled_on,
        "toggled_off": toggled_off,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# PTO Calendar CRUD routes
# ---------------------------------------------------------------------------

@app.route("/pto_calendars/add", methods=["POST"])
def add_pto_calendar():
    state = load_state()
    name = request.form.get("name", "").strip()
    ics_url = request.form.get("ics_url", "").strip()
    match_by = request.form.get("match_by", "summary").strip()
    poll_raw = request.form.get("poll_interval_minutes", "15") or "15"
    poll_interval = max(1, int(poll_raw))

    if not name or not ics_url:
        return "Name and ICS URL required", 400

    cal = {
        "id": str(uuid.uuid4()),
        "name": name,
        "ics_url": ics_url,
        "match_by": match_by,
        "poll_interval_minutes": poll_interval,
        "enabled": True,
        "last_sync": None,
        "last_sync_status": None,
        "last_sync_events": 0,
    }
    state.setdefault("pto_calendars", []).append(cal)
    save_state(state)
    logging.info("Added PTO calendar: %s (%s)", name, ics_url)
    return _redirect_back()


@app.route("/pto_calendars/edit/<cal_id>", methods=["POST"])
def edit_pto_calendar(cal_id: str):
    state = load_state()
    for cal in state.get("pto_calendars", []):
        if cal["id"] == cal_id:
            cal["name"] = request.form.get("name", cal["name"]).strip()
            cal["ics_url"] = request.form.get("ics_url", cal["ics_url"]).strip()
            cal["match_by"] = request.form.get("match_by", cal.get("match_by", "summary")).strip()
            poll_raw = request.form.get("poll_interval_minutes", "")
            if poll_raw:
                cal["poll_interval_minutes"] = max(1, int(poll_raw))
            save_state(state)
            logging.info("Updated PTO calendar: %s", cal["name"])
            return _redirect_back()
    return "Calendar not found", 404


@app.route("/pto_calendars/delete/<cal_id>", methods=["POST"])
def delete_pto_calendar(cal_id: str):
    state = load_state()
    state["pto_calendars"] = [c for c in state.get("pto_calendars", []) if c["id"] != cal_id]
    save_state(state)
    logging.info("Deleted PTO calendar: %s", cal_id)
    return _redirect_back()


@app.route("/pto_calendars/toggle_enabled/<cal_id>", methods=["POST"])
def toggle_pto_calendar_enabled(cal_id: str):
    state = load_state()
    for cal in state.get("pto_calendars", []):
        if cal["id"] == cal_id:
            cal["enabled"] = not cal.get("enabled", True)
            save_state(state)
            if (request.accept_mimetypes.best == "application/json"
                    or request.headers.get("X-Requested-With") == "fetch"):
                return jsonify({"ok": True, "enabled": cal["enabled"]})
            return _redirect_back()
    return "Calendar not found", 404


@app.route("/pto_calendars/sync", methods=["POST"])
def trigger_pto_sync():
    """Manually trigger a PTO calendar sync."""
    try:
        result = sync_pto_calendars()
        if (request.accept_mimetypes.best == "application/json"
                or request.headers.get("X-Requested-With") == "fetch"):
            return jsonify({"ok": True, **result})
        return _redirect_back()
    except Exception as exc:
        if (request.accept_mimetypes.best == "application/json"
                or request.headers.get("X-Requested-With") == "fetch"):
            return jsonify({"ok": False, "error": str(exc)}), 500
        return f"Sync failed: {exc}", 500


@app.route("/api/pto/upload_ics", methods=["POST"])
def upload_pto_ics():
    """Receive an ICS file (e.g. from a Google Apps Script), save it locally,
    upsert a PTO calendar entry pointing to the file, and trigger a sync."""
    api_key = os.environ.get("PTO_UPLOAD_API_KEY", "")
    if api_key and request.headers.get("X-Api-Key") != api_key:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    body = request.get_data()
    if not body:
        return jsonify({"ok": False, "error": "Empty body"}), 400

    name = request.args.get("name", "push").strip()
    slug = "".join(c if c.isalnum() or c == "-" else "-" for c in name.lower()).strip("-") or "push"
    ics_path = DATA_FILE.parent / f"pto-push-{slug}.ics"
    ics_path.write_bytes(body)

    state = load_state()
    cals = state.setdefault("pto_calendars", [])
    marker = f"push:{slug}"
    existing = next((c for c in cals if c.get("push_id") == marker), None)
    if existing:
        existing["ics_url"] = str(ics_path)
        existing["name"] = name
    else:
        cals.append({
            "id": str(uuid.uuid4()),
            "push_id": marker,
            "name": name,
            "ics_url": str(ics_path),
            "match_by": request.args.get("match_by", "summary"),
            "poll_interval_minutes": 15,
            "enabled": True,
            "last_sync": None,
            "last_sync_status": None,
            "last_sync_events": 0,
        })
    state["pto_auto_enabled"] = True
    save_state(state)

    try:
        result = sync_pto_calendars()
        logging.info("ICS upload sync (%s): %s", name, result)
        return jsonify({"ok": True, "file": str(ics_path), **result})
    except Exception as exc:
        logging.error("ICS upload sync failed: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/pto_calendars/toggle_auto", methods=["POST"])
def toggle_pto_auto():
    """Enable/disable automatic background PTO sync."""
    state = load_state()
    state["pto_auto_enabled"] = not state.get("pto_auto_enabled", False)
    save_state(state)
    if (request.accept_mimetypes.best == "application/json"
            or request.headers.get("X-Requested-With") == "fetch"):
        return jsonify({"ok": True, "enabled": state["pto_auto_enabled"]})
    return _redirect_back()


# ---------------------------------------------------------------------------
# Background PTO sync worker
# ---------------------------------------------------------------------------

_pto_sync_stop = threading.Event()


def _pto_sync_worker() -> None:
    """Daemon thread that periodically syncs PTO calendars."""
    logging.info("PTO sync worker started")
    while not _pto_sync_stop.is_set():
        try:
            state = load_state()
        except Exception:
            _pto_sync_stop.wait(60)
            continue

        if not state.get("pto_auto_enabled", False):
            _pto_sync_stop.wait(60)
            continue

        calendars = [c for c in state.get("pto_calendars", []) if c.get("enabled", True)]
        if not calendars:
            _pto_sync_stop.wait(60)
            continue

        min_interval = min((c.get("poll_interval_minutes", 15) for c in calendars), default=15)

        try:
            result = sync_pto_calendars()
            logging.info("PTO background sync: %s", result)
        except Exception as exc:
            logging.error("PTO sync worker error: %s", exc)

        _pto_sync_stop.wait(max(60, min_interval * 60))


def _start_pto_sync_worker() -> None:
    t = threading.Thread(target=_pto_sync_worker, daemon=True, name="pto-sync")
    t.start()


_start_pto_sync_worker()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
