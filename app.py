import json
import logging
import os
import uuid
from datetime import datetime, timezone, timedelta, time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from croniter import croniter
from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, render_template, request, url_for
from zoneinfo import ZoneInfo

# Load .env from the app directory first, then walk up to find a project-root .env.
# In OpenShift the env vars are injected directly so load_dotenv is a no-op.
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent.parent / ".env")

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
    return json.loads(DATA_FILE.read_text())


def save_state(state: Dict[str, List[Dict]]) -> None:
    DATA_FILE.write_text(json.dumps(state, indent=2))



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
    schedules = eval_state.get("schedules", [])

    current_schedules, current_started_utc = compute_current_overlaps(eval_state)
    current_schedule, _single_started, next_schedule, next_start_utc = compute_current_shift(eval_state)
    member_map = get_member_map(state)

    current_members = [member_map.get(s.get("member_id")) for s in current_schedules]
    current_member = member_map.get(current_schedule["member_id"]) if current_schedule else None
    next_member = member_map.get(next_schedule["member_id"]) if next_schedule else None

    zone_groups = [g for g in groups if g.get("zone_id") == selected_zone] if selected_zone else groups

    members_on_pto = set(state.get("pto", []))

    return render_template(
        "index.html",
        members=members,
        members_on_pto=members_on_pto,
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


@app.route("/members/toggle_schedules/<member_id>", methods=["POST"])
def toggle_member_schedules(member_id: str):
    """Activate or deactivate all schedules for a member (PTO management).

    PTO status is stored explicitly in ``state["pto"]`` (a list of member ids)
    so it is independent of schedule active states.  Multiple members can be
    on PTO simultaneously.

    Priority cascade logic:
      - **PTO On**: deactivates all of the member's schedules, then for each
        affected group promotes the next-priority member who is NOT on PTO.
      - **PTO Off**: reactivates schedules only where the member is the
        highest-priority non-PTO person, then demotes lower-priority members
        that were covering.

    Schedules without a priority are always toggled directly (no cascade).
    """
    state = load_state()
    active_param = (request.form.get("active") or "").strip().lower()
    active_value = active_param in ("1", "true", "on", "yes")
    group_filter = request.form.get("group", "").strip() or None
    all_schedules = state.get("schedules", [])
    pto_set = set(state.get("pto", []))

    member_map = get_member_map(state)
    member = member_map.get(member_id, {})
    member_name = member.get("name", member_id)

    # Update the explicit PTO list
    if not active_value:
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

    # -- Priority cascade for affected groups --
    for grp in affected_groups:
        grp_schedules = [s for s in all_schedules if s.get("group") == grp and s.get("priority") is not None]
        if not grp_schedules:
            continue

        if not active_value:
            # PTO On: promote the next-priority member who is NOT on PTO
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
                        logging.info("Cascade: promoted %s (p%s) in group %s",
                                     member_map.get(s["member_id"], {}).get("name", s["member_id"]),
                                     s.get("priority"), grp)
        else:
            # PTO Off: demote lower-priority members in this group
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
                    logging.info("Cascade: demoted %s (p%s) in group %s",
                                 member_map.get(s["member_id"], {}).get("name", s["member_id"]),
                                 s.get("priority"), grp)

    save_state(state)

    action = "deactivated" if not active_value else "activated"
    logging.info("PTO %s for %s: %d %s, %d skipped, %d promoted, %d demoted (group=%s)",
                 "On" if not active_value else "Off", member_name,
                 toggled, action, skipped, promoted, demoted, group_filter)

    if request.accept_mimetypes.best == "application/json" or request.headers.get("X-Requested-With") == "fetch":
        return jsonify({
            "ok": True,
            "member_id": member_id,
            "member_name": member_name,
            "active": active_value,
            "count": toggled,
            "skipped": skipped,
            "promoted": promoted,
            "demoted": demoted,
        })
    return _redirect_back()


# ---------------------------------------------------------------------------
# Schedule CRUD
# ---------------------------------------------------------------------------

@app.route("/schedule/add", methods=["POST"])
def add_schedule():
    state = load_state()
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

        new_schedule = {
            "id": str(uuid.uuid4()),
            "member_id": member_id,
            "start_time": start_time,
            "end_time": end_time or None,
            "days": days_int,
            "timezone": canonical_tz,
            "active": True,
            "group": group,
            "priority": priority,
        }
        state["schedules"].append(new_schedule)
        save_state(state)
        logging.info("Added range schedule: %s %s-%s (%s) days=%s group=%s p=%s", member_id, start_time, end_time or "", canonical_tz, days_int, group, priority)
        return _redirect_back()

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
        "group": group,
        "priority": priority,
    }
    state["schedules"].append(new_schedule)
    save_state(state)
    logging.info("Added cron schedule: %s (%s) group=%s p=%s", cron, canonical_tz, group, priority)
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

    # Round-robin over overlapping active schedules.
    # rr state is persisted to the original (unfiltered) state.
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
