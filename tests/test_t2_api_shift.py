"""HTTP contract tests for peek/commit Round-Robin (issue #30)."""

import json
import uuid
from datetime import datetime, timezone

import pytest

import app as shift_app


FIXED_NOW = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)

SAMIK = "Samik Sanyal"
SHASHI = "Shashi Singh"
CARLOS = "Carlos Arias"
CHETAN = "Chetan Tiwary"
WASIM = "Wasim Raja"


def _member(name):
    return {"id": str(uuid.uuid4()), "name": name}


def _all_day_schedule(member_id, group=None):
    return {
        "id": str(uuid.uuid4()),
        "member_id": member_id,
        "timezone": "UTC",
        "start_time": "00:00",
        "end_time": "23:59",
        "days": [0, 1, 2, 3, 4, 5, 6],
        "active": True,
        "group": group,
    }


def _state(names, group=None, extra_groups=None):
    members = [_member(n) for n in names]
    schedules = [_all_day_schedule(m["id"], group=group) for m in members]
    groups = extra_groups or []
    if group and not any(g.get("id") == group for g in groups):
        groups = [{"id": group, "name": group, "zone_id": "z"}] + groups
    return {
        "zones": [{"id": "z", "name": "Z"}],
        "groups": groups,
        "members": members,
        "schedules": schedules,
        "rr": {},
    }


@pytest.fixture
def client(tmp_path, monkeypatch):
    data_file = tmp_path / "state.json"
    data_file.write_text(json.dumps(_state([])))
    monkeypatch.setattr(shift_app, "DATA_FILE", data_file)
    monkeypatch.setattr(shift_app, "get_now_utc", lambda: FIXED_NOW)

    def seed(names, group=None, extra_state=None):
        payload = extra_state if extra_state is not None else _state(names, group=group)
        data_file.write_text(json.dumps(payload))

    shift_app.app.config["TESTING"] = True
    with shift_app.app.test_client() as test_client:
        test_client.seed = seed
        test_client.data_file = data_file
        yield test_client


def test_peek_empty_pool_returns_nobody(client):
    client.seed([])
    resp = client.get("/api/shift")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["name"] is None
    assert body["on_shift"] is False


def test_omitted_advance_does_not_rotate(client):
    client.seed([CARLOS, CHETAN, SAMIK])
    first = client.get("/api/shift").get_json()["name"]
    second = client.get("/api/shift").get_json()["name"]
    assert first == CARLOS
    assert second == CARLOS


def test_commit_requires_assigned_name(client):
    client.seed([CARLOS, CHETAN])
    resp = client.post("/api/shift/commit", json={})
    assert resp.status_code == 400


def test_commit_samik_idle_peeks_return_shashi(client):
    client.seed([CARLOS, CHETAN, SAMIK, SHASHI, WASIM])
    commit = client.post("/api/shift/commit", json={"assigned_name": SAMIK})
    assert commit.status_code == 200
    assert commit.get_json()["name"] == SHASHI
    assert client.get("/api/shift").get_json()["name"] == SHASHI
    assert client.get("/api/shift").get_json()["name"] == SHASHI


def test_single_person_pool_always_that_person(client):
    client.seed([SAMIK])
    assert client.get("/api/shift").get_json()["name"] == SAMIK
    client.post("/api/shift/commit", json={"assigned_name": SAMIK})
    assert client.get("/api/shift").get_json()["name"] == SAMIK
    assert client.get("/api/shift").get_json()["round_robin"] is False


def test_commit_last_in_pool_wraps_to_first(client):
    client.seed([CARLOS, CHETAN, SAMIK])
    client.post("/api/shift/commit", json={"assigned_name": SAMIK})
    assert client.get("/api/shift").get_json()["name"] == CARLOS


def test_wrap_when_last_assignee_left_the_pool(client):
    """Chetan last; new pool Carlos, Samik, Shashi → Samik, not Carlos."""
    full = _state([CARLOS, CHETAN, SAMIK, SHASHI])
    client.seed([], extra_state=full)
    client.post("/api/shift/commit", json={"assigned_name": CHETAN})
    persisted = json.loads(client.data_file.read_text())
    chetan_id = next(m["id"] for m in persisted["members"] if m["name"] == CHETAN)
    persisted["members"] = [m for m in persisted["members"] if m["id"] != chetan_id]
    persisted["schedules"] = [s for s in persisted["schedules"] if s["member_id"] != chetan_id]
    client.data_file.write_text(json.dumps(persisted))
    assert client.get("/api/shift").get_json()["name"] == SAMIK


def test_groups_do_not_share_last_assigned(client):
    members = [_member(CARLOS), _member(SAMIK), _member(SHASHI)]
    ids = {m["name"]: m["id"] for m in members}
    state = {
        "zones": [{"id": "z", "name": "Z"}],
        "groups": [
            {"id": "anz", "name": "ANZ", "zone_id": "z"},
            {"id": "emea", "name": "EMEA", "zone_id": "z"},
        ],
        "members": members,
        "schedules": [
            _all_day_schedule(ids[CARLOS], group="anz"),
            _all_day_schedule(ids[SAMIK], group="anz"),
            _all_day_schedule(ids[SHASHI], group="anz"),
            _all_day_schedule(ids[CARLOS], group="emea"),
            _all_day_schedule(ids[SAMIK], group="emea"),
            _all_day_schedule(ids[SHASHI], group="emea"),
        ],
        "rr": {},
    }
    client.seed([], extra_state=state)
    client.post("/api/shift/commit?group=anz", json={"assigned_name": SAMIK})
    assert client.get("/api/shift?group=anz").get_json()["name"] == SHASHI
    assert client.get("/api/shift?group=emea").get_json()["name"] == CARLOS


def test_advance_true_does_not_rotate(client):
    client.seed([CARLOS, CHETAN, SAMIK])
    assert client.get("/api/shift").get_json()["name"] == CARLOS
    after = client.get("/api/shift?advance=true").get_json()["name"]
    assert after == CARLOS
    assert client.get("/api/shift").get_json()["name"] == CARLOS
