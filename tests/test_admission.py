#!/usr/bin/env python3
"""Phase-1 admission-control tests for relay.py.

Covers: join (code gating), quarantine of pending agents, operator approval via
the admin CLI, identity binding (no impersonation), active message flow, polite
denial, and the legacy shared-token migration path. No pytest needed:

    pip install fastapi httpx
    python3 tests/test_admission.py
"""
import os
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
os.environ["A2A_DB"] = tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
os.environ["A2A_BUS_TOKENS"] = "research-x:legacysharedtoken"
os.environ["A2A_DEFAULT_JOIN_CODE"] = "142857"
os.environ.setdefault("A2A_ALLOW_LEGACY_TOKEN", "1")

sys.path.insert(0, str(REPO / "server"))
import relay  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), name)


def hdr(token, agent=None):
    h = {"Authorization": f"Bearer {token}"}
    if agent:
        h["X-A2A-Agent"] = agent
    return h


BASE = "/v1/b/research-x"

with TestClient(relay.app) as c:
    # --- join: code gating ---
    r = c.post(f"{BASE}/join", json={"agent_id": "boston-a", "join_code": "000000", "card": {}})
    check("join with bad code -> 403", r.status_code == 403)

    r = c.post(f"{BASE}/join", json={
        "agent_id": "boston-a", "join_code": "142857",
        "card": {"nickname": "boston", "job_types": ["storage", "eval"],
                 "cluster": {"name": "boston", "scheduler": "slurm"}}})
    check("join with good code -> 200", r.status_code == 200)
    tok = r.json().get("token")
    check("join returns a token", bool(tok))
    check("join state is pending", r.json().get("state") == "pending")

    # --- quarantine: a pending agent can do nothing but poll admission ---
    check("pending cannot list peers (403)",
          c.get(f"{BASE}/agents", headers=hdr(tok, "boston-a")).status_code == 403)
    check("pending cannot send (403)",
          c.post(f"{BASE}/messages", headers=hdr(tok, "boston-a"),
                 json={"id": "m1", "from": "boston-a", "to": "_broadcast",
                       "type": "text", "body": "hi"}).status_code == 403)
    a = c.get(f"{BASE}/admission", headers=hdr(tok, "boston-a"))
    check("pending admission readable", a.status_code == 200 and a.json().get("state") == "pending")
    check("pending agent invisible to a legacy peer",
          "boston-a" not in [x.get("agent_id") for x in
                             c.get(f"{BASE}/agents",
                                   headers=hdr("legacysharedtoken", "legacy-peer")).json()])

    # --- operator approval (Phase-1 CLI) ---
    relay.admin_cli(["approve", "research-x", "boston-a"])
    check("admission active after approve",
          c.get(f"{BASE}/admission", headers=hdr(tok, "boston-a")).json().get("state") == "active")
    names = [x.get("agent_id") for x in c.get(f"{BASE}/agents", headers=hdr(tok, "boston-a")).json()]
    check("approved agent now visible", "boston-a" in names)
    check("active can refresh its card",
          c.post(f"{BASE}/agents", headers=hdr(tok, "boston-a"),
                 json={"agent_id": "boston-a", "status": "idle"}).status_code == 200)

    # --- identity binding: token cannot act as anyone else ---
    check("cannot send as another id (400)",
          c.post(f"{BASE}/messages", headers=hdr(tok, "boston-a"),
                 json={"id": "m2", "from": "evil", "to": "_broadcast",
                       "type": "text"}).status_code == 400)
    check("cannot post a card for another id (400)",
          c.post(f"{BASE}/agents", headers=hdr(tok, "boston-a"),
                 json={"agent_id": "evil"}).status_code == 400)

    # --- two approved agents exchange a real message ---
    tok2 = c.post(f"{BASE}/join", json={"agent_id": "tower-a", "join_code": "142857",
                                        "card": {"nickname": "tower"}}).json()["token"]
    relay.admin_cli(["approve", "research-x", "tower-a"])
    check("boston -> tower send ok",
          c.post(f"{BASE}/messages", headers=hdr(tok, "boston-a"),
                 json={"id": "m3", "from": "boston-a", "to": "tower-a",
                       "type": "text", "body": "hello tower"}).status_code == 200)
    inbox = c.get(f"{BASE}/messages", headers=hdr(tok2, "tower-a")).json().get("messages", [])
    check("tower receives the message", any(m["msg"].get("id") == "m3" for m in inbox))

    # --- polite denial ---
    tokd = c.post(f"{BASE}/join", json={"agent_id": "reject-me", "join_code": "142857",
                                        "card": {}}).json()["token"]
    relay.admin_cli(["deny", "research-x", "reject-me", "--message", "no capacity right now"])
    da = c.get(f"{BASE}/admission", headers=hdr(tokd, "reject-me")).json()
    check("denied state reported", da.get("state") == "denied")
    check("denied carries operator message", da.get("message") == "no capacity right now")

    # --- revoke removes from roster ---
    relay.admin_cli(["revoke", "research-x", "tower-a", "--message", "decommissioned"])
    check("revoked agent gone from roster",
          "tower-a" not in [x.get("agent_id") for x in
                            c.get(f"{BASE}/agents", headers=hdr(tok, "boston-a")).json()])
    check("revoked token now blocked (403)",
          c.get(f"{BASE}/agents", headers=hdr(tok2, "tower-a")).status_code == 403)

    # --- legacy migration path + basic errors ---
    check("legacy shared token still works (migration)",
          c.post(f"{BASE}/agents", headers=hdr("legacysharedtoken", "legacy-agent"),
                 json={"agent_id": "legacy-agent", "status": "idle"}).status_code == 200)
    check("unknown bus -> 404",
          c.get("/v1/b/nope/agents", headers=hdr(tok, "boston-a")).status_code == 404)
    check("bad token -> 401",
          c.get(f"{BASE}/agents", headers=hdr("wrong-token", "boston-a")).status_code == 401)

fails = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(fails)}/{len(RESULTS)} passed")
if fails:
    print("FAILED:", ", ".join(fails))
sys.exit(1 if fails else 0)
