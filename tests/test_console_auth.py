#!/usr/bin/env python3
"""Phase-2 tests: Google-gated console, signed sessions, and step-up approvals.

The real Google round-trip is replaced by stubbing exchange_code_for_identity;
everything else (state signing, session cookie, allowlist, step-up action
dispatch, read-endpoint gating) is exercised end-to-end. Run:

    pip install fastapi httpx
    python3 tests/test_console_auth.py
"""
import os
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
os.environ["A2A_DB"] = tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
os.environ["A2A_BUS_TOKENS"] = "research-x:legacy"
os.environ["A2A_DEFAULT_JOIN_CODE"] = "142857"
os.environ["A2A_SESSION_KEY"] = "test-session-key-0123456789"
os.environ["A2A_OIDC_CLIENT_ID"] = "dummy.apps.googleusercontent.com"
os.environ["A2A_OIDC_CLIENT_SECRET"] = "dummy-secret"
os.environ["A2A_ADMIN_EMAILS"] = "op@example.com"
os.environ["A2A_COOKIE_SECURE"] = "0"      # let httpx keep the cookie over http
os.environ["A2A_PUBLIC_URL"] = "http://testserver"

sys.path.insert(0, str(REPO / "server"))
import relay  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), name)


def set_identity(email):
    relay.exchange_code_for_identity = lambda code: email


def state(**kw):
    base = {"a": "", "bus": "", "agent_id": "", "jc": "", "msg": "",
            "n": "t", "ts": time.time()}
    base.update(kw)
    return relay._mktok(base)


BASE = "/v1/b/research-x"

with TestClient(relay.app) as c:
    # --- read endpoints require an operator session ---
    check("overview without session -> 401",
          c.get(f"{BASE}/admin/overview").status_code == 401)
    check("pending without session -> 401",
          c.get(f"{BASE}/admin/pending").status_code == 401)
    check("/ui is served (no session needed to see sign-in)",
          c.get("/ui").status_code == 200)
    check("no direct approve route (step-up only)",
          c.get(f"{BASE}/admin/approve").status_code == 404)

    # --- callback rejects a non-allowlisted Google account ---
    set_identity("intruder@example.com")
    r = c.get(f"/auth/callback?code=x&state={state()}", follow_redirects=False)
    check("callback rejects non-allowlisted email -> 403", r.status_code == 403)
    check("no session was set for intruder",
          c.get(f"{BASE}/admin/overview").status_code == 401)

    # --- callback rejects a forged/expired state ---
    set_identity("op@example.com")
    check("garbage state -> 400",
          c.get("/auth/callback?code=x&state=not.a.valid.token",
                follow_redirects=False).status_code == 400)
    check("expired state -> 400",
          c.get(f"/auth/callback?code=x&state={state(ts=time.time()-1000)}",
                follow_redirects=False).status_code == 400)

    # --- a plain sign-in establishes the 24h view session ---
    r = c.get(f"/auth/callback?code=x&state={state()}", follow_redirects=False)
    check("login redirects to /ui", r.status_code == 302 and r.headers["location"] == "/ui")
    check("login set a session cookie", "a2a_session" in r.headers.get("set-cookie", ""))
    check("overview now authorized", c.get(f"{BASE}/admin/overview").status_code == 200)

    # --- step-up approve: join an agent, approve only via a fresh callback ---
    tok = c.post(f"{BASE}/join", json={"agent_id": "boston-a", "join_code": "142857",
                                       "card": {"nickname": "boston"}}).json()["token"]
    ov = c.get(f"{BASE}/admin/overview").json()
    check("pending agent shows in admissions",
          any(a["agent_id"] == "boston-a" for a in ov["admissions"]))
    c.get(f"/auth/callback?code=x&state={state(a='approve', bus='research-x', agent_id='boston-a')}",
          follow_redirects=False)
    adm = c.get(f"{BASE}/admission", headers={"Authorization": f"Bearer {tok}",
                                              "X-A2A-Agent": "boston-a"}).json()
    check("step-up approve activated the agent", adm.get("state") == "active")
    ov = c.get(f"{BASE}/admin/overview").json()
    check("approved agent left the admissions queue",
          all(a["agent_id"] != "boston-a" for a in ov["admissions"]))
    check("approved agent now in roster",
          any(a["agent_id"] == "boston-a" for a in ov["agents"]))

    # --- step-up deny carries the operator's message ---
    tok2 = c.post(f"{BASE}/join", json={"agent_id": "nope-a", "join_code": "142857",
                                        "card": {}}).json()["token"]
    c.get(f"/auth/callback?code=x&state={state(a='deny', bus='research-x', agent_id='nope-a', msg='no capacity')}",
          follow_redirects=False)
    da = c.get(f"{BASE}/admission", headers={"Authorization": f"Bearer {tok2}",
                                             "X-A2A-Agent": "nope-a"}).json()
    check("step-up deny set denied state", da.get("state") == "denied")
    check("denied carries operator message", da.get("message") == "no capacity")

    # --- step-up join-code rotation ---
    c.get(f"/auth/callback?code=x&state={state(a='joincode', bus='research-x', jc='555000')}",
          follow_redirects=False)
    check("old join code rejected after rotation",
          c.post(f"{BASE}/join", json={"agent_id": "late-a", "join_code": "142857",
                                       "card": {}}).status_code == 403)
    check("new join code accepted",
          c.post(f"{BASE}/join", json={"agent_id": "late-a", "join_code": "555000",
                                       "card": {}}).status_code == 200)

    # --- logout clears the session ---
    c.get("/auth/logout", follow_redirects=False)
    check("overview blocked after logout",
          c.get(f"{BASE}/admin/overview").status_code == 401)

fails = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(fails)}/{len(RESULTS)} passed")
if fails:
    print("FAILED:", ", ".join(fails))
sys.exit(1 if fails else 0)
