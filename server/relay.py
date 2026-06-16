#!/usr/bin/env python3
"""
relay.py — A2A-Code v2 bus server. FastAPI + SQLite (WAL), single file.

One relay hosts many buses (project namespaces); each bus has a bearer token.
Wire format of cards and messages is the frozen v1 JSON (schema a2a-code/v0):
the server stores envelopes verbatim, assigns a global `seq`, and wakes
long-pollers. It never carries file bytes and never sees bucket credentials.

Config (env):
  A2A_DB              SQLite path                 (default ./relay.db)
  A2A_BUS_TOKENS      "bus1:token1,bus2:token2"   (required)
  A2A_RETENTION_DAYS  delete messages older than  (default 30)
  A2A_MAX_WAIT        long-poll hold cap, seconds (default 300)
  A2A_HOST/A2A_PORT   bind address for __main__   (default 127.0.0.1:8080)
  -- admission control (per-agent tokens) --
  A2A_TOKEN_PEPPER    pepper for hashing tokens at rest   (recommended)
  A2A_DEFAULT_JOIN_CODE   join code seeded per bus        (default 142857)
  A2A_ALLOW_LEGACY_TOKEN  accept the old shared token for migration (default 1)
  -- operator console (Google OIDC) --
  A2A_OIDC_CLIENT_ID / A2A_OIDC_CLIENT_SECRET   Google OAuth web client
  A2A_ADMIN_EMAILS    comma-list of operator emails allowed to sign in
  A2A_SESSION_KEY     HMAC key for the 24h console session + OAuth state
  A2A_PUBLIC_URL      external base URL for the OAuth redirect (default duckdns host)
  A2A_COOKIE_SECURE   set 0 only for local non-TLS testing (default 1)

Run:  A2A_BUS_TOKENS=research-x:$(openssl rand -hex 24) python3 relay.py
Admin (headless): python3 relay.py admin pending|approve|deny|revoke|joincode
API:  participate routes under /v1/b/{bus} take a per-agent bearer token
      (X-A2A-Agent must match the token's bound id). New agents POST /join
      (join-code gated) and poll GET /admission. The console /ui and /admin/*
      reads require an operator Google session (GET /auth/login); approvals are
      step-up — a fresh sign-in per action via /auth/callback.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import time
import urllib.parse
import urllib.request
from contextlib import contextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, RedirectResponse

DB_PATH = os.environ.get("A2A_DB", "./relay.db")
RETENTION_DAYS = int(os.environ.get("A2A_RETENTION_DAYS", "30"))
MAX_WAIT = int(os.environ.get("A2A_MAX_WAIT", "300"))
BROADCAST = "_broadcast"
STALE_AFTER_S = 900
AGENT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,31}$")
TOKEN_PEPPER = os.environ.get("A2A_TOKEN_PEPPER", "")
DEFAULT_JOIN_CODE = os.environ.get("A2A_DEFAULT_JOIN_CODE", "142857")
# Migration aid: accept the old shared bus token (with self-asserted identity)
# until cutover. Set A2A_ALLOW_LEGACY_TOKEN=0 to enforce per-agent tokens only.
ALLOW_LEGACY_TOKEN = os.environ.get("A2A_ALLOW_LEGACY_TOKEN", "1") not in ("0", "false", "no", "")
DEFAULT_DENY_MSG = ("Your request to join this bus was declined by the operator. "
                    "Reach out to them if you believe this is a mistake.")

app = FastAPI(title="a2a-relay", docs_url=None, redoc_url=None)
_conds: dict[str, asyncio.Condition] = {}   # one Condition per bus


def bus_tokens() -> dict:
    raw = os.environ.get("A2A_BUS_TOKENS", "")
    pairs = [p for p in raw.split(",") if ":" in p]
    return dict(p.split(":", 1) for p in pairs)


def known_buses() -> set:
    """Buses this relay serves: those in A2A_BUS_TOKENS (legacy/bootstrap) plus
    any created in bus_settings — the post-cutover source of truth, so the relay
    can run with no shared token at all."""
    buses = set(bus_tokens())
    try:
        with db() as c:
            for r in c.execute("SELECT bus FROM bus_settings"):
                buses.add(r["bus"])
    except sqlite3.OperationalError:
        pass  # bus_settings not created yet (pre-init)
    return buses


def bus_known(bus: str) -> bool:
    if bus in bus_tokens():
        return True
    try:
        with db() as c:
            return c.execute("SELECT 1 FROM bus_settings WHERE bus=?",
                             (bus,)).fetchone() is not None
    except sqlite3.OperationalError:
        return False


@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with db() as c:
        c.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS agents (
          bus TEXT, agent_id TEXT, card TEXT NOT NULL, updated REAL NOT NULL,
          PRIMARY KEY (bus, agent_id));
        CREATE TABLE IF NOT EXISTS messages (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          bus TEXT NOT NULL, recipient TEXT NOT NULL, sender TEXT NOT NULL,
          mtype TEXT NOT NULL, thread TEXT, ts REAL NOT NULL,
          msg TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS inbox ON messages (bus, recipient, seq);
        CREATE TABLE IF NOT EXISTS cursors (
          bus TEXT, agent_id TEXT, seq INTEGER NOT NULL,
          PRIMARY KEY (bus, agent_id));
        CREATE TABLE IF NOT EXISTS acks (
          bus TEXT, agent_id TEXT, seq INTEGER,
          PRIMARY KEY (bus, agent_id, seq));
        CREATE TABLE IF NOT EXISTS agent_auth (
          token_sha256 TEXT PRIMARY KEY,
          bus TEXT NOT NULL, agent_id TEXT NOT NULL,
          state TEXT NOT NULL, scope TEXT NOT NULL DEFAULT 'agent',
          req_card TEXT, source_ip TEXT, message TEXT,
          created REAL NOT NULL, decided_at REAL, decided_by TEXT,
          expires_at REAL);
        CREATE UNIQUE INDEX IF NOT EXISTS one_active_cred
          ON agent_auth (bus, agent_id) WHERE state='active';
        CREATE TABLE IF NOT EXISTS bus_settings (
          bus TEXT PRIMARY KEY, join_code_sha TEXT NOT NULL,
          updated REAL NOT NULL, updated_by TEXT);
        """)


def seed_bus_settings() -> None:
    """Give every configured bus a join code (default 142857) if it lacks one."""
    with db() as c:
        for bus in bus_tokens():
            c.execute("INSERT OR IGNORE INTO bus_settings (bus, join_code_sha, "
                      "updated) VALUES (?,?,?)",
                      (bus, _hash(DEFAULT_JOIN_CODE), time.time()))


def cond_for(bus: str) -> asyncio.Condition:
    if bus not in _conds:
        _conds[bus] = asyncio.Condition()
    return _conds[bus]


def _hash(s: str) -> str:
    return hashlib.sha256((TOKEN_PEPPER + s).encode()).hexdigest()


def _bearer(request: Request) -> str:
    h = request.headers.get("authorization", "")
    return h[7:] if h.startswith("Bearer ") else ""


def cred_row(bus: str, token: str):
    if not token:
        return None
    with db() as c:
        return c.execute("SELECT * FROM agent_auth WHERE bus=? AND token_sha256=?",
                         (bus, _hash(token))).fetchone()


def authenticate(bus: str, request: Request) -> "tuple[str, str]":
    """Return (agent_id, state). A per-agent token binds identity on the server
    (unforgeable); the legacy shared bus token is accepted only during migration
    and trusts the self-asserted X-A2A-Agent header."""
    if not bus_known(bus):
        raise HTTPException(404, f"unknown bus {bus!r}")
    token = _bearer(request)
    row = cred_row(bus, token)
    if row is not None:
        if row["expires_at"] and row["expires_at"] < time.time():
            raise HTTPException(401, "credential expired")
        return row["agent_id"], row["state"]
    if ALLOW_LEGACY_TOKEN and token and token == bus_tokens().get(bus):
        agent = request.headers.get("x-a2a-agent", "")
        if not agent:
            raise HTTPException(400, "missing X-A2A-Agent header")
        return agent, "active"  # MIGRATION ONLY — gated by A2A_ALLOW_LEGACY_TOKEN
    raise HTTPException(401, "bad or missing bearer token")


def auth(bus: str, request: Request) -> str:
    """Authenticated *and admitted* caller; used by every participate endpoint.
    Pending/denied/revoked callers are quarantined here."""
    agent, state = authenticate(bus, request)
    if state != "active":
        if state in ("denied", "revoked"):
            raise HTTPException(403, "credential revoked")
        raise HTTPException(403, "pending operator approval")
    return agent


# --------------------------------------------- operator auth (Google OIDC)
#
# The console and every approval action are gated by the operator's Google
# identity, never the bus token. A successful sign-in mints a signed 24h view
# session; each approve/deny/revoke/join-code change requires a *fresh* sign-in
# (step-up), carried as a signed `state` value through the OAuth round-trip.
OIDC_CLIENT_ID = os.environ.get("A2A_OIDC_CLIENT_ID", "")
OIDC_CLIENT_SECRET = os.environ.get("A2A_OIDC_CLIENT_SECRET", "")
ADMIN_EMAILS = {e.strip().lower() for e in
                os.environ.get("A2A_ADMIN_EMAILS", "").split(",") if e.strip()}
SESSION_KEY = os.environ.get("A2A_SESSION_KEY", "")
PUBLIC_URL = os.environ.get("A2A_PUBLIC_URL",
                            "https://adu-a2a-code.duckdns.org").rstrip("/")
COOKIE_SECURE = os.environ.get("A2A_COOKIE_SECURE", "1") not in ("0", "false", "no", "")
SESSION_COOKIE = "a2a_session"
SESSION_TTL = 24 * 3600
STATE_TTL = 600
GOOGLE_AUTH = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO = "https://openidconnect.googleapis.com/v1/userinfo"


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")


def _unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def _sign(body: str) -> str:
    return _b64(hmac.new(SESSION_KEY.encode(), body.encode(), hashlib.sha256).digest())


def _mktok(data: dict) -> str:
    body = _b64(json.dumps(data, separators=(",", ":")).encode())
    return f"{body}.{_sign(body)}"


def _readtok(tok: str):
    try:
        body, sig = tok.split(".", 1)
    except (ValueError, AttributeError):
        return None
    if not (SESSION_KEY and hmac.compare_digest(sig, _sign(body))):
        return None
    try:
        return json.loads(_unb64(body))
    except Exception:
        return None


def make_session(email: str) -> str:
    return _mktok({"email": email, "exp": time.time() + SESSION_TTL})


def session_email(request: Request):
    d = _readtok(request.cookies.get(SESSION_COOKIE, ""))
    if not d or d.get("exp", 0) < time.time():
        return None
    em = (d.get("email") or "").lower()
    return em if em in ADMIN_EMAILS else None


def require_view_session(request: Request) -> str:
    em = session_email(request)
    if not em:
        raise HTTPException(401, "operator sign-in required (GET /auth/login)")
    return em


def exchange_code_for_identity(code: str):
    """Authorization code -> verified operator email (stdlib only, no crypto dep).
    Exchanges the code at Google's token endpoint, then reads userinfo with the
    resulting access token. Patched out in tests."""
    data = urllib.parse.urlencode({
        "code": code, "client_id": OIDC_CLIENT_ID,
        "client_secret": OIDC_CLIENT_SECRET,
        "redirect_uri": f"{PUBLIC_URL}/auth/callback",
        "grant_type": "authorization_code"}).encode()
    with urllib.request.urlopen(urllib.request.Request(GOOGLE_TOKEN, data=data),
                                timeout=15) as r:
        tok = json.loads(r.read())
    access = tok.get("access_token")
    if not access:
        return None
    info_req = urllib.request.Request(
        GOOGLE_USERINFO, headers={"Authorization": f"Bearer {access}"})
    with urllib.request.urlopen(info_req, timeout=15) as r:
        info = json.loads(r.read())
    if not info.get("email_verified"):
        return None
    return (info.get("email") or "").lower()


# ------------------------------------------------- admin actions (shared)

def agents_snapshot(c, bus: str) -> list:
    out = []
    for r in c.execute("SELECT card, updated FROM agents WHERE bus=? "
                       "ORDER BY agent_id", (bus,)):
        card = json.loads(r["card"])
        age = int(time.time() - r["updated"])
        card["_heartbeat_age_s"] = age
        card["_stale"] = age > STALE_AFTER_S
        out.append(card)
    return out


def pending_snapshot(c, bus: str) -> list:
    out = []
    for r in c.execute("SELECT agent_id, req_card, source_ip, created FROM "
                       "agent_auth WHERE bus=? AND state='pending' ORDER BY created",
                       (bus,)):
        card = json.loads(r["req_card"] or "{}")
        clu = card.get("cluster")
        out.append({"agent_id": r["agent_id"], "nickname": card.get("nickname"),
                    "cluster": clu.get("name") if isinstance(clu, dict) else clu,
                    "job_types": card.get("job_types") or [],
                    "gpus": (card.get("capabilities") or {}).get("gpus"),
                    "source_ip": r["source_ip"], "since": r["created"]})
    return out


def adm_approve(c, bus: str, agent_id: str, by: str) -> bool:
    r = c.execute("SELECT req_card FROM agent_auth WHERE bus=? AND agent_id=? "
                  "AND state='pending'", (bus, agent_id)).fetchone()
    if not r:
        return False
    ts = time.time()
    c.execute("UPDATE agent_auth SET state='active', decided_at=?, decided_by=? "
              "WHERE bus=? AND agent_id=? AND state='pending'",
              (ts, by, bus, agent_id))
    card = json.loads(r["req_card"] or "{}")
    card["agent_id"] = agent_id
    c.execute("INSERT INTO agents (bus, agent_id, card, updated) VALUES (?,?,?,?) "
              "ON CONFLICT(bus, agent_id) DO UPDATE SET card=excluded.card, "
              "updated=excluded.updated", (bus, agent_id, json.dumps(card), ts))
    return True


def adm_decide(c, bus: str, agent_id: str, new: str, target: str,
               by: str, message: str = "") -> bool:
    n = c.execute("UPDATE agent_auth SET state=?, message=?, decided_at=?, "
                  "decided_by=? WHERE bus=? AND agent_id=? AND state=?",
                  (new, message or None, time.time(), by, bus, agent_id,
                   target)).rowcount
    if new == "revoked":
        c.execute("DELETE FROM agents WHERE bus=? AND agent_id=?", (bus, agent_id))
    return n > 0


def adm_joincode(c, bus: str, code: str, by: str) -> None:
    c.execute("INSERT INTO bus_settings (bus, join_code_sha, updated, updated_by) "
              "VALUES (?,?,?,?) ON CONFLICT(bus) DO UPDATE SET "
              "join_code_sha=excluded.join_code_sha, updated=excluded.updated, "
              "updated_by=excluded.updated_by", (bus, _hash(code), time.time(), by))


# ------------------------------------------------------------ cursor algebra

def cursor_of(c, bus: str, agent: str) -> int:
    row = c.execute("SELECT seq FROM cursors WHERE bus=? AND agent_id=?",
                    (bus, agent)).fetchone()
    return row["seq"] if row else 0


def unread_rows(c, bus: str, agent: str, include_read: bool = False,
                frm: str = "", mtype: str = "", thread: str = "",
                floor: int = 0):
    """floor lets observers (watch) long-poll above a seq without acking."""
    cur = 0 if include_read else max(cursor_of(c, bus, agent), floor)
    q = ("SELECT seq, msg FROM messages WHERE bus=? AND recipient IN (?,?) "
         "AND sender != ? AND seq > ?")
    args = [bus, agent, BROADCAST, agent, cur]
    if not include_read:
        q += (" AND seq NOT IN (SELECT seq FROM acks WHERE bus=? AND agent_id=?)")
        args += [bus, agent]
    for col, val in (("sender", frm), ("mtype", mtype), ("thread", thread)):
        if val:
            q += f" AND {col} = ?"
            args.append(val)
    q += " ORDER BY seq"
    return c.execute(q, args).fetchall()


def apply_acks(c, bus: str, agent: str, seqs: list) -> int:
    c.executemany("INSERT OR IGNORE INTO acks (bus, agent_id, seq) VALUES (?,?,?)",
                  [(bus, agent, int(s)) for s in seqs])
    # compact: cursor = just below the oldest still-unacked message for us
    row = c.execute(
        "SELECT MIN(seq) AS m FROM messages WHERE bus=? AND recipient IN (?,?) "
        "AND sender != ? AND seq > ? AND seq NOT IN "
        "(SELECT seq FROM acks WHERE bus=? AND agent_id=?)",
        (bus, agent, BROADCAST, agent, cursor_of(c, bus, agent), bus, agent),
    ).fetchone()
    if row["m"] is not None:
        new_cur = row["m"] - 1
    else:
        top = c.execute("SELECT MAX(seq) AS m FROM messages WHERE bus=?",
                        (bus,)).fetchone()
        new_cur = top["m"] or 0
    c.execute("INSERT INTO cursors (bus, agent_id, seq) VALUES (?,?,?) "
              "ON CONFLICT(bus, agent_id) DO UPDATE SET seq=MAX(seq, excluded.seq)",
              (bus, agent, new_cur))
    c.execute("DELETE FROM acks WHERE bus=? AND agent_id=? AND seq <= ?",
              (bus, agent, new_cur))
    return new_cur


# ------------------------------------------------------------------- routes

def touch(bus: str, agent: str) -> None:
    """Presence: any authenticated activity refreshes the heartbeat."""
    with db() as c:
        c.execute("UPDATE agents SET updated=? WHERE bus=? AND agent_id=?",
                  (time.time(), bus, agent))


@app.get("/health")
def health():
    return {"ok": True, "schema": "a2a-code/v0", "buses": len(known_buses())}


# Self-distribution: the relay can hand out its own client and skill bundle,
# so a new machine bootstraps with curl — no git host involved. Place the
# files in $A2A_DIST (default /opt/a2a/dist). Public on purpose: this is
# client code, not secrets; the bus itself still requires the bearer token.
DIST = Path(os.environ.get("A2A_DIST", "/opt/a2a/dist"))


@app.get("/client")
def get_client():
    f = DIST / "a2a.py"
    if not f.exists():
        raise HTTPException(404, "no client published (see deploy/README.md)")
    return FileResponse(f, media_type="text/x-python", filename="a2a.py")


@app.get("/skill")
def get_skill():
    f = DIST / "a2a-code.skill"
    if not f.exists():
        raise HTTPException(404, "no skill bundle published (see deploy/README.md)")
    return FileResponse(f, media_type="application/zip",
                        filename="a2a-code.skill")


# --------------------------------------------------------------- admission

@app.post("/v1/b/{bus}/join")
async def join(bus: str, request: Request):
    """Request to join: gated by the bus join code, not a bearer token. Mints a
    *pending* per-agent token. Pending agents are quarantined by auth() until an
    operator approves them — they cannot send, read, or appear to peers."""
    if not bus_known(bus):
        raise HTTPException(404, f"unknown bus {bus!r}")
    body = await request.json()
    agent_id = (body.get("agent_id") or "").strip()
    if not AGENT_ID_RE.match(agent_id):
        raise HTTPException(400, "invalid agent_id")
    card = dict(body.get("card") or {})
    card["agent_id"] = agent_id
    src = request.client.host if request.client else ""
    with db() as c:
        row = c.execute("SELECT join_code_sha FROM bus_settings WHERE bus=?",
                        (bus,)).fetchone()
        if row is None or _hash(body.get("join_code") or "") != row["join_code_sha"]:
            raise HTTPException(403, "bad join code")
        if c.execute("SELECT 1 FROM agent_auth WHERE bus=? AND agent_id=? "
                     "AND state='active'", (bus, agent_id)).fetchone():
            raise HTTPException(409, f"agent_id {agent_id!r} is already active; "
                                     "operator must revoke it to reassign")
        c.execute("DELETE FROM agent_auth WHERE bus=? AND agent_id=? "
                  "AND state IN ('pending','denied')", (bus, agent_id))
        token = secrets.token_urlsafe(32)
        c.execute("INSERT INTO agent_auth (token_sha256, bus, agent_id, state, "
                  "scope, req_card, source_ip, created) VALUES (?,?,?,?,?,?,?,?)",
                  (_hash(token), bus, agent_id, "pending", "agent",
                   json.dumps(card), src, time.time()))
    return {"token": token, "agent_id": agent_id, "state": "pending"}


@app.get("/v1/b/{bus}/admission")
def admission(bus: str, request: Request):
    """Poll your own admission state; carries a polite message when declined."""
    agent, state = authenticate(bus, request)
    out = {"agent_id": agent, "state": state}
    row = cred_row(bus, _bearer(request))
    if row is not None:
        out["since"] = row["created"]
        if state in ("denied", "revoked"):
            out["message"] = row["message"] or DEFAULT_DENY_MSG
    return out


@app.post("/v1/b/{bus}/agents")
async def upsert_agent(bus: str, request: Request):
    agent = auth(bus, request)
    card = await request.json()
    if card.get("agent_id") != agent:
        raise HTTPException(400, "card.agent_id must match X-A2A-Agent")
    with db() as c:
        c.execute("INSERT INTO agents (bus, agent_id, card, updated) VALUES (?,?,?,?) "
                  "ON CONFLICT(bus, agent_id) DO UPDATE SET card=excluded.card, "
                  "updated=excluded.updated",
                  (bus, agent, json.dumps(card), time.time()))
    return {"ok": True}


@app.get("/v1/b/{bus}/agents")
def list_agents(bus: str, request: Request):
    auth(bus, request)
    with db() as c:
        return agents_snapshot(c, bus)


@app.post("/v1/b/{bus}/messages")
async def post_message(bus: str, request: Request):
    agent = auth(bus, request)
    msg = await request.json()
    for field in ("id", "to", "type"):
        if not msg.get(field):
            raise HTTPException(400, f"message missing {field!r}")
    if msg.get("from") != agent:
        raise HTTPException(400, "message.from must match X-A2A-Agent")
    with db() as c:
        cur = c.execute(
            "INSERT INTO messages (bus, recipient, sender, mtype, thread, ts, msg) "
            "VALUES (?,?,?,?,?,?,?)",
            (bus, msg["to"], agent, msg["type"], msg.get("thread"),
             time.time(), json.dumps(msg)))
        seq = cur.lastrowid
    touch(bus, agent)
    cond = cond_for(bus)
    async with cond:
        cond.notify_all()
    return {"id": msg["id"], "seq": seq}


@app.get("/v1/b/{bus}/messages")
async def get_messages(bus: str, request: Request,
                       cursor: int = 0,            # reserved; server tracks acks
                       wait: int = 0,
                       include_read: bool = False,
                       frm: str = Query("", alias="from"),
                       type: str = "", thread: str = ""):
    agent = auth(bus, request)
    deadline = time.monotonic() + min(max(wait, 0), MAX_WAIT)
    touch(bus, agent)  # any authenticated fetch is a liveness signal
    while True:
        with db() as c:
            rows = unread_rows(c, bus, agent, include_read, frm, type, thread,
                               floor=cursor)
            cur = cursor_of(c, bus, agent)
        if rows or time.monotonic() >= deadline:
            return {"cursor": cur,
                    "messages": [{"seq": r["seq"], "msg": json.loads(r["msg"]),
                                  "read": include_read and r["seq"] <= cur}
                                 for r in rows]}
        cond = cond_for(bus)
        try:
            async with cond:
                await asyncio.wait_for(
                    cond.wait(), timeout=min(30.0, deadline - time.monotonic()))
        except asyncio.TimeoutError:
            pass  # re-check loop condition


@app.post("/v1/b/{bus}/ack")
async def post_ack(bus: str, request: Request):
    agent = auth(bus, request)
    body = await request.json()
    seqs = body.get("seqs", [])
    if not isinstance(seqs, list):
        raise HTTPException(400, "seqs must be a list")
    with db() as c:
        new_cur = apply_acks(c, bus, agent, seqs)
    return {"cursor": new_cur}


# ------------------------------------------------------------- auth routes

@app.get("/auth/login")
def auth_login(action: str = "", bus: str = "", agent_id: str = "",
               joincode: str = "", message: str = ""):
    if not OIDC_CLIENT_ID:
        raise HTTPException(503, "OIDC not configured (set A2A_OIDC_CLIENT_ID)")
    st = _mktok({"a": action, "bus": bus, "agent_id": agent_id, "jc": joincode,
                 "msg": message, "n": secrets.token_urlsafe(6), "ts": time.time()})
    params = {"client_id": OIDC_CLIENT_ID,
              "redirect_uri": f"{PUBLIC_URL}/auth/callback",
              "response_type": "code", "scope": "openid email profile",
              "state": st, "access_type": "online",
              # step-up: a sensitive action forces a fresh authentication
              "prompt": "login" if action else "select_account"}
    return RedirectResponse(f"{GOOGLE_AUTH}?{urllib.parse.urlencode(params)}", 302)


@app.get("/auth/callback")
def auth_callback(code: str = "", state: str = ""):
    sd = _readtok(state)
    if not sd or sd.get("ts", 0) < time.time() - STATE_TTL:
        raise HTTPException(400, "bad or expired sign-in state")
    email = exchange_code_for_identity(code)
    if not email or email not in ADMIN_EMAILS:
        raise HTTPException(403, "not an authorized operator")
    action = sd.get("a") or ""
    if action:  # step-up action — only runs on a fresh, authorized sign-in
        bus, agent_id = sd.get("bus", ""), sd.get("agent_id", "")
        with db() as c:
            if action == "approve":
                adm_approve(c, bus, agent_id, email)
            elif action == "deny":
                adm_decide(c, bus, agent_id, "denied", "pending", email, sd.get("msg", ""))
            elif action == "revoke":
                adm_decide(c, bus, agent_id, "revoked", "active", email, sd.get("msg", ""))
            elif action == "joincode":
                adm_joincode(c, bus, sd.get("jc", ""), email)
    resp = RedirectResponse("/ui", 302)
    resp.set_cookie(SESSION_COOKIE, make_session(email), max_age=SESSION_TTL,
                    httponly=True, secure=COOKIE_SECURE, samesite="lax", path="/")
    return resp


@app.get("/auth/logout")
def auth_logout():
    resp = RedirectResponse("/ui", 302)
    resp.delete_cookie(SESSION_COOKIE, path="/")
    return resp


# --------------------------------------------------- console (operator-gated)

@app.get("/ui")
def get_ui():
    f = Path(os.environ.get("A2A_UI", str(Path(__file__).resolve().parent / "ui.html")))
    if not f.exists():
        raise HTTPException(404, "ui.html not installed next to relay.py")
    return FileResponse(f, media_type="text/html")


@app.get("/v1/b/{bus}/admin/overview")
def admin_overview(bus: str, request: Request):
    require_view_session(request)
    if not bus_known(bus):
        raise HTTPException(404, f"unknown bus {bus!r}")
    out = {"agents": [], "pairs": [], "stats": {}, "pending": [], "admissions": []}
    with db() as c:
        out["agents"] = agents_snapshot(c, bus)
        out["admissions"] = pending_snapshot(c, bus)
        for r in c.execute(
                "SELECT sender, recipient, COUNT(*) AS n, MAX(seq) AS last_seq, "
                "MAX(ts) AS last_ts FROM messages WHERE bus=? "
                "GROUP BY sender, recipient", (bus,)):
            out["pairs"].append({"from": r["sender"], "to": r["recipient"],
                                 "count": r["n"], "last_seq": r["last_seq"],
                                 "last_ts": r["last_ts"]})
        top = c.execute("SELECT MAX(seq) AS m, COUNT(*) AS n FROM messages "
                        "WHERE bus=?", (bus,)).fetchone()
        midnight = time.time() - (time.time() % 86400)
        today = c.execute("SELECT COUNT(*) AS n FROM messages WHERE bus=? AND ts>=?",
                          (bus, midnight)).fetchone()["n"]
        out["stats"] = {"last_seq": top["m"] or 0, "total": top["n"],
                        "today": today, "retention_days": RETENTION_DAYS}
        rows = c.execute(
            "SELECT seq, msg FROM messages WHERE bus=? AND mtype IN "
            "('task_request','task_result') ORDER BY seq DESC LIMIT 500",
            (bus,)).fetchall()
        answered, reqs = set(), []
        for r in rows:
            m = json.loads(r["msg"])
            if m["type"] == "task_result" and m.get("reply_to"):
                answered.add(m["reply_to"])
            elif m["type"] == "task_request":
                reqs.append((r["seq"], m))
        for seq, m in sorted(reqs):
            if m["id"] not in answered:
                body = m.get("body")
                text = body.get("text") if isinstance(body, dict) else body
                out["pending"].append(
                    {"seq": seq, "id": m["id"], "from": m["from"], "to": m["to"],
                     "thread": m.get("thread"), "ts": m.get("ts"),
                     "preview": str(text)[:120]})
    return out


@app.get("/v1/b/{bus}/admin/pending")
def admin_pending(bus: str, request: Request):
    require_view_session(request)
    if not bus_known(bus):
        raise HTTPException(404, f"unknown bus {bus!r}")
    with db() as c:
        return {"admissions": pending_snapshot(c, bus)}


@app.get("/v1/b/{bus}/admin/log")
def admin_log(bus: str, request: Request,
              a: str = "", b: str = "", agent: str = "",
              thread: str = "", type: str = "",
              since_seq: int = 0, limit: int = 200,
              include_broadcast: bool = True):
    require_view_session(request)
    q = "SELECT seq, msg, sender, recipient FROM messages WHERE bus=?"
    args: list = [bus]
    if a and b:
        q += " AND ((sender=? AND recipient=?) OR (sender=? AND recipient=?)"
        args += [a, b, b, a]
        if include_broadcast:
            q += " OR (recipient=? AND sender IN (?,?))"
            args += [BROADCAST, a, b]
        q += ")"
    elif agent:
        q += " AND (sender=? OR recipient=?"
        args += [agent, agent]
        if include_broadcast:
            q += " OR recipient=?"
            args.append(BROADCAST)
        q += ")"
    if thread:
        q += " AND thread=?"
        args.append(thread)
    if type:
        q += " AND mtype=?"
        args.append(type)
    q += " AND seq>? ORDER BY seq DESC LIMIT ?"
    args += [since_seq, min(max(limit, 1), 1000)]
    out = []
    with db() as c:
        for r in reversed(c.execute(q, args).fetchall()):
            rec = r["recipient"]
            if rec == BROADCAST:
                acked = None
            else:
                acked = (r["seq"] <= cursor_of(c, bus, rec) or
                         c.execute("SELECT 1 FROM acks WHERE bus=? AND agent_id=? "
                                   "AND seq=?", (bus, rec, r["seq"])).fetchone()
                         is not None)
            out.append({"seq": r["seq"], "msg": json.loads(r["msg"]),
                        "acked": acked})
    return {"messages": out}


@app.get("/v1/b/{bus}/admin/tail")
async def admin_tail(bus: str, request: Request,
                     since_seq: int = 0, wait: int = 25):
    require_view_session(request)
    deadline = time.monotonic() + min(max(wait, 0), MAX_WAIT)
    while True:
        with db() as c:
            rows = c.execute("SELECT seq, msg FROM messages WHERE bus=? AND "
                             "seq>? ORDER BY seq LIMIT 500",
                             (bus, since_seq)).fetchall()
            agents = agents_snapshot(c, bus)
        if rows or time.monotonic() >= deadline:
            return {"messages": [{"seq": r["seq"], "msg": json.loads(r["msg"])}
                                 for r in rows],
                    "next_seq": rows[-1]["seq"] if rows else since_seq,
                    "agents": agents}
        cond = cond_for(bus)
        try:
            async with cond:
                await asyncio.wait_for(
                    cond.wait(), timeout=min(15.0, deadline - time.monotonic()))
        except asyncio.TimeoutError:
            pass


# ---------------------------------------------------------------- lifecycle

async def retention_loop() -> None:
    while True:
        cutoff = time.time() - RETENTION_DAYS * 86400
        with db() as c:
            c.execute("DELETE FROM messages WHERE ts < ?", (cutoff,))
        await asyncio.sleep(6 * 3600)


@app.on_event("startup")
async def startup() -> None:
    init_db()
    seed_bus_settings()
    if not known_buses():
        raise RuntimeError("no buses configured — set A2A_BUS_TOKENS, or create "
                           "one: python3 relay.py admin joincode <bus> <code>")
    asyncio.get_event_loop().create_task(retention_loop())


def admin_cli(argv) -> None:
    """Phase-1 operator admin (run on the relay host, before the Google console):
      python3 relay.py admin pending [bus]
      python3 relay.py admin approve <bus> <agent_id>
      python3 relay.py admin deny|revoke <bus> <agent_id> [--message TEXT]
      python3 relay.py admin joincode <bus> <code>
      python3 relay.py admin list [bus]
    """
    import argparse
    init_db()
    seed_bus_settings()
    ap = argparse.ArgumentParser(prog="relay.py admin")
    sub = ap.add_subparsers(dest="acmd", required=True)
    sub.add_parser("pending").add_argument("bus", nargs="?")
    sub.add_parser("list").add_argument("bus", nargs="?")
    sub.add_parser("buses")
    for name in ("approve", "deny", "revoke"):
        sp = sub.add_parser(name)
        sp.add_argument("bus")
        sp.add_argument("agent_id")
        if name != "approve":
            sp.add_argument("--message", default="")
    jc = sub.add_parser("joincode")
    jc.add_argument("bus")
    jc.add_argument("code")
    a = ap.parse_args(argv)
    ts = time.time()
    with db() as c:
        if a.acmd == "buses":
            for b in sorted(known_buses()):
                print(b)
            return
        if a.acmd in ("pending", "list"):
            q = ("SELECT bus, agent_id, state, source_ip, created, req_card "
                 "FROM agent_auth")
            conds, args = ([], [])
            if a.acmd == "pending":
                conds.append("state='pending'")
            if a.bus:
                conds.append("bus=?")
                args.append(a.bus)
            if conds:
                q += " WHERE " + " AND ".join(conds)
            rows = c.execute(q + " ORDER BY created", args).fetchall()
            if not rows:
                print("(none)")
                return
            for r in rows:
                card = json.loads(r["req_card"] or "{}")
                clu = card.get("cluster")
                clu = clu.get("name") if isinstance(clu, dict) else (clu or "-")
                print(f"{r['state']:8} {r['bus']}/{r['agent_id']:16} "
                      f"ip={r['source_ip'] or '?':<15} nick={card.get('nickname', '-')} "
                      f"cluster={clu} jobs={','.join(card.get('job_types') or []) or '-'}")
            return
        if a.acmd == "approve":
            ok = adm_approve(c, a.bus, a.agent_id, "cli")
            print(f"approved {a.bus}/{a.agent_id}" if ok
                  else "no pending request for that agent")
            return
        if a.acmd in ("deny", "revoke"):
            new, target = (("denied", "pending") if a.acmd == "deny"
                           else ("revoked", "active"))
            ok = adm_decide(c, a.bus, a.agent_id, new, target, "cli", a.message)
            print(f"{new} {a.bus}/{a.agent_id}" if ok else "no matching credential")
            return
        if a.acmd == "joincode":
            adm_joincode(c, a.bus, a.code, "cli")
            print(f"join code updated for {a.bus}")
            return


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "admin":
        admin_cli(sys.argv[2:])
    else:
        import uvicorn
        uvicorn.run(app,
                    host=os.environ.get("A2A_HOST", "127.0.0.1"),
                    port=int(os.environ.get("A2A_PORT", "8080")),
                    log_level="warning")
