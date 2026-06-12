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

Run:  A2A_BUS_TOKENS=research-x:$(openssl rand -hex 24) python3 relay.py
API:  see DESIGN-V2.md §3. All routes under /v1/b/{bus} require
      `Authorization: Bearer <token>` and `X-A2A-Agent: <agent_id>`.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse

DB_PATH = os.environ.get("A2A_DB", "./relay.db")
RETENTION_DAYS = int(os.environ.get("A2A_RETENTION_DAYS", "30"))
MAX_WAIT = int(os.environ.get("A2A_MAX_WAIT", "300"))
BROADCAST = "_broadcast"
STALE_AFTER_S = 900

app = FastAPI(title="a2a-relay", docs_url=None, redoc_url=None)
_conds: dict[str, asyncio.Condition] = {}   # one Condition per bus


def bus_tokens() -> dict:
    raw = os.environ.get("A2A_BUS_TOKENS", "")
    pairs = [p for p in raw.split(",") if ":" in p]
    return dict(p.split(":", 1) for p in pairs)


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
        """)


def cond_for(bus: str) -> asyncio.Condition:
    if bus not in _conds:
        _conds[bus] = asyncio.Condition()
    return _conds[bus]


def auth(bus: str, request: Request) -> str:
    """Validate bearer token for the bus; return caller's agent id."""
    tok = bus_tokens().get(bus)
    if tok is None:
        raise HTTPException(404, f"unknown bus {bus!r}")
    header = request.headers.get("authorization", "")
    if header != f"Bearer {tok}":
        raise HTTPException(401, "bad or missing bearer token")
    agent = request.headers.get("x-a2a-agent", "")
    if not agent:
        raise HTTPException(400, "missing X-A2A-Agent header")
    return agent


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
    return {"ok": True, "schema": "a2a-code/v0", "buses": len(bus_tokens())}


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
    out = []
    with db() as c:
        for r in c.execute("SELECT card, updated FROM agents WHERE bus=? "
                           "ORDER BY agent_id", (bus,)):
            card = json.loads(r["card"])
            age = int(time.time() - r["updated"])
            card["_heartbeat_age_s"] = age
            card["_stale"] = age > STALE_AFTER_S
            out.append(card)
    return out


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


# ---------------------------------------------------------------- lifecycle

async def retention_loop() -> None:
    while True:
        cutoff = time.time() - RETENTION_DAYS * 86400
        with db() as c:
            c.execute("DELETE FROM messages WHERE ts < ?", (cutoff,))
        await asyncio.sleep(6 * 3600)


@app.on_event("startup")
async def startup() -> None:
    if not bus_tokens():
        raise RuntimeError("A2A_BUS_TOKENS is required (e.g. 'mybus:secret')")
    init_db()
    asyncio.get_event_loop().create_task(retention_loop())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app,
                host=os.environ.get("A2A_HOST", "127.0.0.1"),
                port=int(os.environ.get("A2A_PORT", "8080")),
                log_level="warning")
