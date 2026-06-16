#!/usr/bin/env python3
"""Phase-3 tests: the post-cutover state.

With the legacy shared token disabled, only per-agent tokens are accepted; and a
bus defined purely in bus_settings (no A2A_BUS_TOKENS entry) is fully usable, so
the shared token can be dropped entirely. Run:

    pip install fastapi httpx
    python3 tests/test_cutover.py
"""
import os
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
os.environ["A2A_DB"] = tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
os.environ["A2A_BUS_TOKENS"] = "research-x:legacy"   # registry + the now-rejected token
os.environ["A2A_ALLOW_LEGACY_TOKEN"] = "0"           # cutover: legacy off
os.environ["A2A_DEFAULT_JOIN_CODE"] = "142857"

sys.path.insert(0, str(REPO / "server"))
import relay  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), name)


def hdr(token, agent):
    return {"Authorization": f"Bearer {token}", "X-A2A-Agent": agent}


with TestClient(relay.app) as c:
    # --- legacy shared token is no longer accepted ---
    check("legacy shared token rejected (401)",
          c.get("/v1/b/research-x/agents",
                headers=hdr("legacy", "babel-eval")).status_code == 401)

    # --- the migration result: a per-agent token works ---
    tok = c.post("/v1/b/research-x/join",
                 json={"agent_id": "babel-eval", "join_code": "142857",
                       "card": {"nickname": "babel"}}).json()["token"]
    relay.admin_cli(["approve", "research-x", "babel-eval"])
    check("per-agent token works after approval",
          c.get("/v1/b/research-x/agents", headers=hdr(tok, "babel-eval")).status_code == 200)

    # --- a bus defined ONLY via bus_settings (not in A2A_BUS_TOKENS) is usable ---
    check("newbus unknown before creation (404)",
          c.post("/v1/b/newbus/join",
                 json={"agent_id": "edge-a", "join_code": "777", "card": {}}).status_code == 404)
    relay.admin_cli(["joincode", "newbus", "777"])      # creates the bus in bus_settings
    check("newbus not in A2A_BUS_TOKENS", "newbus" not in relay.bus_tokens())
    check("newbus is a known bus", relay.bus_known("newbus"))
    r = c.post("/v1/b/newbus/join",
               json={"agent_id": "edge-a", "join_code": "777",
                     "card": {"nickname": "edge"}})
    check("join on bus-settings-only bus -> 200", r.status_code == 200)
    tok2 = r.json()["token"]
    relay.admin_cli(["approve", "newbus", "edge-a"])
    check("per-agent token works on the new bus",
          c.get("/v1/b/newbus/agents", headers=hdr(tok2, "edge-a")).status_code == 200)

    # --- bus registry reflects both, with no shared-token role ---
    check("known_buses = {research-x, newbus}",
          relay.known_buses() == {"research-x", "newbus"})
    check("/health counts both buses",
          c.get("/health").json().get("buses") == 2)
    check("unknown bus still 404",
          c.post("/v1/b/ghost/join",
                 json={"agent_id": "x", "join_code": "1", "card": {}}).status_code == 404)

fails = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(fails)}/{len(RESULTS)} passed")
if fails:
    print("FAILED:", ", ".join(fails))
sys.exit(1 if fails else 0)
