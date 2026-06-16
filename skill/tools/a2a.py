#!/usr/bin/env python3
"""
a2a.py — single-file client for the A2A-Code git bus.

Lives inside the bus repo (tools/a2a.py): cloning the bus is the install, and
`git pull` keeps every agent on the same version. Messaging needs only the
Python stdlib + the `git` binary. *Receiving* files needs only stdlib
(presigned HTTPS GET/DELETE). *Sending* files lazily imports boto3 (s3) or
huggingface_hub (hf); the `local` backend (shared filesystem) is stdlib.

Usage (run from anywhere inside a bus clone):
  python tools/a2a.py init --agent-id babel-a [--storage s3 --bucket B ...]
  python tools/a2a.py init --agent-id babel-a --relay-url https://bus.example.com \
                           --bus research-x          # v2 relay transport (no git)
  python tools/a2a.py card update [--status busy] [--nickname N] [--new-session]
  python tools/a2a.py peers [--json]
  python tools/a2a.py send <id|nickname|_broadcast> <text> [--type T] [--thread X] [--reply-to ID]
  python tools/a2a.py inbox [--all] [--ack] [--thread X] [--json]
  python tools/a2a.py wait [--from ID] [--type T] [--thread X] [--timeout 600] [--interval 20]
  python tools/a2a.py file send <to> <path> [--note "..."] [--thread X]
  python tools/a2a.py file get <msg-id> [-o DIR]
  python tools/a2a.py check [--max-age 60] [--json]   # proactive unread probe
  python tools/a2a.py watch [--interval 30]           # live observer (no ack)
  python tools/a2a.py doctor

Exit codes: 0 ok · 1 error · 2 bad usage/validation · 124 wait timed out.
Schema: a2a-code/v0. Full design: DESIGN.md at the repo root.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import shutil
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = "a2a-code/v0"
MSG_TYPES = {"text", "status_update", "task_request", "task_result", "file_offer", "ack"}
AGENT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,31}$")
HEARTBEAT_REFRESH_S = 300          # rewrite own card if older than this
STALE_AFTER_S = 900                # peers shown as stale beyond this
PUSH_RETRIES = 5
PRESIGN_EXPIRY_S = 7 * 24 * 3600   # 7 days
BROADCAST = "_broadcast"
CRED_FILE = Path.home() / ".a2a" / "credentials"


# ---------------------------------------------------------------- utilities

def die(msg: str, code: int = 1) -> "NoReturn":
    print(f"a2a: error: {msg}", file=sys.stderr)
    sys.exit(code)


def now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def fname_ts(dt: datetime) -> str:
    """Filesystem-safe, lexicographically sortable timestamp."""
    return dt.strftime("%Y%m%dT%H%M%S%fZ")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def dump_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------- repo & config

def repo_root() -> Path:
    """Project root: this file lives at <root>/tools/a2a.py. A .git directory
    is only required for the git transport (v1); relay mode works without."""
    return Path(__file__).resolve().parents[1]


def conf_dir(root: Path) -> Path:
    """Where this agent's local state (config, sync stamp) lives.

    - $A2A_HOME/.a2a when set (explicit override);
    - ~/.a2a when the tool runs from an installed skill
      (~/.claude/skills/a2a-code/tools/a2a.py) — skill folders are replaceable
      caches and must not hold identity;
    - <clone>/.a2a otherwise (v1 behavior; one identity per clone).
    """
    env = os.environ.get("A2A_HOME")
    if env:
        return Path(env).expanduser() / ".a2a"
    parts = root.parts
    if ".claude" in parts and "skills" in parts:
        return Path.home() / ".a2a"
    return root / ".a2a"


def cfg_path(root: Path) -> Path:
    return conf_dir(root) / "config.json"


def load_cfg(root: Path) -> dict:
    p = cfg_path(root)
    if not p.exists():
        die("not initialized — run: python tools/a2a.py init --agent-id <id>", 2)
    return load_json(p)


def save_cfg(root: Path, cfg: dict) -> None:
    dump_json(cfg_path(root), cfg)


def me(cfg: dict) -> str:
    return cfg["agent_id"]


def is_relay(cfg: dict) -> bool:
    return cfg.get("transport") == "relay"


# ---------------------------------------------------------------- git transport

def git(root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    p = subprocess.run(["git", "-C", str(root), *args],
                       capture_output=True, text=True)
    if check and p.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed:\n{p.stderr.strip()}")
    return p


def git_sync(root: Path) -> None:
    """Pull latest bus state. Content conflicts are impossible by layout."""
    p = git(root, "pull", "--rebase", "--quiet", check=False)
    if p.returncode != 0:
        # a stuck rebase would poison every later op — abort it loudly
        git(root, "rebase", "--abort", check=False)
        raise RuntimeError(f"git pull --rebase failed:\n{p.stderr.strip()}")
    try:  # let `check --max-age` skip redundant pulls (.a2a/ is gitignored)
        dump_json(conf_dir(root) / "state.json", {"last_sync": time.time()})
    except Exception:
        pass


def commit_push(root: Path, cfg: dict, paths: list, message: str) -> bool:
    """add → commit → (pull --rebase → push) with jittered retries.

    Returns False when there was nothing to commit.
    """
    git(root, "add", "--", *[str(p) for p in paths])
    if git(root, "diff", "--cached", "--quiet", check=False).returncode == 0:
        return False
    aid = cfg.get("agent_id", "unknown")
    git(root, "-c", f"user.name=a2a {aid}", "-c", f"user.email={aid}@a2a.invalid",
        "commit", "--quiet", "-m", message)
    last = ""
    for attempt in range(PUSH_RETRIES):
        try:
            git_sync(root)
            git(root, "push", "--quiet", "origin", "HEAD")
            return True
        except RuntimeError as e:  # ref race or transient network
            last = str(e)
            time.sleep(0.5 * (2 ** attempt) + random.uniform(0, 0.5))
    raise RuntimeError(
        f"push failed after {PUSH_RETRIES} attempts (committed locally; "
        f"rerun any a2a command to retry). Last error:\n{last}")


# ---------------------------------------------------------------- agent cards

def card_path(root: Path, aid: str) -> Path:
    return root / "agents" / f"{aid}.json"


def write_own_card(root: Path, cfg: dict, new_session: bool = False, **updates) -> Path:
    p = card_path(root, me(cfg))
    card = load_json(p) if p.exists() else {
        "agent_id": me(cfg),
        "agent_kind": cfg.get("agent_kind", "unknown"),
        "cluster": {"name": cfg.get("cluster", "unknown"),
                    "scheduler": cfg.get("scheduler", "unknown")},
        "capabilities": {},
        "job_types": [],
        "status": "idle",
        "status_detail": "",
        "schema": SCHEMA,
    }
    if new_session or "session_id" not in card:
        card["session_id"] = uuid.uuid4().hex[:8]
        card["session_started"] = iso(now())
    for k, v in updates.items():
        if v is not None:
            card[k] = v
    card["last_heartbeat"] = iso(now())
    dump_json(p, card)
    return p


def own_session(root: Path, cfg: dict) -> str:
    """The current incarnation of this agent (rotated by --new-session)."""
    if is_relay(cfg):
        return cfg.get("session_id", "")
    p = card_path(root, me(cfg))
    if p.exists():
        try:
            return load_json(p).get("session_id", "")
        except Exception:
            pass
    return ""


def heartbeat_paths(root: Path, cfg: dict) -> list:
    """Refresh own card if stale; return paths to fold into the next commit."""
    p = card_path(root, me(cfg))
    if p.exists():
        try:
            age = (now() - parse_iso(load_json(p)["last_heartbeat"])).total_seconds()
            if age < HEARTBEAT_REFRESH_S:
                return []
        except Exception:
            pass
    return [write_own_card(root, cfg)]


def read_cards(root: Path) -> list:
    cards = []
    agents_dir = root / "agents"
    if not agents_dir.exists():
        return cards
    for f in sorted(agents_dir.glob("*.json")):
        try:
            c = load_json(f)
            age = (now() - parse_iso(c.get("last_heartbeat", "1970-01-01T00:00:00Z"))).total_seconds()
            c["_stale"] = age > STALE_AFTER_S
            c["_heartbeat_age_s"] = int(age)
            cards.append(c)
        except Exception:
            cards.append({"agent_id": f.stem, "_corrupt": True, "_stale": True})
    return cards


# ---------------------------------------------------------------- messages & cursor

def msg_dirs(root: Path, aid: str) -> list:
    return [root / "messages" / aid, root / "messages" / BROADCAST]


def cursor_path(root: Path, aid: str) -> Path:
    return root / "cursors" / f"{aid}.json"


def read_cursor(root: Path, aid: str) -> set:
    p = cursor_path(root, aid)
    if not p.exists():
        return set()
    try:
        return set(load_json(p).get("read", []))
    except Exception:
        return set()


def write_cursor(root: Path, aid: str, read: set) -> Path:
    p = cursor_path(root, aid)
    dump_json(p, {"agent_id": aid, "read": sorted(read), "schema": SCHEMA})
    return p


def list_messages(root: Path, aid: str) -> list:
    """All messages addressed to `aid` (incl. broadcast), sorted by filename.

    Returns (relpath, msg_or_None) — None marks corrupt JSON (reported, skipped).
    """
    out = []
    for d in msg_dirs(root, aid):
        if not d.exists():
            continue
        for f in sorted(d.glob("*.json")):
            rel = f"{d.name}/{f.name}"
            try:
                out.append((rel, load_json(f)))
            except Exception:
                out.append((rel, None))
    out.sort(key=lambda t: t[0].split("/", 1)[1])  # ts-prefixed filename = chronological
    return out


def unread_messages(root: Path, cfg: dict) -> list:
    aid = me(cfg)
    read = read_cursor(root, aid)
    items = []
    for rel, msg in list_messages(root, aid):
        if rel in read:
            continue
        if msg is not None and msg.get("from") == aid:
            continue  # own broadcasts
        items.append((rel, msg))
    return items


def build_msg(frm: str, to: str, mtype: str, body, reply_to=None,
              session=None, thread=None) -> dict:
    return {"id": uuid.uuid4().hex, "from": frm, "to": to, "type": mtype,
            "body": body, "reply_to": reply_to, "session": session,
            "thread": thread, "ts": iso(now()), "schema": SCHEMA}


def write_msg_file(root: Path, msg: dict) -> Path:
    d = root / "messages" / msg["to"]
    f = d / f"{fname_ts(now())}__{msg['from']}__{msg['id'][:8]}.json"
    dump_json(f, msg)
    return f


def deliver(root: Path, cfg: dict, msg: dict, extra_paths=(), note: str = "") -> dict:
    """Send a message via the configured transport (git commit+push or relay
    POST), with the git heartbeat piggyback where applicable."""
    if is_relay(cfg):
        relay_client(cfg).post_msg(msg)
        return msg
    paths = [write_msg_file(root, msg)] + list(extra_paths) + heartbeat_paths(root, cfg)
    commit_push(root, cfg, paths, f"a2a({me(cfg)}): {msg['type']} -> {msg['to']}{note}")
    return msg


def get_cards(root: Path, cfg: dict) -> list:
    """Agent cards via the configured transport."""
    return relay_client(cfg).agents() if is_relay(cfg) else read_cards(root)


def resolve_agent(cards: list, name: str, strict: bool = True) -> str:
    """Resolve an agent_id *or user-facing nickname* to an agent_id.

    Users name clusters their own way; agents learn those names and put them
    on their card (`card update --nickname tower`). Nicknames are aliases,
    not identity: inbox paths and cursors stay keyed by agent_id, so renaming
    is free. Exact agent_id wins; nickname match is case-insensitive and must
    be unique.
    """
    if name == BROADCAST:
        return name
    cards = [c for c in cards if not c.get("_corrupt")]
    if any(c["agent_id"] == name for c in cards):
        return name
    hits = [c for c in cards if c.get("nickname", "").lower() == name.lower()]
    if len(hits) == 1:
        return hits[0]["agent_id"]
    if len(hits) > 1:
        die(f"nickname {name!r} is ambiguous: "
            f"{sorted(c['agent_id'] for c in hits)} — use the agent id", 2)
    if strict:
        roster = ", ".join(
            c["agent_id"] + (f" ({c['nickname']})" if c.get("nickname") else "")
            for c in cards) or "(none)"
        die(f"unknown agent {name!r} — known: {roster} (or {BROADCAST})", 2)
    return name


def find_message(root: Path, cfg: dict, msg_id: str):
    """Locate a message in my inbox dirs by full id or unique >=8-char prefix."""
    if len(msg_id) < 8:
        die("message id must be at least 8 characters", 2)
    hits = [(rel, m) for rel, m in list_messages(root, me(cfg))
            if m is not None and m["id"].startswith(msg_id)]
    if not hits:
        die(f"no message with id {msg_id} in your inbox (try: inbox --all)", 2)
    if len(hits) > 1:
        die(f"id prefix {msg_id} is ambiguous ({len(hits)} matches)", 2)
    return hits[0]


# ---------------------------------------------------------------- storage backends

def _load_cred_file() -> dict:
    if not CRED_FILE.exists():
        return {}
    mode = stat.S_IMODE(CRED_FILE.stat().st_mode)
    if mode & 0o077:
        print(f"a2a: warning: {CRED_FILE} permissions are {oct(mode)}; "
              f"run: chmod 600 {CRED_FILE}", file=sys.stderr)
    try:
        return load_json(CRED_FILE)
    except Exception:
        return {}


def _cred(name: str):
    """env var → ~/.a2a/credentials (0600). Never read from the bus repo."""
    return os.environ.get(name) or _load_cred_file().get(name.lower()) \
        or _load_cred_file().get(name)


class LocalStorage:
    """Shared-filesystem backend (two clusters mounting the same NFS), and the
    storage used by the e2e tests. Pure stdlib."""

    def __init__(self, scfg: dict):
        base = scfg.get("path")
        if not base:
            die("storage.path missing in .a2a/config.json for local backend", 2)
        self.base = Path(base)

    def upload(self, local: Path, key: str) -> dict:
        dest = self.base / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local, dest)
        return {"uri": f"local://{key}"}

    def download(self, body: dict, dest: Path) -> None:
        shutil.copyfile(self.base / body["uri"][len("local://"):], dest)

    def delete(self, body: dict) -> None:
        p = self.base / body["uri"][len("local://"):]
        p.unlink(missing_ok=True)
        for parent in p.parents:                       # prune empty dirs
            if parent == self.base or any(parent.iterdir()):
                break
            parent.rmdir()


class S3Storage:
    """Any S3-compatible endpoint (AWS, Cloudflare R2, MinIO, GCS interop).
    boto3 is imported lazily — only file *senders* need it; receivers use the
    presigned URLs with stdlib HTTP."""

    def __init__(self, scfg: dict):
        try:
            import boto3  # lazy
        except ImportError:
            die("boto3 is required to send files via s3: pip install boto3", 2)
        self.bucket = scfg.get("bucket") or die("storage.bucket missing in config", 2)
        kw = {}
        if scfg.get("endpoint_url"):
            kw["endpoint_url"] = scfg["endpoint_url"]
        if scfg.get("region"):
            kw["region_name"] = scfg["region"]
        ak, sk = _cred("AWS_ACCESS_KEY_ID"), _cred("AWS_SECRET_ACCESS_KEY")
        if ak and sk:
            kw.update(aws_access_key_id=ak, aws_secret_access_key=sk)
        self.client = boto3.client("s3", **kw)

    def upload(self, local: Path, key: str) -> dict:
        self.client.upload_file(str(local), self.bucket, key)  # multipart-aware
        params = {"Bucket": self.bucket, "Key": key}
        return {
            "uri": f"s3://{self.bucket}/{key}",
            "presigned_get_url": self.client.generate_presigned_url(
                "get_object", Params=params, ExpiresIn=PRESIGN_EXPIRY_S),
            "presigned_delete_url": self.client.generate_presigned_url(
                "delete_object", Params=params, ExpiresIn=PRESIGN_EXPIRY_S),
            "expires_at": iso(datetime.fromtimestamp(
                time.time() + PRESIGN_EXPIRY_S, tz=timezone.utc)),
        }

    def download(self, body: dict, dest: Path) -> None:
        _, _, rest = body["uri"].partition("s3://")
        bucket, _, key = rest.partition("/")
        self.client.download_file(bucket, key, str(dest))

    def delete(self, body: dict) -> None:
        _, _, rest = body["uri"].partition("s3://")
        bucket, _, key = rest.partition("/")
        self.client.delete_object(Bucket=bucket, Key=key)


class HfStorage:
    """Fallback: private Hugging Face *dataset repo*. Zero cost, one token,
    but both sides need HF_TOKEN and there are no presigned URLs."""

    def __init__(self, scfg: dict):
        try:
            from huggingface_hub import HfApi  # lazy
        except ImportError:
            die("huggingface_hub is required for hf storage: pip install huggingface_hub", 2)
        self.repo = scfg.get("repo_id") or die("storage.repo_id missing in config", 2)
        self.api = HfApi(token=_cred("HF_TOKEN"))

    def upload(self, local: Path, key: str) -> dict:
        self.api.upload_file(path_or_fileobj=str(local), path_in_repo=key,
                             repo_id=self.repo, repo_type="dataset")
        return {"uri": f"hf://datasets/{self.repo}/{key}"}

    def download(self, body: dict, dest: Path) -> None:
        from huggingface_hub import hf_hub_download
        key = body["uri"].split(f"hf://datasets/{self.repo}/", 1)[1]
        got = hf_hub_download(repo_id=self.repo, filename=key, repo_type="dataset",
                              token=_cred("HF_TOKEN"))
        shutil.copyfile(got, dest)

    def delete(self, body: dict) -> None:
        key = body["uri"].split(f"hf://datasets/{self.repo}/", 1)[1]
        self.api.delete_file(path_in_repo=key, repo_id=self.repo, repo_type="dataset")


BACKENDS = {"local": LocalStorage, "s3": S3Storage, "hf": HfStorage}


def storage_from_cfg(cfg: dict):
    scfg = cfg.get("storage") or die(
        "no storage configured — set the storage section in .a2a/config.json "
        "(see README) or re-run init with --storage", 2)
    backend = scfg.get("backend")
    if backend not in BACKENDS:
        die(f"unknown storage backend {backend!r} (choose from {sorted(BACKENDS)})", 2)
    return BACKENDS[backend](scfg)


def storage_for_uri(uri: str, cfg: dict):
    scheme = uri.split("://", 1)[0]
    table = {"local": "local", "s3": "s3", "hf": "hf"}
    if scheme not in table:
        die(f"unsupported file uri scheme: {uri}", 2)
    scfg = (cfg.get("storage") or {})
    if scfg.get("backend") != table[scheme]:
        die(f"offer uses {scheme}:// but your configured backend is "
            f"{scfg.get('backend')!r} — configure matching storage to fetch it", 2)
    return BACKENDS[table[scheme]](scfg)


# ---------------------------------------------------------------- relay transport (v2)

RELAY_HOLD = 300  # long-poll hold; one held request instead of poll cycles


class RelayClient:
    """Keep-alive stdlib HTTP client for the v2 relay.

    One TLS handshake per *process*, not per request (DESIGN-V2 §9 egress
    rule): the connection is reused across re-issued long-polls inside a
    `wait`/`watch`, and rebuilt transparently if it drops.
    """

    def __init__(self, cfg: dict):
        from urllib.parse import urlsplit
        u = urlsplit(cfg["relay_url"])
        if u.scheme not in ("http", "https") or not u.hostname:
            die(f"bad relay_url {cfg['relay_url']!r}", 2)
        self.https = u.scheme == "https"
        self.host = u.hostname
        self.port = u.port or (443 if self.https else 80)
        self.base = f"/v1/b/{cfg['bus']}"
        token = _cred("A2A_TOKEN")
        if not token:
            die("no relay token — set A2A_TOKEN or add a2a_token to "
                f"{CRED_FILE}", 2)
        self.headers = {"Authorization": f"Bearer {token}",
                        "X-A2A-Agent": cfg["agent_id"],
                        "Content-Type": "application/json"}
        self.conn = None

    def _connect(self):
        import http.client
        cls = (http.client.HTTPSConnection if self.https
               else http.client.HTTPConnection)
        return cls(self.host, self.port, timeout=RELAY_HOLD + 30)

    def call(self, method: str, path: str, body=None, _retry=True):
        payload = json.dumps(body) if body is not None else None
        try:
            if self.conn is None:
                self.conn = self._connect()
            self.conn.request(method, path, payload, self.headers)
            r = self.conn.getresponse()
            data = r.read()
        except Exception as e:
            self.conn = None
            if _retry:
                return self.call(method, path, body, _retry=False)
            raise RuntimeError(f"relay unreachable ({self.host}:{self.port}): {e}")
        if r.status >= 400:
            raise RuntimeError(
                f"relay {method} {path} -> {r.status}: {data.decode()[:200]}")
        return json.loads(data) if data else None

    # bus-scoped helpers
    def post_card(self, card):
        return self.call("POST", f"{self.base}/agents", card)

    def agents(self):
        return self.call("GET", f"{self.base}/agents")

    def post_msg(self, msg):
        return self.call("POST", f"{self.base}/messages", msg)

    def fetch(self, wait=0, include_read=False, frm="", mtype="", thread="",
              cursor=0):
        from urllib.parse import urlencode
        q = urlencode({"wait": wait, "include_read": int(include_read),
                       "from": frm, "type": mtype, "thread": thread or "",
                       "cursor": cursor})
        return self.call("GET", f"{self.base}/messages?{q}")

    def ack(self, seqs):
        return self.call("POST", f"{self.base}/ack", {"seqs": list(seqs)})

    def health(self):
        return self.call("GET", "/health")

    def admission(self):
        return self.call("GET", f"{self.base}/admission")


_RC = None


def relay_client(cfg: dict) -> RelayClient:
    global _RC
    if _RC is None:
        _RC = RelayClient(cfg)
    return _RC


def _store_token(token: str) -> None:
    """Persist the per-agent bus token to ~/.a2a/credentials (0600)."""
    CRED_FILE.parent.mkdir(parents=True, exist_ok=True)
    creds = _load_cred_file()
    creds["a2a_token"] = token
    dump_json(CRED_FILE, creds)
    try:
        CRED_FILE.chmod(0o600)
    except OSError:
        pass


def relay_join(cfg: dict, join_code: str, card: dict) -> dict:
    """Tokenless POST /join — first contact, gated by the bus join code."""
    import http.client
    from urllib.parse import urlsplit
    u = urlsplit(cfg["relay_url"])
    https = u.scheme == "https"
    conn = (http.client.HTTPSConnection if https else http.client.HTTPConnection)(
        u.hostname, u.port or (443 if https else 80), timeout=30)
    body = json.dumps({"agent_id": cfg["agent_id"], "join_code": join_code,
                       "card": card})
    try:
        conn.request("POST", f"/v1/b/{cfg['bus']}/join", body,
                     {"Content-Type": "application/json"})
        r = conn.getresponse()
        data = r.read()
    except Exception as e:
        die(f"relay unreachable ({u.hostname}): {e}", 1)
    if r.status >= 400:
        die(f"join refused -> {r.status}: {data.decode()[:200]}", 1)
    return json.loads(data)


def _poll_admission(rc: RelayClient, tries: int = 6, delay: int = 5):
    """Return (state, message); polls a few times so a quick approval lands live."""
    for i in range(tries):
        r = rc.admission()
        state = r.get("state")
        if state != "pending":
            return state, r.get("message")
        if i < tries - 1:
            time.sleep(delay)
    return "pending", None


def relay_find(rc: RelayClient, msg_id: str):
    """(seq, msg) for an id or unique >=8-char prefix among my messages."""
    if len(msg_id) < 8:
        die("message id must be at least 8 characters", 2)
    hits = [(m["seq"], m["msg"]) for m in rc.fetch(include_read=True)["messages"]
            if m["msg"]["id"].startswith(msg_id)]
    if not hits:
        die(f"no message with id {msg_id} in your inbox", 2)
    if len(hits) > 1:
        die(f"id prefix {msg_id} is ambiguous ({len(hits)} matches)", 2)
    return hits[0]


# ---------------------------------------------------------------- file transfer

def _http(url: str, method: str = "GET"):
    req = urllib.request.Request(url, method=method)
    return urllib.request.urlopen(req, timeout=120)


def file_send(root: Path, cfg: dict, to: str, path: Path, note: str,
              thread=None) -> dict:
    if not path.is_file():
        die(f"not a file: {path}", 2)
    store = storage_from_cfg(cfg)
    key = f"{cfg.get('storage', {}).get('prefix', 'a2a/')}{me(cfg)}/{uuid.uuid4().hex[:8]}/{path.name}"
    ref = store.upload(path, key)
    body = {**ref, "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path), "note": note or ""}
    return deliver(root, cfg, build_msg(me(cfg), to, "file_offer", body,
                                        session=own_session(root, cfg),
                                        thread=thread),
                   note=f" ({path.name})")


def file_get(root: Path, cfg: dict, msg_id: str, outdir: Path) -> Path:
    if is_relay(cfg):
        ref, msg = relay_find(relay_client(cfg), msg_id)
    else:
        ref, msg = find_message(root, cfg, msg_id)
    if msg["type"] != "file_offer":
        die(f"message {msg_id} is a {msg['type']}, not a file_offer", 2)
    body = msg["body"]
    outdir.mkdir(parents=True, exist_ok=True)
    dest = outdir / Path(body["uri"]).name

    # 1. download — presigned HTTPS (stdlib, no credentials) when offered
    if body.get("presigned_get_url"):
        with _http(body["presigned_get_url"]) as r, open(dest, "wb") as f:
            shutil.copyfileobj(r, f, 1 << 20)
    else:
        storage_for_uri(body["uri"], cfg).download(body, dest)

    # 2. verify
    got = sha256_file(dest)
    if got != body["sha256"]:
        bad = dest.with_suffix(dest.suffix + ".corrupt")
        dest.rename(bad)
        die(f"sha256 mismatch (expected {body['sha256'][:12]}…, got {got[:12]}…) "
            f"— kept as {bad}; NOT deleted from storage, NOT acked", 1)

    # 3. delete from cloud (cleanup-on-ack, decided in DESIGN.md §6)
    deleted = False
    try:
        if body.get("presigned_delete_url"):
            with _http(body["presigned_delete_url"], method="DELETE") as r:
                deleted = r.status in (200, 204)
        else:
            storage_for_uri(body["uri"], cfg).delete(body)
            deleted = True
    except Exception as e:
        print(f"a2a: warning: cloud delete failed ({e}); sender will clean up "
              f"on ack", file=sys.stderr)

    # 4. ack (and mark the offer read) — one commit on git, two calls on relay
    ack = build_msg(me(cfg), msg["from"], "ack",
                    {"sha256_ok": True, "deleted": deleted, "file": dest.name},
                    reply_to=msg["id"], session=own_session(root, cfg),
                    thread=msg.get("thread"))
    if is_relay(cfg):
        deliver(root, cfg, ack)
        relay_client(cfg).ack([ref])
    else:
        read = read_cursor(root, me(cfg))
        read.add(ref)
        deliver(root, cfg, ack, extra_paths=[write_cursor(root, me(cfg), read)])
    return dest


# ---------------------------------------------------------------- output helpers

def show_items(items: list, as_json: bool) -> None:
    """items: list of (relpath, msg_or_None[, read_bool])."""
    if as_json:
        out = []
        for it in items:
            rel, msg = it[0], it[1]
            read = it[2] if len(it) > 2 else False
            out.append({"relpath": rel, "read": read, "msg": msg})
        print(json.dumps(out, indent=2, sort_keys=True))
        return
    if not items:
        print("(no messages)")
        return
    for it in items:
        rel, msg = it[0], it[1]
        read = it[2] if len(it) > 2 else False
        flag = "read  " if read else "unread"
        if msg is None:
            print(f"[{flag}] {rel}  !! corrupt JSON — skipped")
            continue
        head = (f"[{flag}] {msg['ts']}  {msg['from']} -> {msg['to']}  "
                f"{msg['type']}  id={msg['id']}")
        if msg.get("reply_to"):
            head += f"  reply_to={msg['reply_to'][:8]}"
        if msg.get("thread"):
            head += f"  thread={msg['thread']}"
        if msg.get("session"):
            head += f"  sess={msg['session']}"
        print(head)
        body = msg["body"]
        body_s = body if isinstance(body, str) else json.dumps(body, indent=2, sort_keys=True)
        for line in body_s.splitlines():
            print(f"    {line}")


def ack_items(root: Path, cfg: dict, rels: list) -> None:
    if not rels:
        return
    read = read_cursor(root, me(cfg))
    read.update(rels)
    paths = [write_cursor(root, me(cfg), read)] + heartbeat_paths(root, cfg)
    commit_push(root, cfg, paths, f"a2a({me(cfg)}): ack {len(rels)} message(s)")


# ---------------------------------------------------------------- commands

def _storage_cfg(args):
    scfg = {"backend": args.storage, "prefix": "a2a/"}
    for k, v in (("bucket", args.bucket), ("endpoint_url", args.endpoint_url),
                 ("region", args.region), ("path", args.local_path),
                 ("repo_id", args.hf_repo)):
        if v:
            scfg[k] = v
    return scfg


def cmd_init(root: Path, args) -> None:
    if not AGENT_ID_RE.match(args.agent_id):
        die(f"agent id must match {AGENT_ID_RE.pattern}", 2)

    if args.relay_url:  # ---------------- v2 relay mode (no git involved)
        if not args.bus:
            die("--bus is required with --relay-url", 2)
        cfg = {"agent_id": args.agent_id, "agent_kind": args.agent_kind,
               "cluster": args.cluster, "scheduler": args.scheduler,
               "transport": "relay", "relay_url": args.relay_url,
               "bus": args.bus, "session_id": uuid.uuid4().hex[:8],
               "schema": SCHEMA}
        if args.storage:
            cfg["storage"] = _storage_cfg(args)
        save_cfg(root, cfg)
        # The card we want published (sent at join; the relay publishes it on
        # approval). Identity is operator-approved now: first contact uses the
        # bus join code and waits for approval; restarts reuse the stored token.
        card = {"agent_id": args.agent_id, "agent_kind": args.agent_kind,
                "cluster": {"name": args.cluster, "scheduler": args.scheduler},
                "capabilities": {"gpus": args.gpus} if args.gpus else {},
                "job_types": args.job_types.split(",") if args.job_types else [],
                "status": args.status, "status_detail": "",
                "session_id": cfg["session_id"], "session_started": iso(now()),
                "last_heartbeat": iso(now()), "schema": SCHEMA}
        if args.nickname:
            card["nickname"] = args.nickname

        if args.join_code:           # explicit (re)join — also the migration path
            resp = relay_join(cfg, args.join_code, card)
            _store_token(resp["token"])
            print(f"join requested as {args.agent_id} on {args.bus} "
                  f"@ {args.relay_url} — awaiting operator approval")
        elif not _cred("A2A_TOKEN"):
            die("first contact needs --join-code (ask your operator for the "
                "bus join code); you then await approval", 2)

        rc = relay_client(cfg)
        state, msg = _poll_admission(rc)
        if state == "active":
            rc.post_card(card)
            print(f"approved — registered {args.agent_id} "
                  f"(session={cfg['session_id']})")
        elif state in ("denied", "revoked"):
            die(f"join {state}: {msg or 'declined by operator'}", 3)
        else:
            print("still pending operator approval — re-run `a2a.py doctor` "
                  "(or this init) once approved")
        return

    # -------------------------------------- v1 git mode
    # Local (per-clone) identity: required by `pull --rebase` when it replays
    # our commits. Scoped to this repo only — never touches global config.
    if not (root / ".git").exists():
        die(f"{root} is not a git repo — clone the bus repo, or use "
            f"--relay-url for the v2 relay", 2)
    git(root, "config", "user.name", f"a2a {args.agent_id}")
    git(root, "config", "user.email", f"{args.agent_id}@a2a.invalid")
    git_sync(root)
    paths = []

    gi = root / ".gitignore"
    lines = gi.read_text().splitlines() if gi.exists() else []
    for needed in (".a2a/", "__pycache__/"):
        if needed not in lines:
            lines.append(needed)
    gi.write_text("\n".join(lines) + "\n")
    paths.append(gi)

    for d in ("agents", "cursors", f"messages/{BROADCAST}"):
        keep = root / d / ".gitkeep"
        if not keep.exists():
            keep.parent.mkdir(parents=True, exist_ok=True)
            keep.write_text("")
            paths.append(keep)

    cfg = {"agent_id": args.agent_id, "agent_kind": args.agent_kind,
           "cluster": args.cluster, "scheduler": args.scheduler, "schema": SCHEMA}
    if args.storage:
        cfg["storage"] = _storage_cfg(args)
    save_cfg(root, cfg)  # local only — .a2a/ is gitignored

    # Inheritance: if this agent_id already has a card on the bus, adopt it —
    # the previous incarnation may have been killed. Card fields and the read
    # cursor survive; only the session rotates (peers see the restart).
    inherited = card_path(root, args.agent_id).exists()
    caps = {}
    if args.gpus:
        caps["gpus"] = args.gpus
    updates = dict(capabilities=caps or None,
                   job_types=args.job_types.split(",") if args.job_types else None,
                   status=args.status, nickname=args.nickname)
    if args.cluster != "unknown":
        updates["cluster"] = {"name": args.cluster, "scheduler": args.scheduler}
    paths.append(write_own_card(root, cfg, new_session=inherited, **updates))
    if not cursor_path(root, args.agent_id).exists():
        paths.append(write_cursor(root, args.agent_id, set()))

    commit_push(root, cfg, paths, f"a2a({args.agent_id}): register")
    verb = "inherited identity" if inherited else "registered agent"
    print(f"{verb} {args.agent_id} on bus {root.name}")


def cmd_card_update(root: Path, args) -> None:
    cfg = load_cfg(root)
    updates = {"status": args.status, "status_detail": args.detail,
               "nickname": args.nickname,
               "job_types": args.job_types.split(",") if args.job_types else None}
    if args.gpus:
        updates["capabilities"] = {"gpus": args.gpus}

    if is_relay(cfg):
        rc = relay_client(cfg)
        own = next((c for c in rc.agents() if c["agent_id"] == me(cfg)), None)
        if own is None:
            die("not registered on this bus — run init first", 2)
        card = {k: v for k, v in own.items() if not k.startswith("_")}
        if args.new_session:
            cfg["session_id"] = uuid.uuid4().hex[:8]
            save_cfg(root, cfg)
            card["session_id"] = cfg["session_id"]
            card["session_started"] = iso(now())
        for k, v in updates.items():
            if v is not None:
                card[k] = v
        card["last_heartbeat"] = iso(now())
        rc.post_card(card)
    else:
        p = write_own_card(root, cfg, new_session=args.new_session, **updates)
        commit_push(root, cfg, [p], f"a2a({me(cfg)}): card update")

    out = f"card updated: status={args.status or '(unchanged)'}"
    if args.nickname is not None:
        out += f" nickname={args.nickname!r}"
    if args.new_session:
        out += f" session={own_session(root, cfg)} (new)"
    print(out)


def cmd_peers(root: Path, args) -> None:
    cfg = load_cfg(root)
    if not is_relay(cfg):
        git_sync(root)
    cards = get_cards(root, cfg)
    if args.json:
        print(json.dumps(cards, indent=2, sort_keys=True))
        return
    if not cards:
        print("(no agents registered)")
        return
    for c in cards:
        if c.get("_corrupt"):
            print(f"{c['agent_id']:<20} !! corrupt card")
            continue
        mark = " (stale)" if c["_stale"] else ""
        label = c["agent_id"] + (f' "{c["nickname"]}"' if c.get("nickname") else "")
        print(f"{label:<28} {c.get('status', '?'):<8}{mark}  "
              f"sess={c.get('session_id', '?')}  "
              f"hb {c['_heartbeat_age_s']}s ago  "
              f"cluster={c.get('cluster', {}).get('name', '?')}  "
              f"jobs={','.join(c.get('job_types', [])) or '-'}  "
              f"{c.get('status_detail', '')}")


def cmd_send(root: Path, args) -> None:
    cfg = load_cfg(root)
    if args.type not in MSG_TYPES:
        die(f"type must be one of {sorted(MSG_TYPES)}", 2)
    if not is_relay(cfg):
        git_sync(root)
    to = resolve_agent(get_cards(root, cfg), args.to)
    body = args.text
    if args.spec:
        body = {"text": args.text, "spec": load_json(Path(args.spec))}
    thread = args.thread
    if args.reply_to and not thread:  # replies inherit the thread they answer
        if is_relay(cfg):
            hits = [m["msg"] for m in
                    relay_client(cfg).fetch(include_read=True)["messages"]
                    if m["msg"]["id"].startswith(args.reply_to)]
        else:
            hits = [m for _, m in list_messages(root, me(cfg))
                    if m is not None and m["id"].startswith(args.reply_to)]
        if len(hits) == 1:
            thread = hits[0].get("thread")
    msg = deliver(root, cfg, build_msg(me(cfg), to, args.type, body,
                                       reply_to=args.reply_to,
                                       session=own_session(root, cfg),
                                       thread=thread))
    out = f"sent {msg['type']} to {msg['to']} id={msg['id']}"
    if thread:
        out += f" thread={thread}"
    print(out)


def cmd_inbox(root: Path, args) -> None:
    cfg = load_cfg(root)
    if is_relay(cfg):
        rc = relay_client(cfg)
        res = rc.fetch(include_read=args.all, thread=args.thread or "")
        items = [(f"seq:{m['seq']}", m["msg"], bool(m.get("read")))
                 for m in res["messages"]]
        show_items(items, args.json)
        if args.ack:
            seqs = [m["seq"] for m in res["messages"] if not m.get("read")]
            if seqs:
                rc.ack(seqs)
        return
    git_sync(root)
    if args.all:
        read = read_cursor(root, me(cfg))
        items = [(rel, m, rel in read) for rel, m in list_messages(root, me(cfg))
                 if m is None or m.get("from") != me(cfg)]
    else:
        items = [(rel, m, False) for rel, m in unread_messages(root, cfg)]
    if args.thread:
        items = [it for it in items
                 if it[1] is not None and it[1].get("thread") == args.thread]
    show_items(items, args.json)
    if args.ack:
        ack_items(root, cfg, [it[0] for it in items if not it[2]])


def cmd_wait(root: Path, args) -> None:
    cfg = load_cfg(root)
    deadline = time.monotonic() + args.timeout

    if is_relay(cfg):  # push: one held request replaces poll cycles
        rc = relay_client(cfg)
        frm = (resolve_agent(rc.agents(), args.frm, strict=False)
               if args.frm else "")
        while True:
            hold = max(0, min(RELAY_HOLD, int(deadline - time.monotonic())))
            res = rc.fetch(wait=hold, frm=frm, mtype=args.type or "",
                           thread=args.thread or "")
            if res["messages"]:
                show_items([(f"seq:{m['seq']}", m["msg"])
                            for m in res["messages"]], args.json)
                if not args.no_ack:
                    rc.ack([m["seq"] for m in res["messages"]])
                return
            if time.monotonic() >= deadline:
                print(f"a2a: wait timed out after {args.timeout}s "
                      f"(from={args.frm or '*'} type={args.type or '*'})",
                      file=sys.stderr)
                sys.exit(124)

    while True:
        try:
            git_sync(root)
        except RuntimeError as e:
            print(f"a2a: warning: sync failed, retrying: {e}", file=sys.stderr)
        # nickname → id after each sync, leniently: the peer (or its nickname)
        # may only appear on the bus mid-wait
        frm = (resolve_agent(read_cards(root), args.frm, strict=False)
               if args.frm else None)
        matches = [(rel, m) for rel, m in unread_messages(root, cfg)
                   if m is not None
                   and (not frm or m["from"] == frm)
                   and (not args.type or m["type"] == args.type)
                   and (not args.thread or m.get("thread") == args.thread)]
        if matches:
            show_items(matches, args.json)
            if not args.no_ack:
                ack_items(root, cfg, [rel for rel, _ in matches])
            return
        if time.monotonic() >= deadline:
            print(f"a2a: wait timed out after {args.timeout}s "
                  f"(from={args.frm or '*'} type={args.type or '*'})", file=sys.stderr)
            sys.exit(124)
        time.sleep(args.interval)


def _preview(body, n: int = 100) -> str:
    if isinstance(body, dict) and body.get("text"):
        s = body["text"]
    elif isinstance(body, str):
        s = body
    else:
        s = json.dumps(body, sort_keys=True)
    s = " ".join(str(s).split())
    return s[:n] + ("…" if len(s) > n else "")


def cmd_check(root: Path, args) -> None:
    """Proactive one-shot probe: pull (throttled by --max-age), report unread
    compactly. Prints NOTHING when the inbox is clear, so it is safe to run
    habitually, from shell prompts, and from Claude Code hooks."""
    cfg = load_cfg(root)
    if is_relay(cfg):
        msgs = [m["msg"] for m in relay_client(cfg).fetch()["messages"]]
    else:
        last = 0.0
        try:
            last = load_json(conf_dir(root) / "state.json").get("last_sync", 0.0)
        except Exception:
            pass
        if time.time() - last >= args.max_age:
            try:
                git_sync(root)
            except RuntimeError as e:
                print(f"a2a: warning: sync failed; reporting local state: {e}",
                      file=sys.stderr)
        msgs = [m for _, m in unread_messages(root, cfg) if m is not None]
    if args.json:
        print(json.dumps({"unread": len(msgs), "messages": [
            {"id": m["id"], "from": m["from"], "type": m["type"],
             "thread": m.get("thread"), "preview": _preview(m["body"])}
            for m in msgs]}, indent=2, sort_keys=True))
        return
    if not msgs:
        return
    print(f"[a2a] {len(msgs)} unread message(s) from peer agents:")
    for m in msgs:
        t = f" (thread {m['thread']})" if m.get("thread") else ""
        print(f"  - {m['type']} from {m['from']}{t} id={m['id'][:8]}: "
              f"{_preview(m['body'])}")
    print("  read & ack: python tools/a2a.py inbox --ack   "
          "(task_request -> reply --type task_result --reply-to <id>)")


def cmd_watch(root: Path, args) -> None:
    """Foreground observer: poll the bus and print new arrivals. Never acks —
    reading remains the agent's job. For humans, tmux panes, and demos."""
    cfg = load_cfg(root)
    mode = "push" if is_relay(cfg) else f"every {args.interval}s"
    print(f"[a2a] watching as {me(cfg)} ({mode}; observer only, Ctrl-C to stop)",
          flush=True)
    try:
        if is_relay(cfg):  # held requests above a moving floor — true push
            rc = relay_client(cfg)
            floor = 0
            while True:
                res = rc.fetch(wait=RELAY_HOLD, cursor=floor)
                fresh = [m for m in res["messages"] if m["seq"] > floor]
                if fresh:
                    show_items([(f"seq:{m['seq']}", m["msg"]) for m in fresh],
                               False)
                    sys.stdout.flush()
                    floor = max(m["seq"] for m in fresh)
            return
        seen = set()
        while True:
            try:
                git_sync(root)
            except RuntimeError as e:
                print(f"a2a: warning: sync failed: {e}", file=sys.stderr)
            fresh = [(rel, m) for rel, m in unread_messages(root, cfg)
                     if rel not in seen]
            if fresh:
                show_items(fresh, False)
                sys.stdout.flush()
                seen.update(rel for rel, _ in fresh)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[a2a] watch stopped")


def cmd_file_send(root: Path, args) -> None:
    cfg = load_cfg(root)
    if not is_relay(cfg):
        git_sync(root)
    to = resolve_agent(get_cards(root, cfg), args.to)
    path = Path(args.path)
    msg = file_send(root, cfg, to, path, args.note, thread=args.thread)
    print(f"uploaded {path.name} ({path.stat().st_size} bytes), "
          f"sent file_offer to {to} id={msg['id']}")


def cmd_file_get(root: Path, args) -> None:
    cfg = load_cfg(root)
    if not is_relay(cfg):
        git_sync(root)
    dest = file_get(root, cfg, args.msg_id, Path(args.outdir))
    print(f"received {dest} (verified, cloud copy deleted, ack sent)")


def cmd_doctor(root: Path, args) -> None:
    results = []  # (level, name, detail)

    def check(name, ok, detail="", warn=False):
        results.append(("OK" if ok else ("WARN" if warn else "FAIL"), name, detail))

    cfg = load_json(cfg_path(root)) if cfg_path(root).exists() else {}
    if is_relay(cfg):
        try:
            rc = relay_client(cfg)
            h = rc.health()
            check("relay reachable", bool(h.get("ok")), cfg.get("relay_url", ""))
            adm = rc.admission()
            st = adm.get("state", "?")
            check(f"admission: {st}", st == "active",
                  adm.get("message", "") if st != "active" else f"bus={cfg.get('bus')}",
                  warn=(st == "pending"))
            if st == "active":
                rc.agents()
                check("relay auth (per-agent token)", True, f"agent={cfg.get('agent_id')}")
        except RuntimeError as e:
            check("relay reachable/auth", False, str(e)[:120])
        check("initialized (.a2a/config.json)", True,
              f"agent_id={cfg.get('agent_id')} transport=relay")
        _doctor_storage(check, cfg)
        _doctor_creds(check)
        _doctor_print(results)
        return

    p = git(root, "remote", "get-url", "origin", check=False)
    check("git remote 'origin'", p.returncode == 0, p.stdout.strip() or p.stderr.strip())
    if p.returncode == 0:
        p2 = git(root, "ls-remote", "--exit-code", "origin", "HEAD", check=False)
        check("origin reachable", p2.returncode == 0, p2.stderr.strip()[:120])

    for d in ("agents", "cursors", f"messages/{BROADCAST}", "tools"):
        check(f"layout: {d}/", (root / d).exists())

    if cfg:
        check("initialized (.a2a/config.json)", True, f"agent_id={cfg.get('agent_id')}")
        check("own card pushed", card_path(root, cfg.get("agent_id", "")).exists())
        _doctor_storage(check, cfg)
    else:
        check("initialized (.a2a/config.json)", False, "run: a2a.py init --agent-id <id>")
    _doctor_creds(check)
    _doctor_print(results)


def _doctor_storage(check, cfg) -> None:
    scfg = cfg.get("storage")
    if not scfg:
        check("storage", True, "not configured — messaging-only", warn=True)
    elif scfg.get("backend") == "local":
        check("storage: local path", Path(scfg.get("path", "")).exists(),
              scfg.get("path", ""))
    elif scfg.get("backend") == "s3":
        try:
            import boto3  # noqa: F401
            have = True
        except ImportError:
            have = False
        check("storage: boto3 importable", have,
              "" if have else "pip install boto3 (needed to SEND files only)", warn=True)
        creds = bool(_cred("AWS_ACCESS_KEY_ID")) or (Path.home() / ".aws").exists()
        check("storage: s3 credentials", creds,
              "env AWS_* / ~/.a2a/credentials / ~/.aws", warn=True)
    elif scfg.get("backend") == "hf":
        check("storage: HF_TOKEN", bool(_cred("HF_TOKEN")),
              "env HF_TOKEN or ~/.a2a/credentials", warn=True)


def _doctor_creds(check) -> None:
    if CRED_FILE.exists():
        mode = stat.S_IMODE(CRED_FILE.stat().st_mode)
        check("credentials file perms 0600", not (mode & 0o077), oct(mode))


def _doctor_print(results) -> None:
    width = max(len(n) for _, n, _ in results)
    for level, name, detail in results:
        print(f"[{level:>4}] {name:<{width}}  {detail}")
    if any(level == "FAIL" for level, _, _ in results):
        sys.exit(1)


# ---------------------------------------------------------------- argparse

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="a2a.py", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("init", help="join a relay bus (await operator approval) or init a git clone")
    p.add_argument("--agent-id", required=True)
    p.add_argument("--relay-url", help="v2: relay base URL (switches transport to relay)")
    p.add_argument("--bus", help="v2: bus (project namespace) on the relay")
    p.add_argument("--join-code", help="v2: bus join code for first contact; you then await operator approval")
    p.add_argument("--nickname", help="user-facing alias (your user's name for this cluster)")
    p.add_argument("--agent-kind", default="claude-code")
    p.add_argument("--cluster", default="unknown")
    p.add_argument("--scheduler", default="unknown")
    p.add_argument("--job-types", default="")
    p.add_argument("--gpus", default="")
    p.add_argument("--status", default="idle")
    p.add_argument("--storage", choices=sorted(BACKENDS), default=None)
    p.add_argument("--bucket")
    p.add_argument("--endpoint-url")
    p.add_argument("--region")
    p.add_argument("--local-path")
    p.add_argument("--hf-repo")
    p.set_defaults(fn=cmd_init)

    p = sub.add_parser("card", help="manage own agent card")
    csub = p.add_subparsers(dest="sub", required=True)
    c = csub.add_parser("update")
    c.add_argument("--status", choices=["idle", "busy", "offline"])
    c.add_argument("--detail")
    c.add_argument("--nickname", help="user-facing alias; '' clears it")
    c.add_argument("--job-types")
    c.add_argument("--gpus")
    c.add_argument("--new-session", action="store_true",
                   help="rotate session id (run at the start of a new working session)")
    c.set_defaults(fn=cmd_card_update)

    p = sub.add_parser("peers", help="list registered agents")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_peers)

    p = sub.add_parser("send", help="send a message")
    p.add_argument("to")
    p.add_argument("text")
    p.add_argument("--type", default="text")
    p.add_argument("--spec", help="JSON file embedded as body.spec")
    p.add_argument("--reply-to")
    p.add_argument("--thread", help="conversation/goal scope (replies inherit it)")
    p.set_defaults(fn=cmd_send)

    p = sub.add_parser("inbox", help="list messages (unread by default)")
    p.add_argument("--new", action="store_true", help="(default behavior)")
    p.add_argument("--all", action="store_true")
    p.add_argument("--ack", action="store_true")
    p.add_argument("--thread")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_inbox)

    p = sub.add_parser("wait", help="block until a matching message arrives")
    p.add_argument("--from", dest="frm")
    p.add_argument("--type")
    p.add_argument("--thread")
    p.add_argument("--timeout", type=int, default=600)
    p.add_argument("--interval", type=int, default=20)
    p.add_argument("--no-ack", action="store_true")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_wait)

    p = sub.add_parser("file", help="large-file hand-off via storage backend")
    fsub = p.add_subparsers(dest="sub", required=True)
    f = fsub.add_parser("send")
    f.add_argument("to")
    f.add_argument("path")
    f.add_argument("--note", default="")
    f.add_argument("--thread")
    f.set_defaults(fn=cmd_file_send)
    f = fsub.add_parser("get")
    f.add_argument("msg_id")
    f.add_argument("-o", "--outdir", default=".")
    f.set_defaults(fn=cmd_file_get)

    p = sub.add_parser("check", help="one-shot unread probe (silent when clear; hook-friendly)")
    p.add_argument("--max-age", type=int, default=0,
                   help="skip pulling if last sync was within S seconds")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_check)

    p = sub.add_parser("watch", help="foreground observer: print arrivals, never ack")
    p.add_argument("--interval", type=int, default=30)
    p.set_defaults(fn=cmd_watch)

    p = sub.add_parser("doctor", help="diagnose setup")
    p.set_defaults(fn=cmd_doctor)
    return ap


def main(argv=None) -> None:
    args = build_parser().parse_args(argv)
    root = repo_root()
    try:
        args.fn(root, args)
    except RuntimeError as e:
        die(str(e))


if __name__ == "__main__":
    main()
