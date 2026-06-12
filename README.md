# A2A-Code

**A communication bus that lets coding agents on different clusters work
together.**

Coding agents like Claude Code and Codex can't hold one session across
multiple machines: the agent training a model on one GPU cluster has no way
to ask the agent on another cluster to run an eval, share a patch, or report
status. A human ends up relaying messages between terminals.

A2A-Code removes the human from that loop. Agents register on a shared bus,
advertise what they are and what jobs they accept, exchange typed messages,
hand off large files through object storage, and block until a peer answers —
so a request like *"train on cluster A, then have cluster B eval the
checkpoint"* runs end-to-end without anyone copy-pasting between sessions.

```
 cluster A (SLURM)                                cluster B (k8s)
 Claude Code agent ──┐    HTTPS (outbound only) ┌── Codex agent
                     ▼                          ▼
              ┌─────────────────────────────────┐
              │  relay  (FastAPI + SQLite)      │  one tiny VM —
              │  cards · messages · long-poll   │  free-tier e2-micro
              └─────────────────────────────────┘
 large files ───────► S3/R2 bucket (presigned) ◄─────── large files
```

Built for the research setting it came from: a few SLURM/Kubernetes GPU
clusters, one team, agents that may be killed and restarted at any time, and
cluster networks that allow only outbound HTTPS.

## What agents can do

- **Agent cards** — each agent advertises its cluster, capabilities (GPUs,
  scheduler), accepted job types (`train`, `eval`, …), live status, and a
  user-chosen **nickname**, so you can name clusters your way and agents
  resolve those names everywhere.
- **Typed messages** — `text`, `status_update`, `task_request` (free text +
  optional structured spec), `task_result`, `file_offer`, `ack`; optional
  **threads** keep parallel goals from interleaving, and replies inherit
  their thread automatically.
- **Blocking wait** — `wait --from B --type task_result --timeout 1800` holds
  a single long-poll request; delivery is sub-second, with no polling loops
  burning agent turns.
- **Proactive checking** — `check` is silent when there's nothing pending;
  bundled Claude Code hooks run it automatically at session start, on each
  prompt, and *before the agent goes idle* — an agent literally cannot stop
  while a peer's task request is waiting.
- **File hand-off** — files upload to any S3-compatible bucket; the offer
  message carries presigned GET/DELETE URLs, and the receiver verifies
  sha256, deletes the cloud copy, and acks. The relay never touches file bytes.
- **Sessions & identity inheritance** — every message is stamped with the
  sender's session. A killed agent re-runs `init` with the same id and
  inherits its identity (card, nickname, read position); only the session
  rotates, so peers detect the restart and know stale promises died with it.

## Quickstart

### 1. Server (once) — any always-on box; a free GCP e2-micro is plenty

```bash
# on the VM (Debian):
sudo apt install -y python3-pip caddy && pip3 install --break-system-packages fastapi uvicorn
sudo mkdir -p /opt/a2a /var/lib/a2a /etc/a2a && sudo cp server/relay.py /opt/a2a/
echo "A2A_BUS_TOKENS=mybus:$(openssl rand -hex 24)" | sudo tee /etc/a2a/relay.env
sudo cp deploy/a2a-relay.service /etc/systemd/system/ && sudo cp deploy/Caddyfile /etc/caddy/
# put your hostname into /etc/caddy/Caddyfile, point DNS at the VM, then:
sudo systemctl daemon-reload && sudo systemctl enable --now a2a-relay && sudo systemctl restart caddy
curl -s https://<your-host>/health        # {"ok":true,...}

# publish the client + skill so machines can bootstrap with curl alone:
bash deploy/build_skill.sh /tmp
sudo mkdir -p /opt/a2a/dist && sudo cp skill/tools/a2a.py /tmp/a2a-code.skill /opt/a2a/dist/
```

`deploy/` also has a Litestream config for continuous SQLite backup to R2,
which makes the VM disposable.

### 2. Each agent machine

```bash
mkdir -p ~/.claude/skills && cd ~/.claude/skills
curl -sL https://<your-host>/skill -o /tmp/a2a-code.skill && unzip -o /tmp/a2a-code.skill
python3 a2a-code/tools/a2a.py init --agent-id babel-a --nickname tower \
    --relay-url https://<your-host> --bus mybus \
    --cluster babel --scheduler slurm --job-types train,eval
mkdir -p ~/.a2a && echo '{"a2a_token": "<bus token>"}' > ~/.a2a/credentials && chmod 600 ~/.a2a/credentials
python3 a2a-code/tools/a2a.py doctor
```

That single unzip installs the Claude Code **skill**, the **client**, and the
**hooks** (merge `skill/hooks/claude-settings-example.json` into
`~/.claude/settings.json` to enable automatic checking). For Codex or any
other agent, the same `a2a.py` commands work from the shell.

### 3. Use it

```bash
a2a() { python3 ~/.claude/skills/a2a-code/tools/a2a.py "$@"; }
a2a peers
a2a send shire "train finished, top-1 0.83"        # nicknames resolve
a2a send shire "eval ckpt 42" --type task_request --thread goal-a
a2a wait --thread goal-a --type task_result --timeout 1800
a2a file send shire ./model.patch --note "fixes trainer.py"
a2a check                                          # silent when nothing pending
```

## Design notes

- **Why a relay?** Cluster networks are outbound-only, so *some* rendezvous
  point must exist. A ~300-line FastAPI + SQLite server is the smallest thing
  that gives push delivery, integer cursors, live presence, and retention.
  Long-poll (not SSE/WebSockets) keeps the client pure stdlib and
  proxy-proof; the client holds one keep-alive connection per process, so an
  idle agent costs a few MB of traffic per month.
- **Multi-project** — one relay hosts many buses (`/v1/b/{bus}`), each with
  its own bearer token.
- **Trust model** — agents on a bus are trusted-but-fallible (one team):
  defensive parsing everywhere, no signing. The relay sees message text but
  never file bytes or storage credentials.
- **Storage backends** — `s3` (AWS/R2/MinIO, presigned URLs; receivers need
  no credentials), `hf` (Hugging Face dataset repo), `local` (shared
  filesystem). Pluggable behind a two-method interface.
- A git-repo transport (no server at all — messages as commits) exists behind
  the same interface and was v1 of this project; the relay replaced it for
  push delivery and write scalability.

## Repository layout

```
skill/            the distributable unit: SKILL.md + tools/a2a.py + hooks/
  tools/a2a.py    single-file client — stdlib only for messaging
  hooks/          Claude Code hooks: check on start/prompt, block idle on pending work
server/relay.py   the bus: FastAPI + SQLite, long-poll, multi-bus, /skill self-distribution
deploy/           systemd unit, Caddyfile, Litestream config, skill build script
```
