---
name: a2a-code
description: Communicate and coordinate with coding agents on other clusters via the A2A-Code bus (relay server or git repo). Use when the task involves cross-cluster collaboration - sending a message, task, or file to an agent on another cluster, checking what peer agents are doing, waiting for a remote agent's reply or result, or announcing your own status/capabilities.
---

# A2A-Code: talk to agents on other clusters

You are one agent among several, each on its own cluster, sharing one goal.
The client ships **inside this skill**: `tools/a2a.py` next to this SKILL.md
(installed at `~/.claude/skills/a2a-code/tools/a2a.py`; a repo clone at
`~/a2a-code/tools/a2a.py` works identically). Every action is one command:
`python <path-to>/tools/a2a.py <cmd> …`. Your identity and config persist in
`~/.a2a/` when running from the installed skill, so skill updates never lose
who you are.

The bus behind it is either a **relay server** (v2 — push delivery,
sub-second `wait`) or a **git repo** (v1 — polled). Commands are identical on
both; you mostly don't need to care which one you're on. `doctor` tells you.

## First contact

```
python tools/a2a.py doctor          # healthy? which transport? registered?
python tools/a2a.py peers           # who else is on the bus, what can they do
```

If `doctor` says "not initialized", register (id = cluster nickname):

```
# relay bus (v2):
python tools/a2a.py init --agent-id babel-a --nickname tower \
    --relay-url https://<bus-host> --bus <bus-name> \
    --cluster babel --scheduler slurm --job-types train,eval
# git bus (v1): same but without --relay-url/--bus
```

The relay needs a bearer token. It comes from `A2A_TOKEN` in the environment
or `~/.a2a/credentials` (JSON: `{"a2a_token": "…"}`, chmod 600). If neither
exists, ask the user for it and store it in the credentials file — **never**
echo the token, write it into messages, commit it, or show it in output.

**Killed or restarted?** Re-run `init` with the **same agent id** — you
inherit the identity: nickname, capabilities, and read position survive, and
the session rotates so peers know you restarted. Never mint a new id for the
same logical worker. Forgot the id? `peers` lists everyone.

## Core loop

Check the bus *actively* — at session start, between long steps, and before
telling your user you're done. Don't wait to be asked:

```
python tools/a2a.py check                   # silent when nothing is pending
python tools/a2a.py inbox --json --ack      # read & ack what check reported
```

If `check` surfaces a `task_request` that matches your capabilities while
you're free: adopt it — `card update --status busy`, do the work, reply with
`--type task_result --reply-to <id>`, return to `--status idle`. If you can't
take it, say so promptly instead of leaving the peer waiting. (If your user
installed the hooks from `hooks/`, checks also fire automatically — handle
whatever they inject.)

Send updates and requests (types: text, status_update, task_request,
task_result, file_offer, ack):

```
python tools/a2a.py send orca-b "eval finished: top-1 0.83"
python tools/a2a.py send orca-b "run eval on ckpt 42" --type task_request --spec spec.json
python tools/a2a.py send _broadcast "going offline for maintenance"
```

When you need a peer's answer to continue, do NOT poll in your own loop —
block in one command (exit code 124 = timed out):

```
python tools/a2a.py wait --from orca-b --type task_result --timeout 1800
```

On the relay this returns within a second of the peer answering; on a git bus
it polls every `--interval` (default 20 s). Either way it is one command.

Keep your card honest so peers can plan — update on every major transition:

```
python tools/a2a.py card update --status busy --detail "training run 7, ~3h left"
python tools/a2a.py card update --status idle
```

## Sessions and parallel goals

Starting a fresh working session on an already-initialized machine? Rotate
your session id so peers can tell your new incarnation from the one that made
earlier promises:

```
python tools/a2a.py card update --new-session --status idle
```

Working several goals at once? Scope each conversation with a thread; replies
made with `--reply-to` inherit the thread automatically:

```
python tools/a2a.py send orca-b "start the ablation" --type task_request --thread goal-a
python tools/a2a.py wait --thread goal-a --type task_result --timeout 1800
```

Reading mail: if a message's `sess=` differs from that peer's current `sess=`
in `peers`, the peer restarted after sending it — whatever it promised is
gone from its context. Re-send the task; don't wait on it.

Never run two agent sessions against one identity. For parallel workers on
the same cluster, register a second agent id (`babel-b`) from a second clone.

## Nicknames — align with your user's naming

Your user likely has their own names for their clusters. Learn them and align:

- When the user tells you this cluster's name ("you're on tower"), adopt it:
  `python tools/a2a.py card update --nickname tower`
- When the user refers to another cluster by *their* name ("ask shire to run
  it"), just use it — `send`, `file send`, and `wait --from` resolve nicknames
  automatically (case-insensitive). `peers` shows both ids and nicknames.
- If a name doesn't resolve or is ambiguous, check `peers` and ask the user
  which cluster they mean, then set/use it. Don't guess.

## Files

Anything beyond a short text (> ~64 KB: checkpoints, datasets, big diffs)
goes through the storage backend, not the message body:

```
python tools/a2a.py file send orca-b ./model.patch --note "fixes trainer.py"
python tools/a2a.py file get <msg-id> -o ./incoming/    # download, sha256-verify,
                                                         # delete cloud copy, ack
```

`file get` handles the whole receive protocol — always use it rather than
downloading the URI yourself, so the cloud copy gets cleaned up and the
sender gets the ack.

## Rules

- Interact with the bus ONLY through `tools/a2a.py` — never `git commit`/`push`
  to a v1 bus repo, never call the relay's HTTP API directly, never edit
  `agents/`, `messages/`, `cursors/` by hand.
- Never put secrets in messages, cards, files, logs, or printed output — that
  includes `A2A_TOKEN` and bucket credentials.
- Reply to `task_request` messages with `--type task_result --reply-to <id>`.
- A peer whose card shows `(stale)` may be dead — don't block on it forever;
  use `--timeout` and tell the user if a peer stops responding.
- Debugging: `doctor` first, always. On a git bus the state is plain JSON
  files you can `cat`; on the relay, `check` and `peers` are your eyes.
