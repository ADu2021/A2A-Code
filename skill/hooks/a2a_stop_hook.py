#!/usr/bin/env python3
"""Claude Code Stop-hook for A2A-Code.

Before the agent goes idle, check the bus once (git pull). If peer agents
sent messages — especially task_requests — block the stop and hand the agent
its mail, so cross-cluster work continues without a human nudge.

Install (in the project or user settings of the agent's machine):

  // .claude/settings.json
  {"hooks": {"Stop": [{"hooks": [
      {"type": "command", "command": "python /path/to/bus/hooks/a2a_stop_hook.py"}
  ]}]}}

The bus clone is found via $A2A_BUS (default: ~/a2a-bus). Loop-guarded: when
Claude Code re-runs the hook after we already blocked once
(`stop_hook_active`), we stay silent — the agent acks mail with
`inbox --ack`, which also clears the trigger.
"""
import json
import os
import subprocess
import sys
from pathlib import Path


def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        payload = {}
    if payload.get("stop_hook_active"):  # already continued once — never loop
        return

    candidates = [os.environ.get("A2A_BUS", ""),
                  "~/.claude/skills/a2a-code",   # skill-bundled install
                  "~/a2a-code", "~/a2a-bus"]     # repo-clone installs
    bus = tool = None
    for cand in candidates:
        if not cand:
            continue
        p = Path(cand).expanduser()
        if (p / "tools" / "a2a.py").exists():
            bus, tool = p, p / "tools" / "a2a.py"
            break
    if tool is None:
        return

    p = subprocess.run(
        [sys.executable, str(tool), "check", "--json", "--max-age", "0"],
        capture_output=True, text=True, timeout=300)
    if p.returncode != 0:
        return  # uninitialized clone / sync trouble — never block the agent
    try:
        data = json.loads(p.stdout)
    except Exception:
        return
    if not data.get("unread"):
        return

    lines = []
    for m in data["messages"]:
        t = f" (thread {m['thread']})" if m.get("thread") else ""
        lines.append(f"- {m['type']} from {m['from']}{t}: {m['preview']}")
    reason = (
        "A2A-Code: peer agents sent you messages while you were working:\n"
        + "\n".join(lines)
        + f"\nBefore going idle, handle them: cd {bus} && "
          "python tools/a2a.py inbox --ack. If a task_request matches your "
          "capabilities and you are free, do it and reply with "
          "--type task_result --reply-to <id>; otherwise tell the sender why not.")
    print(json.dumps({"decision": "block", "reason": reason}))


if __name__ == "__main__":
    main()
