# hcom

[![PyPI](https://img.shields.io/pypi/v/hcom)](https://pypi.org/project/hcom/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/Built_with-Rust-dea584)](https://www.rust-lang.org/)
[![GitHub stars](https://img.shields.io/github/stars/aannoo/hcom)](https://github.com/aannoo/hcom/stargazers)

> **Hook your coding agents together**

Prefix agents with `hcom` to let them message, watch, and spawn each other across terminals.

https://github.com/user-attachments/assets/1ce23ed9-f529-4be0-8124-816aa4c2fd43

---

## Install

```bash
brew install aannoo/hcom/hcom
```

<details><summary>Other install options</summary>

```bash
# Shell installer for macOS, Linux, Android (Termux), and WSL
curl -fsSL https://github.com/aannoo/hcom/releases/latest/download/hcom-installer.sh | sh
```

```bash
# With PyPI
uv tool install hcom  # or: pip install hcom
```

</details>

---

## Quickstart

```bash
hcom claude  # gemini / codex / opencode
```

Prompt:

- `send a message to codex`
- `review what claude did and send fixes`
- `spawn 3x gemini, split work, collect results`
- `fork yourself to investigate the bug and report back`

Open the TUI:

```bash
hcom
```

---

## What agents can do

**Message** each other in real-time, bundle context for handoffs.

**Observe** each other: transcripts, file edits, terminal screens, command history.

**Subscribe** to each other: notify on status changes, file edits, specific events. React automatically.

**Spawn**, **fork**, **resume**, **kill** each other, in any terminal emulator.

---

## How it works

Hooks record activity to a local SQLite database and deliver messages from it.

```bash
agent → hooks → db → hooks → other agent
```

For **Claude Code**, **Gemini CLI**, **Codex**, and **OpenCode**, messages arrive mid-turn (injected between tool calls) or wake idle agents immediately. Any other AI tool can join by running `hcom start`. Any process can wake agents with `hcom send`.

Hooks go into tool config dirs under `~/` (or `HCOM_DIR`) on first run. If you aren't using hcom, the hooks do nothing.

---

## Terminal

Every agent runs in a real terminal you can see, scroll, and interrupt. Any emulator works for spawning; **kitty**, **wezterm**, and **tmux** additionally support closing panes from `hcom kill`.

```bash
hcom config terminal --info   # tell agent to run this to configure other terminals
```

---

## Cross-device

Connect agents across machines via MQTT relay.

```bash
hcom relay new                 # get token
hcom relay connect <token>     # on each device
```

```bash
hcom relay status              # check connection
hcom relay off|on              # toggle
```

<details>
<summary>Relay Security</summary>

### Security

- Relay payloads are end-to-end encrypted. Brokers do not see data.
- Treat the join token like an SSH key or API key.
- If the token may have leaked, run `hcom relay off --all` to disconnect all devices.
- Use a private/custom/self-hosted broker with `--broker` and `--password` for better security.

### Security model

`hcom relay` is one trust domain for one operator's devices. Membership is all-or-nothing. There are no scoped roles, read-only peers, or per-device permissions.

Relay payloads use a shared PSK with XChaCha20-Poly1305. The encryption binds each payload to the relay, topic, and timestamp. A replay guard drops duplicate envelopes inside a freshness window.

Brokers and network observers cannot read or forge payloads without the PSK. They can still see metadata: topic names, timing, message sizes, and connection patterns.

### What the token means

The join token contains the relay ID, broker URL, and raw PSK. hcom does not ask a server to validate it. It has no expiry, no scope, and no revocation list.

On public brokers, a leaked token gives an attacker full control of the relay. They can decrypt captured traffic, publish authenticated relay traffic, send text to listening agents, launch agents on enrolled devices, kill running agents, and use remote relay RPCs. If those agents can run tools, treat that as shell access on every enrolled device in the relay.

On private brokers with `--password`, the token still leaks the PSK, so captured traffic is still exposed. But the token alone is not enough to publish unless the attacker also has the broker password. Use a private broker when broker-side access control matters, or when the metadata shape of your traffic is itself sensitive. `--password` is broker access control, not another layer of message encryption.

### Limits by design

- Forward secrecy. A leaked PSK can decrypt old captured traffic.
- Per-device attribution inside a relay. Sender identity is routing metadata, not authorization. Every enrolled device speaks with full authority.
- Prompt injection from an authenticated peer. Enrollment is total trust — a peer can launch, kill, and drive agents via RPC, not just send messages. Only enroll devices you would give shell access to.
- Local OS compromise. hcom trusts the local user account and `~/.hcom/config.toml`. It does not defend against another user on the same account or malware with filesystem access.

### Storage

The PSK is stored in `~/.hcom/config.toml`. On Unix, hcom writes that file with mode `0600`.

hcom keeps the PSK out of environment variables. Remote `config_get` and `config_set` refuse `relay_psk`, `relay_token`, `relay_id`, and the broker URL. `hcom relay status` shows only a short fingerprint so two devices can verify they share the same key without printing it.

Anyone who can read that file — another user on the same OS account, malware, or a backup written without preserving permissions — has the full PSK.

### Incident response

Run `hcom relay off --all`. It asks every reachable trusted peer to disable the relay, then disables it locally, so your agents stop acting on attacker messages. It is best-effort damage control, not containment: the attacker's device ignores the request.

The PSK cannot be revoked. There is no server to notify and no denylist to update. Anyone who has the PSK can keep using the old relay until you stop using it.

To keep using relay after a leak, create a new relay with `hcom relay new` and move every trusted device to the new token. Rotation also changes the `relay_id`, so retained state on the old broker topics is orphaned.

</details>

---

## Troubleshoot

```bash
hcom status           # diagnostics
hcom reset all        # clear and archive: database + hooks + config
hcom run docs         # tell agent to run
```

---

## Uninstall

```bash
hcom hooks remove                     # safely remove all hcom hooks
brew uninstall hcom                   # or: rm $(which hcom)
```

---

## Reference

<details>
<summary>Tools & Launch</summary>

### Supported tools

| Tool | Automatic delivery | Connect |
|---|---|---|
| Claude Code | yes, hooks | `hcom claude` |
| Gemini CLI | yes, hooks | `hcom gemini` |
| Codex CLI | yes, hooks | `hcom codex` |
| OpenCode | yes, plugin | `hcom opencode` |
| Anything else | manual poll via `hcom listen` | `hcom start` (run inside tool) |

### Launch

**`hcom [N] <tool> [tool-args] [hcom-flags]`**

```bash
hcom 3 claude                 # launch 3 instances
hcom claude --model sonnet    # unknown args forwarded to tool
hcom claude --terminal kitty  # launch in a specific terminal
hcom claude --headless        # run in background with hcom pty
```

### Claude Code headless and subagents

Detached background processes in print mode stay alive. Manage them through the TUI.

```bash
hcom claude -p 'say hi in hcom'
```

For subagents, run `hcom claude`, then prompt:

> run 2x task tool and get them to talk to each other in hcom

</details>


<details>
<summary>CLI</summary>

### CLI commands

What you might type from a shell. Agents run their own commands that they learn from the hcom CLI primer (~700 tokens) at launch. `hcom <command> --help` for full flags.

### Spawn

```bash
hcom                                # TUI dashboard
hcom [N] claude|gemini|codex|opencode   # launch N agents (default 1)
hcom r <name>                       # resume stopped agent
hcom f <name>                       # fork session (claude/codex/opencode)
hcom kill <name|tag:T|all>          # kill + close terminal pane
```

hcom launch flags:

| Flag | Purpose |
|---|---|
| `--tag <name>` | Group label — agents spawn as `tag-name` and can be addressed as `@tag` |
| `--terminal <preset>` | Where windows open: `kitty`, `wezterm`, `tmux`, `cmux`, `iterm`, … |
| `--dir <path>` | Working directory for the agent |
| `--headless` | Run in background with no terminal window |
| `--device <name>` | Spawn on a remote device (via relay) |
| `--hcom-prompt <text>` | Initial user prompt |
| `--hcom-system-prompt <text>` | System prompt override |

Anything else is forwarded to the tool: `--model sonnet`, `--yolo`, `--sandbox danger-full-access`, `-p`, etc.

### Observe

```bash
hcom list                           # who's alive, status, unread counts
hcom term [name]                    # view/inject into an agent's PTY screen
hcom transcript <name> [N-M]        # read an agent's conversation
hcom events --agent <name>          # event history (messages, file edits, lifecycle)
```

### Drive

```bash
hcom send -b @luna -- hey           # one-off message to an agent
hcom run <script>                   # run a workflow (debate, confess, fatcow, …)
hcom run                            # list available scripts
```

### Configure

```bash
hcom hooks add|remove [tool]        # install/remove tool hooks
hcom config                         # view/edit global + per-agent settings
hcom status [--logs]                # installation + diagnostics
hcom archive [N]                    # browse past sessions
hcom reset [all]                    # archive + clear (+ reset hooks/config)
hcom update                         # self-update
```

### Sync across devices

```bash
hcom relay new                      # create group, get join token
hcom relay connect <token>          # join from another device
hcom relay on|off                   # toggle sync
hcom relay daemon start|stop        # manage background daemon
```

</details>

<details>
<summary>Config</summary>

### Configuration

Config lives in `~/.hcom/config.toml`. Precedence: defaults < `config.toml` < env vars.

```bash
hcom config                       # show all values with sources
hcom config <key>                 # get
hcom config <key> <value>         # set
hcom config <key> --info          # detailed help for a key
hcom config --edit                # open config.toml in $EDITOR
hcom config -i <name|self> ...    # per-agent override (tag, timeout, hints, subagent_timeout)
```

### Keys

| Key | Purpose |
|---|---|
| `tag` | Group label — launched agents become `tag-name` |
| `hints` | Text appended to every message the agent receives |
| `notes` | Text appended to bootstrap (one-time, at launch) |
| `auto_approve` | Auto-approve safe hcom commands (send/list/events/…) |
| `auto_subscribe` | Event subscription presets: `collision`, `created`, `stopped`, `blocked` |
| `timeout` | Idle timeout for headless/vanilla Claude (seconds) |
| `subagent_timeout` | Keep-alive for Claude subagents (seconds) |
| `name_export` | Export instance name to a custom env var |
| `terminal` | Where new agent windows open (`hcom config terminal --info`) |
| `claude_args` / `gemini_args` / `codex_args` / `opencode_args` | Default args passed to the tool (launch-time args win) |
| `codex_sandbox_mode` | `off`, `workspace`, … |
| `gemini_system_prompt` / `codex_system_prompt` | System prompt injected on launch |
| `relay` / `relay_id` / `relay_token` / `relay_psk` / `relay_enabled` | Relay config (see `hcom relay --help`) |

### Scope

```bash
hcom config tag mycrew                          # global
hcom config -i luna hints "respond in JSON"     # per-agent
HCOM_TAG=dev hcom 3 claude                      # per-launch env
```

### Per-project isolation

```bash
export HCOM_DIR="$PWD/.hcom"    # isolate state + hooks to this project
hcom hooks remove && rm -rf "$HCOM_DIR"
```

Run `hcom config <key> --info` or `hcom run docs --config` for the full per-key reference.

Edit `~/.hcom/env` to set env vars passed to every launched agent (e.g. `ANTHROPIC_MODEL`, `GEMINI_MODEL`, API keys). Any non-`HCOM_*` key works.

</details>

<details>
<summary>Workflow Scripts</summary>

### Multi-agent workflows

Bundled and user scripts (`~/.hcom/scripts/`) for multi-agent patterns:

```bash
hcom run                   # list available scripts
hcom run debate "topic"    # run one
hcom run docs              # tell agent to run this to create any new workflow
```

### Included Scripts

Tell agent to run them:

**`hcom run confess`** — An agent (or background clone) writes an honesty self-eval. A spawned calibrator reads the target's transcript independently. A judge compares both reports and sends back a verdict via hcom message.

**`hcom run debate`** — A judge spawns and sets up a debate with existing agents. It coordinates rounds in a shared thread where all agents see each other's arguments, with shared context of workspace files and transcripts.

**`hcom run fatcow`** — headless agent reads every file in a path, subscribes to file edit events to stay current, and answers other agents on demand.

Custom scripts: drop `*.sh` or `*.py` into `~/.hcom/scripts/` — auto-discovered, override bundled scripts of the same name. Ask an agent to author one; `hcom run docs --scripts` is the authoring guide.

</details>

<details>
<summary>Build</summary>

### Building from Source

```bash
# Prerequisites: Rust 1.86+

git clone https://github.com/aannoo/hcom.git
cd hcom
cargo test
cargo build
```

### Using local build

Two options:

**Symlink** — simple, dev build is global.

```bash
ln -sf $(pwd)/target/debug/hcom ~/.cargo/bin/hcom
```

**dev_root** — works regardless of how hcom was installed (brew, pip, etc.); picks the newer of debug/release automatically:

```bash
hcom config dev_root $(pwd)
hcom config dev_root --unset  # revert
hcom -v    # run local build
```

For concurrent worktrees, scope each to its own DB:

```bash
HCOM_DIR=$PWD/.hcom HCOM_DEV_ROOT=$PWD hcom claude
```

</details>


---

## Contributing

Issues and PRs welcome. The codebase is Rust.

```bash
cargo test && cargo build
hcom config dev_root $(pwd)
hcom -v
```

---

## License

[MIT](LICENSE)
