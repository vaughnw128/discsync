# sbux

Discord → embeddings → Qdrant pipeline, with a SQLite backup and resumable checkpoints.

What it does
- Streams Discord MessageCreate events (Twilight gateway) and batches messages.
- Creates local embeddings using ONNX Runtime (all-MiniLM-L6-v2), downloaded on first run.
- Stores raw messages in SQLite (or another database in the future) and upserts vectors to Qdrant.
- Periodic REST "check job" can backfill missed messages when a guild ID is provided.

Quick start (Windows cmd.exe)
1) Build
```cmd
cargo build --release
```

2) Run with environment variables (recommended for tokens)
```cmd
set DISCORD_TOKEN=YOUR_BOT_TOKEN
set QDRANT_URL=http://localhost:6333
rem Optional: enable backfill (per-guild)
set GUILD_ID=123456789012345678

rem If DATABASE_URL is omitted, sbux will create and use sqlite://messages.sqlite automatically
 target\release\sbux.exe
```

3) Or run with CLI flags
```cmd
rem Provide Discord token and Qdrant via flags
 target\release\sbux.exe ^
  --discord-token %DISCORD_TOKEN% ^
  --qdrant-url http://localhost:6333 ^
  --guild-id 123456789012345678

rem Use a specific SQLite file path (auto-creates if missing)
 target\release\sbux.exe ^
  --discord-token %DISCORD_TOKEN% ^
  --database-url C:\data\discord_messages.sqlite ^
  --qdrant-url http://localhost:6333

rem Use Postgres in the future (URL is accepted as-is)
 target\release\sbux.exe ^
  --discord-token %DISCORD_TOKEN% ^
  --database-url postgres://user:pass@host:5432/dbname ^
  --qdrant-url http://localhost:6333
```

CLI flags (also read from environment)
- `--discord-token` (env: DISCORD_TOKEN) – Discord bot token.
- `--database-url` (env: DATABASE_URL) – URL or file path:
  - If omitted, sbux creates and uses `sqlite://messages.sqlite`.
  - If given a path (e.g., `C:\path\db.sqlite`), sbux treats it as SQLite and creates the file if missing.
  - If given a full URL (`sqlite://...` or `postgres://...`), it’s used as-is.
- `--qdrant-url` (env: QDRANT_URL, default: `http://localhost:6333`) – Qdrant endpoint.
- `--guild-id` (env: GUILD_ID) – Enables periodic REST check job to backfill per guild.
- `--model-dir` (env: MODEL_DIR, default: `model/minilm`) – Where the model/tokenizer are stored.

Storage
- SQLite tables
  - `messages(id TEXT PRIMARY KEY, content TEXT, author_id TEXT, timestamp INTEGER, embedded INTEGER DEFAULT 0)`
  - `channel_checkpoints(channel_id TEXT PRIMARY KEY, last_message_id TEXT, last_timestamp INTEGER)`
- Qdrant collection
  - Name: `discord_messages`
  - Vector size: 384 (cosine distance)
  - Payload: `{ message_id: string, timestamp: i64, author_id: string }`

Notes
- Message Content intent must be enabled for your bot in the Discord Developer Portal.
- First run downloads the ONNX model + tokenizer (~small-ish). Subsequent runs are offline.
- The periodic check job (when `--guild-id` is provided) queries REST every 60s and enqueues any newer messages after your last checkpoint.
- `.env` is not auto-loaded in cmd.exe; set env vars explicitly or pass flags.

Troubleshooting
- Qdrant not reachable: ensure `QDRANT_URL` points to a running Qdrant instance.
- No messages stored: check bot permissions and that Message Content intent is enabled.
- SQLite lock issues on Windows: close readers and try again; WAL is not explicitly enabled.

Debian/Ubuntu setup
- Requirements
  - build-essential (C/C++ toolchain)
  - pkg-config
  - libsqlite3-dev (for SQLx SQLite bindings)
  - curl, git (for convenience)
  - Optional: a system libonnxruntime if you don’t want the crate to auto-download

1) Install system packages
```bash
sudo apt update
sudo apt install -y build-essential pkg-config libsqlite3-dev curl git
```

2) Optional: install system ONNX Runtime (libonnxruntime)
By default, this project auto-downloads ONNX Runtime at build/run via the `ort` crate. If you prefer a system library:
- Download the CPU Linux x64 archive from the official releases (https://github.com/microsoft/onnxruntime/releases)
- Extract and point sbux to the shared library via `ORT_DYLIB_PATH`.
Example:
```bash
# Example using a specific ORT version; adjust for the latest release
VER=1.18.0
curl -LO https://github.com/microsoft/onnxruntime/releases/download/v${VER}/onnxruntime-linux-x64-${VER}.tgz
sudo mkdir -p /opt/onnxruntime/${VER}
sudo tar -xzf onnxruntime-linux-x64-${VER}.tgz -C /opt/onnxruntime/${VER} --strip-components=1

# Tell the app where to find the runtime (libonnxruntime.so)
export ORT_DYLIB_PATH=/opt/onnxruntime/${VER}/lib/libonnxruntime.so
```

3) Install Rust (via rustup)
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
```

4) Build
```bash
cargo build --release
```

5) Run (Linux)
```bash
export DISCORD_TOKEN=YOUR_BOT_TOKEN
export QDRANT_URL=http://localhost:6333
# Optional: enable periodic backfill per guild
export GUILD_ID=123456789012345678

# If DATABASE_URL is omitted, SQLite file messages.sqlite is created and used automatically
./target/release/sbux
```

Troubleshooting on Debian/Ubuntu
- sqlite3 linker errors: ensure `libsqlite3-dev` is installed, then `cargo clean && cargo build`.
- ONNX Runtime download blocked (airgapped network): install a system `libonnxruntime.so` and set `ORT_DYLIB_PATH` as shown above.
- Permission denied on model dir: override with `--model-dir` to a writable path.
