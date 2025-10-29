# discsync

A simple Discord archiver that mirrors guilds, channels, users, and messages
into a SQL database (SQLite locally, Postgres in prod). It continuously polls
and fills gaps to maintain a complete history. Bot and webhook messages are
ignored.

## Why would you even need this?

I built this solely to fit some fun use cases for my friends and I. Namely,
I'm working on a tool that transforms group sentiment to an active stock ticker.
When working with data from discord, I sometimes want to refresh things quickly for testing,
as well as have a much more quick-access location to query historical data. This is not 
possible in Discord natively, especially given the lack of a search feature.

## Overview
- Enumerates all guilds the bot can access and upserts their metadata.
- Enumerates message-bearing channels (Text, News, Threads, Forum) and upserts
  their metadata.
- Backfills full message history per channel using pagination and stores nested
  fields (mentions, attachments, embeds, components, reactions, references) as
  JSON.
- Repeats periodically to catch new messages and fill any gaps.

## Quick start

### Build
```bash
cargo build --release
```

### Set variables

```bash
export DISCORD_TOKEN=my_bot_token
export DATABASE_URL=my_postgres_url_or_sqlite_path (if this is missing, it will use a local sqlite path)
```

### Run

Variables can also be passed in as flags.

```bash
# Postgres
target/release/discsync \
  --discord-token my_discord_token \
  --database-url postgres://user:pass@host:5432/dbname
  
# Sqlite
target/release/discsync \
  --discord-token my_discord_token
```

## Technical Details

### Schema overview
- Tables: `guilds`, `channels`, `users`, `messages`.
- Each table also stores the full raw Discord JSON in `raw_json` for fidelity.
- `messages` includes nested JSON columns for mentions, attachments, embeds,
  components, reactions, and message_reference. `created_at_ms` is derived from
  the Discord snowflake for stable sorting.

### Notes
- Message Content intent must be enabled for your bot in the Discord Developer Portal.
- Ensure the bot has View Channels and Read Message History permissions.
- `.env` is not auto-loaded in Windows; set env vars explicitly or pass flags.

### Troubleshooting
- No messages stored: check bot permissions and that Message Content intent is enabled.
- SQLite lock issues on Windows: close readers and try again; WAL is not explicitly enabled.