# wechat-pipeline

WeChat chat data pipeline for kusanagi (MacBook Air M-series).

## Components

### decrypt/
Decrypts WeChat 4.1.x SQLite databases using lldb memory scan.
-  — extract encryption keys from running WeChat process
-  — decrypt raw .db files → decrypted/
-  — called by LaunchAgent hourly
-  — FastMCP server for AI queries

### sync/
Syncs decrypted WeChat messages to Zeabur PostgreSQL.
-  — main sync script (reads decrypted/ → uploads to Postgres)
- Handles WeChat 4.1.x schema (Msg_<hash> tables, zstd-compressed source field)

## LaunchAgents (kusanagi)
| Agent | Script | Schedule |
|-------|--------|---------|
| com.velocity1.wechat.decrypt | decrypt/wechat-decrypt-hourly.sh | hourly |
| com.velocity1.wechat.sync | sync/sqlite_to_postgres.py | daily 7AM |
| com.velocity1.wechat.mcp | decrypt/mcp_server.py | always-on |
