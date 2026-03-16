# wechat-pipeline

WeChat chat data pipeline — extracts, decrypts, and syncs WeChat 4.1.x messages to Zeabur PostgreSQL.

**Tested on:** macOS 15, Apple Silicon (M-series), WeChat 4.1.8

---

## Architecture

```
WeChat (running) ──[lldb memscan]──► wechat_keys.json
                                           │
WeChat encrypted .db files ──[decrypt_db.py + sqlcipher]──► decrypted/*.db
                                                                    │
                                    [sqlite_to_postgres.py]──► Zeabur PostgreSQL
                                           │
                                    [mcp_server.py]──► AI queries (MCP)
```

## File Structure

```
wechat-pipeline/
├── decrypt/                         # Key extraction + DB decryption
│   ├── find_key_memscan.py          # Extract keys from WeChat process memory
│   ├── decrypt_db.py                # Decrypt all DBs using extracted keys
│   ├── verify_keys.py               # Verify key correctness
│   ├── mcp_server.py                # FastMCP server for AI queries
│   ├── wechat-decrypt-hourly.sh     # Called by LaunchAgent
│   └── wechat_keys.json             # [gitignored] Extracted keys (sensitive!)
│   └── decrypted/                   # [gitignored] Decrypted SQLite files
│
└── sync/
    ├── sqlite_to_postgres.py        # ★ Main sync script (SQLite → Postgres)
    ├── explore_schema.py            # One-time schema inspector
    ├── prisma/schema.prisma         # PostgreSQL schema definition
    └── sync_state.json              # [gitignored] Incremental sync state
```

---

## First-Time Setup (New Machine)

### 1. Prerequisites

```bash
# Install Homebrew if not present
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Required tools
brew install llvm sqlcipher python@3.14

# Python deps (use brew python, NOT system python)
/opt/homebrew/bin/pip3 install psycopg2-binary zstandard fastmcp --break-system-packages
```

### 2. Clone this repo

```bash
mkdir -p ~/codebase
cd ~/codebase
git clone git@github.com:lycfyi/wechat-pipeline.git

# Also clone as working dirs (LaunchAgents reference these paths)
cp -r wechat-pipeline/decrypt wechat-db-decrypt-macos
mkdir -p personal/wechat_group_sync/personal_workflow
cp -r wechat-pipeline/sync/. personal/wechat_group_sync/personal_workflow/
```

### 3. Disable SIP (required for lldb key extraction)

> ⚠️ Only needed for key extraction. Re-enable after step 4 is done.

1. Shut down Mac completely
2. Hold power button until "Loading startup options" appears
3. Select Options → Continue → open Terminal
4. Run: `csrutil disable`
5. Reboot normally

### 4. Extract WeChat encryption keys

WeChat must be **running and logged in**:

```bash
cd ~/codebase/wechat-db-decrypt-macos

# Run key extraction via lldb
echo "script exec(open('find_key_memscan.py').read())" > /tmp/run_memscan.lldb
sudo /opt/homebrew/opt/llvm/bin/lldb --source /tmp/run_memscan.lldb
```

Keys saved to `wechat_keys.json`. Verify:

```bash
python3 verify_keys.py
```

Expected: all databases show ✅ (some non-critical ones like `solitaire.db` may fail — OK).

### 5. Re-enable SIP

1. Boot into Recovery Mode again
2. Run: `csrutil enable`
3. Reboot

### 6. First decryption

```bash
cd ~/codebase/wechat-db-decrypt-macos
/opt/homebrew/bin/python3 decrypt_db.py
```

Decrypted files written to `decrypted/` (~3–4 GB). Verify:

```bash
sqlite3 decrypted/contact/contact.db "SELECT count(*) FROM contact"
# Should return a large number (thousands of contacts)
```

### 7. Set up hourly decrypt LaunchAgent

```bash
# Create LaunchAgent
cat > ~/Library/LaunchAgents/com.velocity1.wechat.decrypt.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.velocity1.wechat.decrypt</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>/Users/velocity1/codebase/wechat-db-decrypt-macos/wechat-decrypt-hourly.sh</string>
    </array>
    <key>StartInterval</key>
    <integer>3600</integer>
    <key>StandardOutPath</key>
    <string>/tmp/wechat-decrypt.out</string>
    <key>StandardErrorPath</key>
    <string>/tmp/wechat-decrypt.err</string>
    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>
EOF

launchctl load ~/Library/LaunchAgents/com.velocity1.wechat.decrypt.plist
```

### 8. Set up MCP server (always-on, for AI queries)

```bash
cat > ~/Library/LaunchAgents/com.velocity1.wechat.mcp.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.velocity1.wechat.mcp</string>
    <key>ProgramArguments</key>
    <array>
        <string>/opt/homebrew/bin/python3</string>
        <string>/Users/velocity1/codebase/wechat-db-decrypt-macos/mcp_server.py</string>
    </array>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/wechat-mcp.out</string>
    <key>StandardErrorPath</key>
    <string>/tmp/wechat-mcp.err</string>
</dict>
</plist>
EOF

launchctl load ~/Library/LaunchAgents/com.velocity1.wechat.mcp.plist
```

### 9. Configure PostgreSQL sync

Edit `sync/sqlite_to_postgres.py` — update these constants at the top if needed:

```python
DECRYPTED_DIR = Path.home() / "codebase/wechat-db-decrypt-macos/decrypted"
POSTGRES_DSN  = "postgresql://root:<password>@sjc1.clusters.zeabur.com:30929/postgres"
SELF_WXID     = "leon-eternity"   # from db_storage directory name
```

Run a dry-run first to validate:

```bash
cd ~/codebase/personal/wechat_group_sync/personal_workflow
/opt/homebrew/bin/python3 sqlite_to_postgres.py --dry-run --limit 100 --db message_0.db
```

Should see: `167 Msg_* tables`, messages listed per table, no errors.

First real sync (limited test):

```bash
/opt/homebrew/bin/python3 sqlite_to_postgres.py --limit 50 --db message_0.db
```

Full initial sync (will take 10–30 min):

```bash
nohup /opt/homebrew/bin/python3 sqlite_to_postgres.py > /tmp/wechat-sync-full.log 2>&1 &
tail -f /tmp/wechat-sync-full.log
```

### 10. Set up daily sync LaunchAgent

```bash
cat > ~/Library/LaunchAgents/com.velocity1.wechat.sync.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.velocity1.wechat.sync</string>
    <key>ProgramArguments</key>
    <array>
        <string>/opt/homebrew/bin/python3</string>
        <string>/Users/velocity1/codebase/personal/wechat_group_sync/personal_workflow/sqlite_to_postgres.py</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>7</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>/tmp/wechat-sync.out</string>
    <key>StandardErrorPath</key>
    <string>/tmp/wechat-sync.err</string>
    <key>RunAtLoad</key>
    <false/>
    <key>EnvironmentVariables</key>
    <dict>
        <key>HOME</key>
        <string>/Users/velocity1</string>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
EOF

launchctl load ~/Library/LaunchAgents/com.velocity1.wechat.sync.plist
```

---

## Daily Operations

### Check LaunchAgent status

```bash
launchctl list | grep wechat
# Should show: com.velocity1.wechat.decrypt / .sync / .mcp
```

### Monitor logs

```bash
tail -f /tmp/wechat-decrypt.out    # hourly decrypt
tail -f /tmp/wechat-sync.out       # daily Postgres sync
tail -f /tmp/wechat-mcp.out        # MCP server
```

### Manual sync

```bash
/opt/homebrew/bin/python3 ~/codebase/personal/wechat_group_sync/personal_workflow/sqlite_to_postgres.py
```

### Re-extract keys (after WeChat update)

Keys are tied to the running process — re-extract whenever WeChat updates:

```bash
# Disable SIP (Recovery Mode), then:
echo "script exec(open('find_key_memscan.py').read())" > /tmp/run_memscan.lldb
sudo /opt/homebrew/opt/llvm/bin/lldb --source /tmp/run_memscan.lldb
# Re-enable SIP after extraction
```

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `lldb: command not found` | llvm not installed | `brew install llvm` |
| Key extraction fails | SIP enabled | Disable SIP (Recovery Mode) |
| `incorrect decryption key` | WeChat updated, old keys | Re-extract keys |
| `chatlog` tool not working | chatlog doesn't support WeChat 4.1.x | Use `decrypt_db.py` instead |
| Postgres: `integer out of range` | uint64 server_id overflow | Fixed in `sqlite_to_postgres.py` via `safe_int64()` |
| Postgres: `NUL character` | Binary content in message | Fixed via `sanitize_str()` |
| Postgres: `column additional_data does not exist` | Column is `"additionalData"` (no snake_case mapping) | Fixed in INSERT SQL |
| Sync is slow | First run, millions of messages | Normal — use `nohup` and wait |
| `decrypted/` missing | decrypt LaunchAgent not run yet | Run `python3 decrypt_db.py` manually |

---

## Schema Notes (WeChat 4.1.x)

WeChat 4.1.x changed DB schema significantly vs 4.0.x:

- **Messages**: No longer in a single `Chat` table. Each contact/group has its own `Msg_<MD5(wxid)>` table inside `message_0.db` through `message_13.db`
- **source field**: Zstandard-compressed binary (not plain XML). Use `zstandard` library to decompress
- **Group sender**: Embedded as `wxid_xxx:\ncontent` prefix in `message_content`
- **Keys**: Per-database keys (not one master key). `chatlog` tool's single-key approach doesn't work

---

## PostgreSQL Schema (Zeabur)

```
Connection: postgresql://root:<pw>@sjc1.clusters.zeabur.com:30929/postgres
```

```sql
-- chat_rooms: WeChat contacts + groups
id VARCHAR(500) PK, name VARCHAR(500), "isChatRoom" BOOLEAN

-- users: individual WeChat accounts  
id VARCHAR(500) PK, name VARCHAR(500)

-- messages
seq BIGINT PK,           -- server_id from WeChat
time TIMESTAMP,
content TEXT,
type INT,                -- local_type (1=text, 3=image, 34=voice, 43=video...)
sub_type INT,
is_self BOOLEAN,
talker_id VARCHAR(500),  -- group wxid or contact wxid
sender_id VARCHAR(500),  -- actual sender in group chats
"additionalData" JSONB   -- source XML + packed_info for non-text messages
```
