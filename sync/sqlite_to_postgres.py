#!/usr/bin/env python3
"""
WeChat SQLite → Zeabur PostgreSQL sync
Reads decrypted WeChat 4.1.x SQLite DBs and upserts to PostgreSQL.

Usage:
  python3 sqlite_to_postgres.py                # full incremental sync
  python3 sqlite_to_postgres.py --limit 100   # test: 100 msgs per table
  python3 sqlite_to_postgres.py --dry-run     # parse only, no writes
  python3 sqlite_to_postgres.py --db message_0.db  # single DB
"""

import os
import sys
import json
import zlib
import sqlite3
import hashlib
import logging
import argparse
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Tuple, List, Set
import xml.etree.ElementTree as ET

# zstd is optional — install via: pip3 install zstandard
try:
    import zstandard as zstd
    _ZSTD_AVAILABLE = True
except ImportError:
    _ZSTD_AVAILABLE = False

# ─── Configuration ─────────────────────────────────────────────────────────────
DECRYPTED_DIR = Path.home() / "codebase/wechat-db-decrypt-macos/decrypted"
SCRIPT_DIR = Path(__file__).parent
STATE_FILE = SCRIPT_DIR / "sync_state.json"
POSTGRES_DSN = "postgresql://root:gmu4K8wEY2efGP5k90il1VX7I3T6JLBh@sjc1.clusters.zeabur.com:30929/postgres"

# Self wxid — extracted from DB directory name "leon-eternity_3758"
SELF_WXID = "leon-eternity"
SELF_WXID_FULL = "leon-eternity_3758"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─── Dependency check ──────────────────────────────────────────────────────────
def ensure_psycopg2():
    try:
        import psycopg2
        import psycopg2.extras
        return psycopg2
    except ImportError:
        log.info("psycopg2 not found — installing via pip3 …")
        import subprocess
        subprocess.run(
            ["/opt/homebrew/bin/pip3", "install", "psycopg2-binary",
             "--break-system-packages"],
            check=True,
        )
        import psycopg2
        import psycopg2.extras
        return psycopg2


# ─── State management ──────────────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ─── Content helpers ───────────────────────────────────────────────────────────
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"

def decompress_blob(data) -> str:
    """Decompress content blob (zstd or zlib) → UTF-8 string."""
    if not data:
        return ""
    if isinstance(data, memoryview):
        data = bytes(data)
    if isinstance(data, str):
        return data
    # zstd (WeChat 4.1.x uses zstd for source field)
    if data[:4] == ZSTD_MAGIC and _ZSTD_AVAILABLE:
        try:
            return zstd.ZstdDecompressor().decompress(data).decode("utf-8", errors="replace")
        except Exception:
            pass
    # zlib
    try:
        return zlib.decompress(data).decode("utf-8", errors="replace")
    except Exception:
        try:
            return zlib.decompress(data, -15).decode("utf-8", errors="replace")
        except Exception:
            return ""


def decode_source(source) -> str:
    """Decode source field: may be bytes (zstd/zlib compressed) or plain str."""
    if not source:
        return ""
    if isinstance(source, (bytes, memoryview)):
        raw = bytes(source) if isinstance(source, memoryview) else source
        if raw[:4] == ZSTD_MAGIC:
            return decompress_blob(raw)
        # try zlib
        result = decompress_blob(raw)
        if result:
            return result
        # fallback: try utf-8 decode directly
        try:
            return raw.decode("utf-8", errors="replace")
        except Exception:
            return ""
    return str(source)


def parse_source_xml(source: Optional[str]) -> Tuple[int, Optional[str]]:
    """
    Parse WeChat `source` XML field.
    Returns: (sub_type:int, fromusername:str|None)
    Uses regex fast-path (XML in source is often malformed).
    """
    sub_type = 0
    fromusername = None
    if not source or not source.strip():
        return sub_type, fromusername

    # sub_type
    m = re.search(r"<subtype>\s*(\d+)\s*</subtype>", source, re.IGNORECASE)
    if m:
        try:
            sub_type = int(m.group(1))
        except ValueError:
            pass

    # fromusername (real sender in group messages)
    m = re.search(r"<fromusername>\s*(.*?)\s*</fromusername>",
                  source, re.IGNORECASE | re.DOTALL)
    if m:
        fromusername = m.group(1).strip()
        if not fromusername:
            fromusername = None

    return sub_type, fromusername


def sender_from_content_prefix(content: str) -> Optional[str]:
    """
    Some group-chat messages embed sender as first line:
      "wxid_abc123:\nHello world"
    Returns the wxid prefix, or None.
    """
    if not content:
        return None
    nl = content.find("\n")
    if nl < 0:
        return None
    first = content[:nl].strip()
    if first.endswith(":") and 3 < len(first) < 80:
        candidate = first[:-1]
        # Must look like a valid wxid (no spaces, reasonable charset)
        if re.match(r"^[A-Za-z0-9_\-@\.]+$", candidate):
            return candidate
    return None


def md5_hex(s: str) -> str:
    """MD5 of lowercased string → hex digest."""
    return hashlib.md5(s.lower().encode()).hexdigest()


# ─── Data loaders ──────────────────────────────────────────────────────────────
def load_contacts(contact_db: Path) -> Dict[int, dict]:
    """
    Load contact table → {contact_id: {id, username, nick_name, remark, alias}}
    Handles column name variations between WeChat versions.
    """
    contacts: Dict[int, dict] = {}
    if not contact_db.exists():
        log.warning(f"contact.db not found: {contact_db}")
        return contacts
    try:
        conn = sqlite3.connect(f"file:{contact_db}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("PRAGMA table_info(contact)")
        raw_cols = [row[1] for row in cur.fetchall()]
        log.info(f"contact table columns: {raw_cols}")
        cols_lower = {c.lower(): c for c in raw_cols}

        def col(names):
            for n in names:
                if n in cols_lower:
                    return cols_lower[n]
            return None

        id_col      = col(["id", "local_id", "localid"])
        uname_col   = col(["username", "UserName", "user_name"])
        nick_col    = col(["nick_name", "NickName", "nickname"])
        remark_col  = col(["remark", "Remark"])
        alias_col   = col(["alias", "Alias"])

        if not id_col or not uname_col:
            log.error("contact table missing required columns")
            conn.close()
            return contacts

        select = ", ".join(filter(None, [id_col, uname_col, nick_col, remark_col, alias_col]))
        cur.execute(f"SELECT {select} FROM contact")
        for row in cur.fetchall():
            row = dict(row)
            cid = row.get(id_col)
            if cid is None:
                continue
            contacts[cid] = {
                "id":        cid,
                "username":  (row.get(uname_col)  or "").strip(),
                "nick_name": (row.get(nick_col)   or "").strip() if nick_col else "",
                "remark":    (row.get(remark_col) or "").strip() if remark_col else "",
                "alias":     (row.get(alias_col)  or "").strip() if alias_col else "",
            }
        conn.close()
        log.info(f"Loaded {len(contacts)} contacts")
    except Exception as e:
        log.error(f"Failed to load contacts: {e}", exc_info=True)
    return contacts


def contacts_by_username(contacts: Dict[int, dict]) -> Dict[str, dict]:
    return {c["username"]: c for c in contacts.values() if c["username"]}


def load_sessions(session_db: Path) -> Dict[str, str]:
    """Load SessionTable → {wxid: display_name}"""
    sessions: Dict[str, str] = {}
    if not session_db.exists():
        log.warning(f"session.db not found: {session_db}")
        return sessions
    try:
        conn = sqlite3.connect(f"file:{session_db}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("PRAGMA table_info(SessionTable)")
        raw_cols = [row[1] for row in cur.fetchall()]
        log.info(f"SessionTable columns: {raw_cols}")
        cols_lower = {c.lower(): c for c in raw_cols}

        username_col = next((cols_lower[k] for k in
                             ("strusrname", "username", "wxid", "user_name")
                             if k in cols_lower), None)
        nick_col = next((cols_lower[k] for k in
                         ("strnickname", "nickname", "nick_name", "displayname", "remark")
                         if k in cols_lower), None)

        if not username_col:
            log.warning("SessionTable: could not identify username column")
            conn.close()
            return sessions

        if nick_col:
            cur.execute(f'SELECT "{username_col}", "{nick_col}" FROM SessionTable')
            for row in cur.fetchall():
                if row[0]:
                    sessions[row[0]] = row[1] or row[0]
        else:
            cur.execute(f'SELECT "{username_col}" FROM SessionTable')
            for row in cur.fetchall():
                if row[0]:
                    sessions[row[0]] = row[0]

        conn.close()
        log.info(f"Loaded {len(sessions)} sessions")
    except Exception as e:
        log.error(f"Failed to load sessions: {e}", exc_info=True)
    return sessions


def load_name2id(msg_db: Path) -> Dict[str, str]:
    """
    Build {md5_hash_lower → wxid} from the Name2Id table in a message DB.
    MD5(wxid.lower()) == the hash suffix in Msg_<hash> table names.
    """
    mapping: Dict[str, str] = {}
    try:
        conn = sqlite3.connect(f"file:{msg_db}?mode=ro", uri=True)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Name2Id'")
        if not cur.fetchone():
            conn.close()
            log.debug(f"{msg_db.name}: no Name2Id table")
            return mapping

        cur.execute("PRAGMA table_info(Name2Id)")
        cols = [row[1] for row in cur.fetchall()]
        log.debug(f"{msg_db.name} Name2Id columns: {cols}")

        # Find username column
        un_col = next((c for c in cols if c.lower() == "username"), None)
        if not un_col and cols:
            un_col = cols[0]
        if not un_col:
            conn.close()
            return mapping

        cur.execute(f'SELECT "{un_col}" FROM Name2Id')
        for (wxid,) in cur.fetchall():
            if wxid:
                h = md5_hex(str(wxid))
                mapping[h] = wxid

        conn.close()
        log.debug(f"{msg_db.name}: {len(mapping)} Name2Id entries")
    except Exception as e:
        log.debug(f"Name2Id load failed {msg_db.name}: {e}")
    return mapping


# ─── Postgres helpers ──────────────────────────────────────────────────────────
def upsert_chat_room(cur, wxid: str, name: str, is_chat_room: bool):
    now = datetime.utcnow()
    cur.execute(
        '''INSERT INTO chat_rooms (id, name, "isChatRoom", "createdAt", "updatedAt")
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (id) DO NOTHING''',
        (wxid[:500], (name or wxid)[:500], is_chat_room, now, now),
    )


def upsert_user(cur, wxid: str, name: str):
    now = datetime.utcnow()
    cur.execute(
        '''INSERT INTO users (id, name, "createdAt", "updatedAt")
           VALUES (%s, %s, %s, %s)
           ON CONFLICT (id) DO NOTHING''',
        (wxid[:500], (name or wxid)[:500], now, now),
    )


def sanitize_str(s: Optional[str], max_len: int = 50000) -> str:
    """Strip null bytes and truncate for Postgres compatibility."""
    if not s:
        return ""
    if isinstance(s, bytes):
        s = s.decode("utf-8", errors="replace")
    # Postgres TEXT cannot contain null bytes
    return s.replace("\x00", "")[:max_len]


INT32_MAX = 2_147_483_647
INT64_MAX = 9_223_372_036_854_775_807

def safe_int32(v) -> int:
    """Clamp to Postgres INT range."""
    try:
        return max(-INT32_MAX - 1, min(INT32_MAX, int(v)))
    except Exception:
        return 0

def safe_int64(v) -> int:
    """Convert uint64 → signed int64 if needed, clamp for Postgres BIGINT."""
    try:
        n = int(v)
        if n > INT64_MAX:
            n -= (1 << 64)  # reinterpret as signed
        return n
    except Exception:
        return 0


def insert_message(cur, msg: dict) -> bool:
    """Insert one message row. Returns True if a new row was inserted."""
    now = datetime.utcnow()  # noqa: psycopg2.extras not needed for basic ops
    try:
        # Prisma field `additionalData` has no @map(), so actual column is "additionalData"
        cur.execute(
            '''INSERT INTO messages
               (seq, time, content, type, sub_type, is_self,
                talker_id, sender_id, "additionalData", "createdAt", "updatedAt")
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (seq) DO NOTHING''',
            (
                safe_int64(msg["seq"]),
                msg["time"],
                sanitize_str(msg["content"]),
                safe_int32(msg["type"]),
                safe_int32(msg["sub_type"]),
                msg["is_self"],
                sanitize_str(msg["talker_id"] or "", 500),
                sanitize_str(msg["sender_id"] or "", 500),
                json.dumps(msg["additional_data"]) if msg["additional_data"] else None,
                now,
                now,
            ),
        )
        return cur.rowcount > 0
    except Exception as e:
        log.error(f"insert_message seq={msg.get('seq')}: {e}")
        cur.execute("ROLLBACK TO SAVEPOINT sp_msg")  # recover transaction
        return False


# ─── Core sync logic ───────────────────────────────────────────────────────────
def sync_db(
    msg_db: Path,
    contacts: Dict[int, dict],
    contacts_un: Dict[str, dict],
    sessions: Dict[str, str],
    pg_conn,
    state: dict,
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> Tuple[int, int]:
    """
    Sync all Msg_<hash> tables from one message_N.db.
    Returns (total_processed, total_inserted).
    """
    db_key = msg_db.name
    total_processed = 0
    total_inserted = 0

    try:
        conn = sqlite3.connect(f"file:{msg_db}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Enumerate message tables
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Msg_%'")
        tables = [row[0] for row in cur.fetchall()]
        log.info(f"{msg_db.name}: {len(tables)} Msg_* tables")

        # Load hash→wxid mapping
        hash_to_wxid = load_name2id(msg_db)
        if not hash_to_wxid:
            log.warning(f"{msg_db.name}: Name2Id empty or missing — skipping")
            conn.close()
            return 0, 0

        pg_cur = None if dry_run else pg_conn.cursor()

        # Cache self contact id
        self_contact = contacts_un.get(SELF_WXID) or contacts_un.get(SELF_WXID_FULL)
        self_contact_id = self_contact["id"] if self_contact else None

        for table_name in tables:
            hash_part = table_name[4:].lower()  # strip "Msg_"
            wxid = hash_to_wxid.get(hash_part)
            if not wxid:
                log.debug(f"  No wxid for {table_name} (hash={hash_part[:8]}…)")
                continue

            is_chat_room = wxid.endswith("@chatroom")
            talker_id = wxid
            display_name = sessions.get(wxid, wxid)

            # Upsert talker into chat_rooms
            if not dry_run:
                try:
                    upsert_chat_room(pg_cur, wxid, display_name, is_chat_room)
                except Exception as e:
                    log.debug(f"  upsert_chat_room {wxid}: {e}")
                    pg_conn.rollback()

            table_key = f"{db_key}::{table_name}"
            last_time = state.get(table_key, 0)

            # Detect available columns in this table
            cur.execute(f"PRAGMA table_info([{table_name}])")
            avail_cols: Set[str] = {row[1] for row in cur.fetchall()}

            want_cols = ["local_id", "server_id", "local_type", "real_sender_id",
                         "create_time", "message_content", "compress_content",
                         "source", "packed_info_data"]
            select_cols = [c for c in want_cols if c in avail_cols]

            query = (
                f"SELECT {', '.join(select_cols)}"
                f" FROM [{table_name}]"
                f" WHERE server_id IS NOT NULL AND server_id != 0"
                f"   AND create_time > ?"
                f" ORDER BY create_time ASC"
            )
            if limit:
                query += f" LIMIT {limit}"

            try:
                cur.execute(query, [last_time])
                rows = cur.fetchall()
            except Exception as e:
                log.error(f"  Query {table_name}: {e}")
                continue

            max_time = last_time
            table_inserted = 0

            for row in rows:
                row = dict(row)
                server_id = row.get("server_id")
                if not server_id:
                    continue

                local_type   = row.get("local_type") or 0
                create_time  = row.get("create_time") or 0
                real_sender_id = row.get("real_sender_id")
                # source may be zstd-compressed bytes in WeChat 4.1.x
                source_xml = decode_source(row.get("source"))

                # ── Content ─────────────────────────────────────────────────
                raw_content = row.get("message_content") or ""
                if isinstance(raw_content, (bytes, memoryview)):
                    raw_content = bytes(raw_content).decode("utf-8", errors="replace")
                content = raw_content
                if not content and row.get("compress_content"):
                    content = decompress_blob(row["compress_content"])

                # ── Parse source XML ─────────────────────────────────────────
                sub_type, source_from = parse_source_xml(source_xml)

                # ── Determine sender wxid ─────────────────────────────────────
                sender_wxid: Optional[str] = None

                if is_chat_room:
                    # Priority 1: fromusername in source XML
                    if source_from:
                        sender_wxid = source_from
                    # Priority 2: content prefix "wxid_xxx:\n..."
                    if not sender_wxid:
                        sender_wxid = sender_from_content_prefix(content)
                    # Priority 3: real_sender_id → contact → username
                    if not sender_wxid and real_sender_id:
                        c = contacts.get(real_sender_id)
                        if c:
                            sender_wxid = c["username"]
                else:
                    # 1-on-1: real_sender_id → contact → username
                    if real_sender_id:
                        c = contacts.get(real_sender_id)
                        if c:
                            sender_wxid = c["username"]
                    # fallback: the other party
                    if not sender_wxid:
                        sender_wxid = wxid

                # ── Strip sender prefix from content for group messages ──────
                # Format: "wxid_xxx:\nactual message content"
                if is_chat_room and content:
                    nl = content.find("\n")
                    if nl > 0:
                        first_line = content[:nl].strip()
                        if first_line.endswith(":") and re.match(r"^[A-Za-z0-9_\-@\.]+$", first_line[:-1]):
                            content = content[nl+1:]

                # ── is_self ──────────────────────────────────────────────────
                if sender_wxid:
                    is_self = (
                        sender_wxid == SELF_WXID
                        or sender_wxid == SELF_WXID_FULL
                        or sender_wxid.startswith(SELF_WXID + "_")
                    )
                else:
                    # No sender resolved — check if real_sender_id matches self
                    is_self = bool(
                        self_contact_id is not None
                        and real_sender_id == self_contact_id
                    )

                # ── Ensure sender in users table ──────────────────────────────
                if sender_wxid and not dry_run:
                    c = contacts_un.get(sender_wxid)
                    name = ""
                    if c:
                        name = c["remark"] or c["nick_name"] or sender_wxid
                    try:
                        upsert_user(pg_cur, sender_wxid, name)
                    except Exception:
                        pass  # non-fatal

                # ── Additional data for non-text messages ────────────────────
                additional_data: Optional[dict] = None
                if local_type != 1:
                    additional_data = {}
                    if source_xml:
                        additional_data["source"] = source_xml[:5000]
                    if row.get("packed_info_data"):
                        try:
                            additional_data["packed_info_data"] = (
                                bytes(row["packed_info_data"]).hex()[:2000]
                            )
                        except Exception:
                            pass
                    if not additional_data:
                        additional_data = None

                # ── Build message dict ────────────────────────────────────────
                msg = {
                    "seq":             server_id,
                    "time":            (datetime.utcfromtimestamp(create_time)
                                        if create_time else datetime.utcnow()),
                    "content":         content,
                    "type":            local_type,
                    "sub_type":        sub_type,
                    "is_self":         is_self,
                    "talker_id":       talker_id,
                    "sender_id":       sender_wxid or talker_id,
                    "additional_data": additional_data,
                }

                if not dry_run:
                    pg_cur.execute("SAVEPOINT sp_msg")
                    ok = insert_message(pg_cur, msg)
                    if ok:
                        table_inserted += 1
                        total_inserted += 1
                else:
                    table_inserted += 1
                    total_inserted += 1

                if create_time > max_time:
                    max_time = create_time
                total_processed += 1

            # ── Commit per table ──────────────────────────────────────────────
            if not dry_run:
                try:
                    pg_conn.commit()
                except Exception as e:
                    log.error(f"  Commit failed {table_name}: {e}")
                    pg_conn.rollback()

            # ── Update state ──────────────────────────────────────────────────
            if max_time > last_time:
                state[table_key] = max_time

            if table_inserted > 0:
                log.info(f"  ✓ {table_name} ({wxid}): +{table_inserted} msgs")

        conn.close()
    except Exception as e:
        log.error(f"sync_db failed for {msg_db}: {e}", exc_info=True)

    return total_processed, total_inserted


# ─── Entry point ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="WeChat SQLite → PostgreSQL incremental sync"
    )
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit rows per Msg_* table (for testing)")
    parser.add_argument("--db", type=str, default=None,
                        help="Process only one DB file, e.g. message_0.db")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse & log without writing to Postgres")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("WeChat SQLite → PostgreSQL Sync")
    log.info(f"  Decrypted dir : {DECRYPTED_DIR}")
    log.info(f"  State file    : {STATE_FILE}")
    log.info(f"  Self wxid     : {SELF_WXID}")
    log.info(f"  Dry run       : {args.dry_run}")
    log.info("=" * 60)

    psycopg2 = ensure_psycopg2()

    state = load_state()

    # Load reference data
    contacts   = load_contacts(DECRYPTED_DIR / "contact" / "contact.db")
    contacts_un = contacts_by_username(contacts)
    sessions   = load_sessions(DECRYPTED_DIR / "session" / "session.db")

    # Find message DBs
    message_dir = DECRYPTED_DIR / "message"
    if args.db:
        msg_dbs = [message_dir / args.db]
    else:
        msg_dbs = sorted(
            [f for f in message_dir.glob("message_*.db")
             if re.fullmatch(r"message_\d+\.db", f.name) and f.is_file()],
            key=lambda p: int(re.search(r"(\d+)", p.stem).group(1)),
        )

    log.info(f"Message DBs: {len(msg_dbs)}")

    # Connect to Postgres
    if not args.dry_run:
        log.info("Connecting to PostgreSQL …")
        pg_conn = psycopg2.connect(POSTGRES_DSN)
        pg_conn.autocommit = False
        log.info("PostgreSQL connected ✓")
    else:
        pg_conn = None
        log.info("DRY RUN — no Postgres writes")

    # Main sync loop
    total_processed = 0
    total_inserted  = 0

    for db in msg_dbs:
        if not db.exists():
            log.warning(f"Missing DB: {db}")
            continue
        log.info(f"\n▶ Processing {db.name} …")
        p, i = sync_db(
            db, contacts, contacts_un, sessions,
            pg_conn, state,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        total_processed += p
        total_inserted  += i
        log.info(f"  {db.name}: processed={p}  inserted={i}")
        save_state(state)  # checkpoint after each DB

    if pg_conn:
        pg_conn.close()

    log.info(f"\n{'=' * 60}")
    log.info(f"DONE — total processed={total_processed}  inserted={total_inserted}")
    log.info(f"State saved → {STATE_FILE}")


if __name__ == "__main__":
    main()
