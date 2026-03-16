#!/usr/bin/env python3
"""
Quick schema explorer — run on kusanagi to verify DB structures.
Usage: python3 explore_schema.py
"""
import sqlite3
from pathlib import Path

DECRYPTED = Path.home() / "codebase/wechat-db-decrypt-macos/decrypted"

def show_table_schema(conn, table):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info([{table}])")
    cols = cur.fetchall()
    print(f"\n  [{table}]")
    for col in cols:
        print(f"    {col[1]:<25} {col[2]}")

def show_sample(conn, table, limit=3):
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM [{table}] LIMIT {limit}")
        rows = cur.fetchall()
        if rows:
            print(f"    Sample ({limit} rows):")
            for row in rows:
                print(f"      {dict(row)}")
    except Exception as e:
        print(f"    Sample failed: {e}")

print("=" * 60)
print("WeChat DB Schema Explorer")
print("=" * 60)

# Contact DB
contact_db = DECRYPTED / "contact" / "contact.db"
print(f"\n📁 {contact_db}")
if contact_db.exists():
    conn = sqlite3.connect(f"file:{contact_db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    print(f"  Tables: {tables}")
    for t in tables[:3]:
        show_table_schema(conn, t)
        show_sample(conn, t, 2)
    conn.close()

# Session DB
session_db = DECRYPTED / "session" / "session.db"
print(f"\n📁 {session_db}")
if session_db.exists():
    conn = sqlite3.connect(f"file:{session_db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    print(f"  Tables: {tables}")
    for t in tables[:3]:
        show_table_schema(conn, t)
        show_sample(conn, t, 2)
    conn.close()

# Message DB 0
msg_db = DECRYPTED / "message" / "message_0.db"
print(f"\n📁 {msg_db}")
if msg_db.exists():
    conn = sqlite3.connect(f"file:{msg_db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    msg_tables = [t for t in tables if t.startswith("Msg_")]
    other_tables = [t for t in tables if not t.startswith("Msg_")]
    print(f"  Non-Msg tables: {other_tables}")
    print(f"  Msg_* tables: {len(msg_tables)} total, first 3: {msg_tables[:3]}")

    for t in other_tables:
        show_table_schema(conn, t)
        show_sample(conn, t, 3)

    if msg_tables:
        print(f"\n  First Msg_* table schema ({msg_tables[0]}):")
        show_table_schema(conn, msg_tables[0])
        show_sample(conn, msg_tables[0], 2)
    conn.close()

print("\n" + "=" * 60)
print("Done")
