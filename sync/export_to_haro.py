#!/usr/bin/env python3
"""
Export 出海去 + XBC/BoostClub chatrooms from PostgreSQL to JSON files.
Output: ~/clawd-velocity1/wechat-groups/<group_name>/<year>/chatlog_YYYY-MM.json

Usage: python3 export_to_haro.py [--output-dir /path/to/output]
"""

import os
import sys
import json
import re
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("psycopg2 not found. Run: pip install psycopg2-binary")
    sys.exit(1)

DB_URL = "postgresql://root:gmu4K8wEY2efGP5k90il1VX7I3T6JLBh@sjc1.clusters.zeabur.com:30929/postgres"

WHERE_CLAUSE = """
    cr.name LIKE '%出海去%' 
    OR cr.name LIKE '%XBC%' 
    OR cr.name LIKE '%BoostClub%'
"""

def safe_dirname(name: str) -> str:
    """Convert chatroom name to a safe directory name."""
    # Replace filesystem-unsafe characters
    name = name.strip()
    # Replace slashes, colons, null bytes etc
    name = re.sub(r'[/\\:*?"<>|\x00]', '_', name)
    # Collapse multiple spaces/underscores
    name = re.sub(r'[\s_]+', '_', name)
    name = name.strip('_')
    return name or 'unnamed'

def parse_db_url(url: str) -> dict:
    """Parse a postgres URL into connection kwargs."""
    import urllib.parse
    result = urllib.parse.urlparse(url)
    return {
        'host': result.hostname,
        'port': result.port,
        'user': result.username,
        'password': result.password,
        'dbname': result.path.lstrip('/'),
    }

def main():
    parser = argparse.ArgumentParser(description='Export WeChat groups to Haro workspace')
    parser.add_argument('--output-dir', default=os.path.expanduser('~/wechat-groups-export'),
                        help='Output directory (default: ~/wechat-groups-export)')
    parser.add_argument('--dry-run', action='store_true', help='Just list chatrooms, do not export')
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    conn_kwargs = parse_db_url(DB_URL)
    print(f"Connecting to {conn_kwargs['host']}:{conn_kwargs['port']}...")
    conn = psycopg2.connect(**conn_kwargs)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # 1. Get matching chatrooms
    cur.execute(f"""
        SELECT cr.id, cr.name,
               (SELECT COUNT(*) FROM messages m WHERE m.talker_id = cr.id) as msg_count
        FROM chat_rooms cr
        WHERE {WHERE_CLAUSE}
        ORDER BY cr.name
    """)
    rooms = cur.fetchall()
    print(f"\nFound {len(rooms)} chatrooms matching filter")

    if args.dry_run:
        for r in rooms:
            print(f"  [{r['msg_count']:>6}] {r['name']}")
        conn.close()
        return

    total_msgs = 0
    total_files = 0

    for i, room in enumerate(rooms):
        room_id = room['id']
        room_name = room['name']
        msg_count = room['msg_count']
        safe_name = safe_dirname(room_name)

        print(f"\n[{i+1}/{len(rooms)}] {room_name} ({msg_count} msgs)")

        # 2. Fetch messages for this room with sender name
        cur.execute("""
            SELECT 
                m.seq,
                m.time,
                m.content,
                m.type,
                m.sub_type,
                m.is_self,
                m.sender_id,
                COALESCE(u.name, m.sender_id, '') as sender_name,
                m."additionalData" as additional_data
            FROM messages m
            LEFT JOIN users u ON u.id = m.sender_id
            WHERE m.talker_id = %s
            ORDER BY m.time ASC
        """, (room_id,))
        messages = cur.fetchall()

        # 3. Group by year-month
        by_month = defaultdict(list)
        for msg in messages:
            t = msg['time']
            if isinstance(t, datetime):
                ym = t.strftime('%Y-%m')
                year = t.strftime('%Y')
            else:
                ym = str(t)[:7]
                year = str(t)[:4]
            
            by_month[(year, ym)].append({
                'seq': msg['seq'],
                'time': msg['time'].isoformat() if isinstance(msg['time'], datetime) else str(msg['time']),
                'content': msg['content'],
                'type': msg['type'],
                'sub_type': msg['sub_type'],
                'is_self': msg['is_self'],
                'sender_id': msg['sender_id'],
                'sender_name': msg['sender_name'],
                'additional_data': msg['additional_data'],
            })

        # 4. Write JSON files
        for (year, ym), msgs in sorted(by_month.items()):
            year_dir = output_dir / safe_name / year
            year_dir.mkdir(parents=True, exist_ok=True)
            out_file = year_dir / f"chatlog_{ym}.json"

            data = {
                'chatroom_id': room_id,
                'chatroom_name': room_name,
                'year_month': ym,
                'message_count': len(msgs),
                'exported_at': datetime.now().isoformat(),
                'messages': msgs,
            }

            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            
            total_files += 1
            total_msgs += len(msgs)
            print(f"  → {safe_name}/{year}/chatlog_{ym}.json ({len(msgs)} msgs)")

    conn.close()

    print(f"\n✅ Done! Exported {total_msgs} messages across {total_files} files")
    print(f"   Output: {output_dir}")
    print(f"\nNext: rsync to section9:")
    print(f"   rsync -avz --delete {output_dir}/ lycfyi@192.168.68.108:~/clawd-velocity1/wechat-groups/")

if __name__ == '__main__':
    main()
