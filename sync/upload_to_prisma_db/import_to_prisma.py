#!/usr/bin/env python3
"""
Import WeChat chat data from JSON files to Prisma PostgreSQL database
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

# Note: You'll need to install prisma client: pip install prisma
from prisma import Prisma
from progress_tracker import ProgressTracker


def load_chat_data(json_file_path: str) -> Dict:
    """Load chat data from JSON file"""
    with open(json_file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def extract_chat_rooms(messages: List[Dict]) -> Dict[str, Dict]:
    """Extract unique chat rooms from messages"""
    chat_rooms = {}

    for msg in messages:
        talker_id = msg.get('talker', '')
        talker_name = msg.get('talkerName', '')
        is_chat_room = msg.get('isChatRoom', False)

        if talker_id:
            # Skip obviously malformed talker IDs (likely XML/HTML content)
            if talker_id.startswith('<') or len(talker_id) > 1000:
                print(f"⚠️  Skipping malformed talker ID (len={len(talker_id)}): {talker_id[:50]}...")
                continue

            # Truncate fields to avoid PostgreSQL index size limits
            truncated_id = talker_id[:500]

            if truncated_id not in chat_rooms:
                # Handle empty talkerName
                display_name = talker_name if talker_name else f"Unknown_{talker_id[:8]}"
                truncated_name = display_name[:500]

                chat_rooms[truncated_id] = {'id': truncated_id, 'name': truncated_name, 'isChatRoom': is_chat_room}

    return chat_rooms


def extract_users(messages: List[Dict]) -> Dict[str, Dict]:
    """Extract unique users from messages"""
    users = {}

    for msg in messages:
        sender_id = msg.get('sender', '')
        sender_name = msg.get('senderName', '')

        if sender_id:
            # Skip obviously malformed sender IDs (likely XML/HTML content)
            if sender_id.startswith('<') or len(sender_id) > 1000:
                print(f"⚠️  Skipping malformed sender ID (len={len(sender_id)}): {sender_id[:50]}...")
                continue

            # Truncate fields to avoid PostgreSQL index size limits
            truncated_id = sender_id[:500]

            if truncated_id not in users:
                # Handle empty senderName
                display_name = sender_name if sender_name else f"User_{sender_id[:8]}"
                truncated_name = display_name[:500]

                users[truncated_id] = {'id': truncated_id, 'name': truncated_name}

    return users


def prepare_messages(messages: List[Dict]) -> List[Dict]:
    """Prepare messages for database insertion"""
    prepared_messages = []

    for msg in messages:
        # Skip messages with missing required fields
        if not all(key in msg for key in ['seq', 'time']):
            print(f"⚠️  Skipping message with missing required fields: {msg.get('seq', 'unknown')}")
            continue

        # Parse datetime
        try:
            time_obj = datetime.fromisoformat(msg['time'].replace('Z', '+00:00'))
        except ValueError as e:
            print(f"⚠️  Skipping message with invalid time format: {msg.get('seq', 'unknown')} - {e}")
            continue

        # Convert empty strings to None for optional foreign keys and truncate long IDs
        talker_id = msg.get('talker', '')
        sender_id = msg.get('sender', '')

        # Skip messages with malformed IDs (likely XML/HTML content)
        if (sender_id and (sender_id.startswith('<') or len(sender_id) > 1000)) or (
            talker_id and (talker_id.startswith('<') or len(talker_id) > 1000)
        ):
            print(f"⚠️  Skipping message with malformed ID (seq={msg.get('seq', 'unknown')})")
            continue

        # Truncate IDs to match database constraints
        truncated_talker_id = talker_id[:500] if talker_id.strip() else None
        truncated_sender_id = sender_id[:500] if sender_id.strip() else None

        # Collect any additional fields that aren't part of the standard schema
        standard_fields = {
            'seq',
            'time',
            'content',
            'type',
            'subType',
            'isSelf',
            'talker',
            'sender',
            'talkerName',
            'senderName',
            'isChatRoom',
        }
        additional_data = {k: v for k, v in msg.items() if k not in standard_fields}

        prepared_msg = {
            'seq': int(msg['seq']),
            'time': time_obj,
            'content': msg.get('content', ''),
            'type': int(msg.get('type', 0)),
            'subType': int(msg.get('subType', 0)),
            'isSelf': bool(msg.get('isSelf', False)),
        }

        # Add optional foreign key fields only if they have values
        if truncated_talker_id:
            prepared_msg['talkerId'] = truncated_talker_id
        if truncated_sender_id:
            prepared_msg['senderId'] = truncated_sender_id

        # Handle additionalData: convert contents and other extra fields to JSON
        if additional_data:
            try:
                from prisma import Json

                prepared_msg['additionalData'] = Json(additional_data)
            except Exception as e:
                print(f"⚠️  Failed to convert additionalData for message {msg.get('seq', 'unknown')}: {e}")

        # Also handle contents field if it exists directly in the message
        contents = msg.get('contents')
        if contents and 'additionalData' not in prepared_msg:
            try:
                from prisma import Json

                prepared_msg['additionalData'] = Json({'contents': contents})
            except Exception as e:
                print(f"⚠️  Failed to convert contents for message {msg.get('seq', 'unknown')}: {e}")

        prepared_messages.append(prepared_msg)

    return prepared_messages


async def import_chat_data_to_db(
    json_file_path: str, batch_size: int = 100, progress_tracker: Optional[ProgressTracker] = None
):
    """
    Import chat data from JSON file to Prisma PostgreSQL database

    NOTE: This is a template function. You need to:
    1. Install prisma: pip install prisma
    2. Set up your DATABASE_URL environment variable
    3. Run: prisma generate
    4. Uncomment the Prisma imports and usage below
    """

    file_path = Path(json_file_path)

    # Check if file has already been processed successfully
    if progress_tracker and progress_tracker.is_file_processed(file_path):
        processing_info = progress_tracker.get_processing_info(file_path)
        stats = processing_info.get("processing_stats", {})
        print(f"⏭️  Skipping {file_path.name} - already processed successfully")
        print(
            f"   📊 Previous results: {stats.get('messages_count', 0)} messages, {stats.get('chat_rooms_count', 0)} rooms, {stats.get('users_count', 0)} users"
        )
        return

    # Mark processing start
    if progress_tracker:
        progress_tracker.mark_file_processing_start(file_path)

    print(f"📥 Loading data from {json_file_path}")

    try:
        data = load_chat_data(json_file_path)
        messages = data.get('messages', [])

        if not messages:
            print("❌ No messages found in file")
            if progress_tracker:
                progress_tracker.mark_file_processing_failed(file_path, "No messages found in file")
            return

        print(f"📊 Processing {len(messages)} messages...")

        # Extract entities
        chat_rooms = extract_chat_rooms(messages)
        users = extract_users(messages)
        prepared_messages = prepare_messages(messages)

        print(f"📁 Found {len(chat_rooms)} unique chat rooms")
        print(f"👤 Found {len(users)} unique users")
        print(f"💬 Prepared {len(prepared_messages)} valid messages")

        # Uncomment below when you have Prisma set up

        prisma = Prisma()
        await prisma.connect()

        try:
            # 1. Upsert ChatRooms
            print("📁 Upserting chat rooms...")
            for room_data in chat_rooms.values():
                await prisma.chatroom.upsert(
                    where={'id': room_data['id']},
                    data={
                        'create': room_data,
                        'update': {'name': room_data['name'], 'isChatRoom': room_data['isChatRoom']},
                    },
                )

            # 2. Upsert Users
            print("👤 Upserting users...")
            for user_data in users.values():
                await prisma.user.upsert(
                    where={'id': user_data['id']}, data={'create': user_data, 'update': {'name': user_data['name']}}
                )

            # 3. Insert Messages in batches
            print("💬 Inserting messages...")
            for i in range(0, len(prepared_messages), batch_size):
                batch = prepared_messages[i : i + batch_size]

                # Use create_many for batch insert
                try:
                    result = await prisma.message.create_many(
                        data=batch, skip_duplicates=True  # Skip if seq already exists
                    )
                    print(f"   ✅ Inserted batch {i//batch_size + 1}: {result} messages")
                except Exception as e:
                    print(f"   ❌ Error in batch {i//batch_size + 1}: {e}")
                    # Try individual inserts for this batch
                    for msg in batch:
                        try:
                            # Prepare update data (exclude auto-generated fields)
                            update_data = {k: v for k, v in msg.items() if k not in ['createdAt', 'updatedAt']}

                            await prisma.message.upsert(
                                where={'seq': msg['seq']}, data={'create': msg, 'update': update_data}
                            )
                        except Exception as msg_e:
                            print(f"     ❌ Failed to insert message {msg['seq']}: {msg_e}")

                            # If upsert fails, try simple create
                            try:
                                await prisma.message.create(data=msg)
                                print(f"     ✅ Created message {msg['seq']} with simple create")
                            except Exception as create_e:
                                print(f"     ❌ Simple create also failed for {msg['seq']}: {create_e}")

            print("✅ Import completed successfully!")

        finally:
            await prisma.disconnect()

        # Mark processing as successful
        if progress_tracker:
            stats = {
                "messages_count": len(prepared_messages),
                "chat_rooms_count": len(chat_rooms),
                "users_count": len(users),
            }
            progress_tracker.mark_file_processing_success(file_path, stats)

    except Exception as e:
        error_msg = f"Processing failed: {str(e)}"
        print(f"❌ {error_msg}")
        if progress_tracker:
            progress_tracker.mark_file_processing_failed(file_path, error_msg)
        raise  # Re-raise the exception for debugging

    # For now, just show what would be imported
    print("\n📋 Import Preview (Prisma integration needed):")
    print(f"Would create/update:")
    print(f"  📁 {len(chat_rooms)} ChatRoom records")
    print(f"  👤 {len(users)} User records")
    print(f"  💬 {len(prepared_messages)} Message records")

    print(f"\n📁 Sample ChatRooms:")
    for i, (room_id, room_data) in enumerate(list(chat_rooms.items())[:3]):
        print(f"  {i+1}. ID: {room_id}, Name: '{room_data['name']}', IsChatRoom: {room_data['isChatRoom']}")

    print(f"\n👤 Sample Users:")
    for i, (user_id, user_data) in enumerate(list(users.items())[:3]):
        print(f"  {i+1}. ID: {user_id}, Name: '{user_data['name']}'")

    print(f"\n💬 Sample Messages:")
    for i, msg in enumerate(prepared_messages[:3]):
        print(f"  {i+1}. Seq: {msg['seq']}, Type: {msg['type']}, Content: '{msg['content'][:50]}...'")


def setup_instructions():
    """Print setup instructions for Prisma integration"""
    print("\n🔧 To enable actual database import:")
    print("1. Install Prisma: pip install prisma")
    print("2. Set DATABASE_URL environment variable")
    print("3. Run: prisma generate")
    print("4. Run: prisma db push (to create tables)")
    print("5. Uncomment Prisma code in this script")
    print("6. Run the import!")


if __name__ == "__main__":
    import sys

    if len(sys.argv) not in [2, 3]:
        print("Usage: python import_to_prisma.py <json_file_path> [--use-progress]")
        print("Example: python import_to_prisma.py chat_history/changchen0102/2025/chatlog_2025-08.json")
        print("         python import_to_prisma.py chat_history/changchen0102/2025/chatlog_2025-08.json --use-progress")
        sys.exit(1)

    json_file = sys.argv[1]
    use_progress = len(sys.argv) == 3 and sys.argv[2] == "--use-progress"

    if not Path(json_file).exists():
        print(f"❌ File not found: {json_file}")
        sys.exit(1)

    # Initialize progress tracker if requested
    progress_tracker = ProgressTracker() if use_progress else None

    if progress_tracker:
        print("📊 Using progress tracking to avoid duplicate imports")
        progress_tracker.print_summary()

    # Run the import
    asyncio.run(import_chat_data_to_db(json_file, progress_tracker=progress_tracker))
    setup_instructions()
