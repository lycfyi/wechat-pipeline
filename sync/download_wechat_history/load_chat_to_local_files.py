#!/usr/bin/env python3
"""
Script to fetch chat logs from WeChat API for multiple groups over the last 10 years
Features:
- Fetches chat history for all groups in GROUP_NAMES by month (newest to oldest)
- Uses date ranges (YYYY-MM-DD~YYYY-MM-DD) for efficient monthly batch fetching
- Organizes data in proper folder structure (group/year/month/)
- Only creates files when there's actual message data
- Tracks progress to avoid redoing work when rerunning (including empty months)
- Automatically cleans up empty year and talker folders
- Records earliest message dates for backfill planning
- Handles API rate limiting and failures gracefully
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import requests

GROUP_NAMES = [
    # Personal groups
    # "my family",
    # "家",
    # Work related groups
    # "🐦 X BoostClub (总 Fo 248w)",
    # "(朱鹤) GOO真出海",
    # "哥飞的朋友们SVIP群",
    # "推特 IP Launch 快闪群 - 陈唱",
    # "⛴️ 出海去社区会员群3️⃣",
    # "⛴️ 出海去社区会员群2️⃣",
    # "⛴️ 出海去孵化器会员群1️⃣",
    "XBC Group",
]


def load_chatroom_names_from_json(json_file_path="api_data/chatroom_20250805_003403.json"):
    """
    Load chatroom names from the chatroom JSON file

    Args:
        json_file_path (str): Path to the chatroom JSON file

    Returns:
        list: List of chatroom names (NickName values)
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            chatrooms = json.load(f)

        # Extract NickName from each chatroom, skip empty ones
        group_names = []
        for chatroom in chatrooms:
            nickname = chatroom.get('NickName', '').strip()
            if nickname:  # Only add non-empty nicknames
                group_names.append(nickname)

        print(f"📁 Loaded {len(group_names)} chatroom names from {json_file_path}")
        return group_names

    except FileNotFoundError:
        print(f"⚠️  Chatroom file not found: {json_file_path}")
        print("   Falling back to default group names")
        return ["XBC Group"]  # Fallback to original hardcoded names

    except json.JSONDecodeError as e:
        print(f"⚠️  Error parsing chatroom JSON: {e}")
        print("   Falling back to default group names")
        return ["XBC Group"]

    except Exception as e:
        print(f"⚠️  Error loading chatroom names: {e}")
        print("   Falling back to default group names")
        return ["XBC Group"]


def load_contact_usernames_from_json(json_file_path="api_data/contact_20250805_003403.json"):
    """
    Load contact usernames from the contact JSON file

    Args:
        json_file_path (str): Path to the contact JSON file

    Returns:
        list: List of individual contact usernames (UserName values, excluding chatrooms)
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            contacts = json.load(f)

        # Extract UserName from individual contacts (not chatrooms), skip empty ones
        contact_usernames = []
        for contact in contacts:
            username = contact.get('UserName', '').strip()
            # Skip chatrooms (those ending with @chatroom) and empty usernames
            if (
                username
                and not username.endswith('@chatroom')
                and not username.endswith('@openim')
                and not username.startswith('wxid_')
                and not username.startswith('gh_')
            ):
                contact_usernames.append(username)

        print(f"👤 Loaded {len(contact_usernames)} contact usernames from {json_file_path}")
        return contact_usernames

    except FileNotFoundError:
        print(f"⚠️  Contact file not found: {json_file_path}")
        print("   Falling back to default individual names")
        return ["daisy_zheng2011"]  # Fallback to original hardcoded names

    except json.JSONDecodeError as e:
        print(f"⚠️  Error parsing contact JSON: {e}")
        print("   Falling back to default individual names")
        return ["daisy_zheng2011"]

    except Exception as e:
        print(f"⚠️  Error loading contact usernames: {e}")
        print("   Falling back to default individual names")
        return ["daisy_zheng2011"]


# Load group names dynamically from chatroom JSON file
GROUP_NAMES = load_chatroom_names_from_json()

# Load contact usernames dynamically from contact JSON file
INDIVIDUAL_NAMES = load_contact_usernames_from_json()

ALL_NAMES = GROUP_NAMES + INDIVIDUAL_NAMES
print("len(ALL_NAMES):", len(ALL_NAMES))

# Configuration
BASE_OUTPUT_DIR = "chat_history"
PROGRESS_FILE = "chat_download_progress.json"
EARLIEST_DATES_FILE = "group_earliest_dates.json"  # Track earliest message dates per group
DELAY_BETWEEN_REQUESTS = 0.5  # seconds to avoid overwhelming the API
YEARS_TO_FETCH = 10  # last 10 years


def load_progress():
    """Load progress from the progress file"""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            print(f"Warning: Could not load progress file {PROGRESS_FILE}")

    return {"processed": [], "failed": [], "last_updated": None}


def save_progress(progress):
    """Save progress to the progress file"""
    progress["last_updated"] = datetime.now().isoformat()
    try:
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Warning: Could not save progress: {e}")


def load_earliest_dates():
    """Load earliest dates information from file"""
    if os.path.exists(EARLIEST_DATES_FILE):
        try:
            with open(EARLIEST_DATES_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            print(f"Warning: Could not load earliest dates file {EARLIEST_DATES_FILE}")

    return {"earliest_dates": {}, "last_updated": None}


def save_earliest_dates(earliest_dates_info):
    """Save earliest dates information to file"""
    earliest_dates_info["last_updated"] = datetime.now().isoformat()
    try:
        with open(EARLIEST_DATES_FILE, 'w', encoding='utf-8') as f:
            json.dump(earliest_dates_info, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Warning: Could not save earliest dates: {e}")


def cleanup_empty_folders(talker_name=None):
    """
    Remove empty year folders and empty talker folders

    Args:
        talker_name (str, optional): If provided, only clean up folders for this talker.
                                   If None, clean up all folders.
    """
    base_path = Path(BASE_OUTPUT_DIR)
    if not base_path.exists():
        return

    removed_count = 0

    # If specific talker is provided, only check that talker's folder
    if talker_name:
        talker_folders = [base_path / create_safe_filename(talker_name)]
    else:
        # Check all folders in the base directory
        talker_folders = [f for f in base_path.iterdir() if f.is_dir()]

    for talker_folder in talker_folders:
        if not talker_folder.exists():
            continue

        # Check each year folder within this talker folder
        year_folders = [f for f in talker_folder.iterdir() if f.is_dir() and f.name.isdigit()]

        for year_folder in year_folders:
            # Check if year folder is empty (no .json files)
            json_files = list(year_folder.glob("*.json"))
            if len(json_files) == 0:
                try:
                    year_folder.rmdir()  # Only removes if truly empty
                    print(f"🗑️  Removed empty year folder: {year_folder}")
                    removed_count += 1
                except OSError:
                    # Folder not empty (might have hidden files), skip
                    pass

        # Check if talker folder is now empty after removing year folders
        remaining_items = list(talker_folder.iterdir())
        if len(remaining_items) == 0:
            try:
                talker_folder.rmdir()
                print(f"🗑️  Removed empty talker folder: {talker_folder}")
                removed_count += 1
            except OSError:
                # Folder not empty, skip
                pass

    if removed_count > 0:
        print(f"🧹 Cleanup completed: removed {removed_count} empty folders")

    return removed_count


def find_earliest_message_date(messages_data):
    """
    Find the earliest message date from chat data

    Args:
        messages_data: The chat messages data (list or dict)

    Returns:
        str: Earliest date in YYYY-MM-DD format, or None if no messages found
    """
    if not messages_data:
        return None

    earliest_time = None
    messages = []

    # Extract messages array from different formats
    if isinstance(messages_data, list):
        messages = messages_data
    elif isinstance(messages_data, dict):
        # Check for our saved format with metadata
        if 'messages' in messages_data and isinstance(messages_data['messages'], list):
            messages = messages_data['messages']
        else:
            # Check standard API response formats
            messages = messages_data.get('messages', messages_data.get('data', messages_data.get('results', [])))

    if not isinstance(messages, list) or len(messages) == 0:
        return None

    # Find earliest timestamp
    for message in messages:
        if isinstance(message, dict):
            # Check common timestamp fields
            time_str = message.get('time', message.get('timestamp', message.get('createTime', '')))
            if time_str:
                try:
                    # Parse different time formats
                    if 'T' in time_str:  # ISO format like "2025-07-28T17:22:29+08:00"
                        time_obj = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                    else:  # Try parsing as timestamp
                        time_obj = datetime.fromtimestamp(float(time_str))

                    if earliest_time is None or time_obj < earliest_time:
                        earliest_time = time_obj
                except (ValueError, TypeError):
                    continue

    return earliest_time.strftime("%Y-%m-%d") if earliest_time else None


def update_talker_earliest_date(talker_name, messages_data, earliest_dates_info):
    """
    Update the earliest date for a talker if we found earlier messages

    Args:
        talker_name (str): Name of the talker
        messages_data: The chat messages data
        earliest_dates_info (dict): Current earliest dates tracking info

    Returns:
        bool: True if the earliest date was updated
    """
    if not has_meaningful_data(messages_data):
        return False

    message_earliest_date = find_earliest_message_date(messages_data)
    if not message_earliest_date:
        return False

    current_earliest = earliest_dates_info["earliest_dates"].get(talker_name)

    # Update if this is the first time or if we found an earlier date
    if current_earliest is None or message_earliest_date < current_earliest:
        earliest_dates_info["earliest_dates"][talker_name] = message_earliest_date
        print(f"📅 Updated earliest date for {talker_name}: {message_earliest_date}")
        return True

    return False


def generate_month_list():
    """Generate list of months for the last YEARS_TO_FETCH years (newest to oldest)"""
    months = []
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=365 * YEARS_TO_FETCH)

    # Start from the first day of the start month
    current_year = start_date.year
    current_month = start_date.month

    # Generate all months from start to end
    while True:
        month_start = datetime(current_year, current_month, 1).date()

        # Calculate month end (last day of the month)
        if current_month == 12:
            next_month_start = datetime(current_year + 1, 1, 1).date()
        else:
            next_month_start = datetime(current_year, current_month + 1, 1).date()
        month_end = next_month_start - timedelta(days=1)

        # Don't go beyond current date
        if month_start > end_date:
            break

        # If this month extends beyond end_date, use end_date as month_end
        if month_end > end_date:
            month_end = end_date

        months.append(
            {
                'year': current_year,
                'month': current_month,
                'start_date': month_start.strftime("%Y-%m-%d"),
                'end_date': month_end.strftime("%Y-%m-%d"),
                'month_key': f"{current_year}-{current_month:02d}",
            }
        )

        # Move to next month
        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1

    # Reverse to start from newest month
    return months[::-1]


def create_safe_filename(text):
    """Create a safe filename from text"""
    # Remove or replace unsafe characters
    safe_chars = "".join(c for c in text if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
    return safe_chars.replace(' ', '_')


def get_output_path(talker_name, month_info):
    """Generate the output path for a given group and month"""
    safe_group = create_safe_filename(talker_name)

    # Create folder structure: chat_history/talker_name/year/
    folder_path = Path(BASE_OUTPUT_DIR) / safe_group / str(month_info['year'])
    folder_path.mkdir(parents=True, exist_ok=True)

    # Monthly filename: chatlog_2024-01.json
    filename = f"chatlog_{month_info['month_key']}.json"
    return folder_path / filename


def fetch_chat_logs(base_url="http://localhost:5030", time_range="2025-07-28", talker=ALL_NAMES[0], format_type="json"):
    """
    Fetch chat logs from the WeChat API

    Args:
        base_url (str): Base URL of the API
        time (str): Date in YYYY-MM-DD format or date range YYYY-MM-DD~YYYY-MM-DD
        talker (str): Name of the chat room or talker
        format_type (str): Response format (json)

    Returns:
        dict: Parsed JSON response or None if error
    """

    # Construct the full URL
    url = f"{base_url}/api/v1/chatlog"

    # Parameters for the GET request
    # requests automatically handles URL encoding
    params = {"time": time_range, "talker": talker, "format": format_type}

    try:
        # Make the GET request
        response = requests.get(url, params=params, timeout=30)

        # Check if request was successful
        response.raise_for_status()

        # Parse JSON response
        data = response.json()

        return data

    except requests.exceptions.ConnectionError:
        print(f"Error: Could not connect to {url}")
        print("Make sure the server is running on localhost:5030")
        return None

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        return None

    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
        return None

    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
        print(f"Response text: {response.text}")
        return None


def pretty_print_json(data):
    """Pretty print JSON data similar to jq"""
    if data is not None:
        print(json.dumps(data, indent=2, ensure_ascii=False))


def print_simplified_messages(data):
    """Print only senderName and content from chat messages"""
    if data is None:
        return

    print("聊天记录 (senderName, content):")
    print("=" * 50)

    if isinstance(data, list):
        for i, message in enumerate(data, 1):
            if isinstance(message, dict):
                sender = message.get('senderName', 'Unknown')
                content = message.get('content', '')
                print(f"{i:3d}. {sender}: {content}")
            else:
                print(f"{i:3d}. {message}")
    elif isinstance(data, dict):
        # If the response is a dict, look for common message array keys
        messages = data.get('messages', data.get('data', data.get('results', [])))
        if isinstance(messages, list):
            for i, message in enumerate(messages, 1):
                if isinstance(message, dict):
                    sender = message.get('senderName', 'Unknown')
                    content = message.get('content', '')
                    print(f"{i:3d}. {sender}: {content}")
                else:
                    print(f"{i:3d}. {message}")
        else:
            # If no messages array found, treat the dict itself as a single message
            sender = data.get('senderName', 'Unknown')
            content = data.get('content', '')
            print(f"1. {sender}: {content}")

    print("=" * 50)


def has_meaningful_data(data):
    """
    Check if the chat data contains actual messages

    Args:
        data: The chat logs data to check

    Returns:
        bool: True if data contains messages, False if empty or no meaningful content
    """
    if data is None:
        return False

    # If data is a list (array of messages)
    if isinstance(data, list):
        return len(data) > 0

    # If data is a dict, check common message array keys
    if isinstance(data, dict):
        # Check for our saved format with metadata
        if 'messages' in data and 'metadata' in data:
            messages = data['messages']
            if isinstance(messages, list):
                return len(messages) > 0
            elif isinstance(messages, dict):
                return has_meaningful_data(messages)

        # Check standard API response formats
        messages = data.get('messages', data.get('data', data.get('results', [])))
        if isinstance(messages, list):
            return len(messages) > 0
        # If no standard message array found, check if the dict itself has content
        return bool(data.get('content', '').strip())

    return False


def save_chat_data(data, talker_name, month_info):
    """
    Save chat logs data to a JSON file in organized folder structure

    Args:
        data: The chat logs data to save
        talker_name (str): Name of the chat group/talker
        month_info (dict): Month information with year, month, start_date, end_date, month_key

    Returns:
        Path: Path to the saved file or None if error
    """
    try:
        output_path = get_output_path(talker_name, month_info)

        # Add metadata to the saved data
        save_data = {
            "metadata": {
                "talker_name": talker_name,
                "month": month_info['month_key'],
                "date_range": f"{month_info['start_date']}~{month_info['end_date']}",
                "fetched_at": datetime.now().isoformat(),
                "message_count": (
                    len(data)
                    if isinstance(data, list)
                    else (
                        len(data.get('messages', data.get('data', data.get('results', []))))
                        if isinstance(data, dict)
                        else 0
                    )
                ),
            },
            "messages": data,
        }

        # Save to file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, indent=2, ensure_ascii=False)

        return output_path

    except Exception as e:
        print(f"Error saving to file: {e}")
        return None


def process_single_month_group(talker_name, month_info, progress, earliest_dates_info):
    """
    Process a single group-month combination

    Args:
        talker_name (str): Name of the group to process
        month_info (dict): Month information with year, month, start_date, end_date, month_key
        progress (dict): Progress tracking dictionary
        earliest_dates_info (dict): Earliest dates tracking dictionary

    Returns:
        bool: True if successful, False if failed
    """
    task_id = f"{talker_name}_{month_info['month_key']}"

    # Check if already processed
    if task_id in progress["processed"]:
        return True

    # Check if file already exists
    output_path = get_output_path(talker_name, month_info)
    if output_path.exists():
        print(f"📄 File already exists: {output_path}")
        progress["processed"].append(task_id)
        return True

    date_range = f"{month_info['start_date']}~{month_info['end_date']}"
    print(f"📥 Fetching {talker_name} for {month_info['month_key']} ({date_range})...")

    # Fetch chat logs using date range
    chat_logs = fetch_chat_logs(time_range=date_range, talker=talker_name)

    if chat_logs is not None:
        # Check if data contains meaningful content
        if has_meaningful_data(chat_logs):
            # Update earliest date tracking before saving
            update_talker_earliest_date(talker_name, chat_logs, earliest_dates_info)

            # Save the data only if it has content
            saved_path = save_chat_data(chat_logs, talker_name, month_info)
            if saved_path:
                message_count = (
                    len(chat_logs)
                    if isinstance(chat_logs, list)
                    else (
                        len(chat_logs.get('messages', chat_logs.get('data', chat_logs.get('results', []))))
                        if isinstance(chat_logs, dict)
                        else 0
                    )
                )
                print(f"✅ Saved: {saved_path} ({message_count} messages)")
                progress["processed"].append(task_id)
                return True
            else:
                print(f"❌ Failed to save data for {talker_name} for {month_info['month_key']}")
                progress["failed"].append(task_id)
                return False
        else:
            # No meaningful data, but mark as processed (don't create empty file)
            print(f"📭 No messages found for {talker_name} for {month_info['month_key']} (marked as processed)")
            progress["processed"].append(task_id)
            return True
    else:
        print(f"⚠️  API request failed for {talker_name} for {month_info['month_key']}")
        progress["failed"].append(task_id)
        return False


def main():
    """Main function to execute the comprehensive chat log fetch"""

    print("🚀 Starting comprehensive WeChat chat history download (Monthly Batch Mode)")
    print(f"📅 Fetching last {YEARS_TO_FETCH} years of chat history by month")
    print(f"👥 Groups: {ALL_NAMES}")
    print(f"📁 Output directory: {BASE_OUTPUT_DIR}")
    print("-" * 60)

    # Load progress
    progress = load_progress()
    if progress["last_updated"]:
        print(f"📋 Loaded progress file (last updated: {progress['last_updated']})")
        print(f"   - Already processed: {len(progress['processed'])} items")
        print(f"   - Failed: {len(progress['failed'])} items")

    # Load earliest dates tracking
    earliest_dates_info = load_earliest_dates()
    if earliest_dates_info["last_updated"]:
        print(f"📅 Loaded earliest dates file (last updated: {earliest_dates_info['last_updated']})")
        if earliest_dates_info["earliest_dates"]:
            print("   Current earliest dates:")
            for group, date in earliest_dates_info["earliest_dates"].items():
                print(f"   - {group}: {date}")
        else:
            print("   - No earliest dates recorded yet")

    # Generate month list
    print("📅 Generating month list...")
    months = generate_month_list()
    print(
        f"📅 Generated {len(months)} months from {months[0]['month_key']} (newest) to {months[-1]['month_key']} (oldest)"
    )

    # Calculate total tasks
    total_tasks = len(ALL_NAMES) * len(months)
    completed_tasks = len(progress["processed"])
    failed_tasks = len(progress["failed"])
    remaining_tasks = total_tasks - completed_tasks - failed_tasks

    print(f"\n📊 Task Overview:")
    print(f"   - Total tasks: {total_tasks}")
    print(f"   - Completed: {completed_tasks}")
    print(f"   - Failed: {failed_tasks}")
    print(f"   - Remaining: {remaining_tasks}")

    if remaining_tasks == 0:
        print("\n🎉 All tasks completed! No new work to do.")
        return

    print(f"\n⏰ Starting processing (delay: {DELAY_BETWEEN_REQUESTS}s between requests)")
    print("-" * 60)

    # Process each group and month combination
    start_time = datetime.now()
    processed_count = 0
    success_count = 0

    try:
        for talker_name in ALL_NAMES:
            # print(f"\n🏢 Processing group: {talker_name}")
            # print("-" * 40)

            for month_info in months:
                task_id = f"{talker_name}_{month_info['month_key']}"

                # Skip if already processed or failed
                if task_id in progress["processed"] or task_id in progress["failed"]:
                    continue

                # Process this month-group combination
                success = process_single_month_group(talker_name, month_info, progress, earliest_dates_info)

                processed_count += 1
                if success:
                    success_count += 1

                # Save progress periodically (every 5 items for monthly batches)
                if processed_count % 5 == 0:
                    save_progress(progress)
                    save_earliest_dates(earliest_dates_info)
                    elapsed = datetime.now() - start_time
                    rate = processed_count / elapsed.total_seconds() * 60  # per minute
                    remaining = remaining_tasks - processed_count
                    eta_minutes = remaining / rate if rate > 0 else 0

                    print(
                        f"\n⏱️  Progress: {processed_count}/{remaining_tasks} "
                        f"({processed_count/remaining_tasks*100:.1f}%) "
                        f"| Rate: {rate:.1f}/min "
                        f"| ETA: {eta_minutes:.0f}min"
                    )

                # Rate limiting (longer for monthly batches as they fetch more data)
                time.sleep(DELAY_BETWEEN_REQUESTS * 2)

            # Clean up empty folders for this talker after processing all its months
            cleanup_empty_folders(talker_name)

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user. Saving progress...")
        save_progress(progress)
        save_earliest_dates(earliest_dates_info)
        print("Progress saved. You can resume by running the script again.")
        return

    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        save_progress(progress)
        save_earliest_dates(earliest_dates_info)
        print("Progress saved.")
        raise

    # Final save and summary
    save_progress(progress)
    save_earliest_dates(earliest_dates_info)

    # Final cleanup of all empty folders
    print(f"\n🧹 Running final cleanup of empty folders...")
    cleanup_empty_folders()

    elapsed = datetime.now() - start_time
    print(f"\n🏁 Processing completed!")
    print(f"⏱️  Total time: {elapsed}")
    print(f"📊 Results:")
    print(f"   - Processed in this run: {processed_count}")
    print(f"   - Successful: {success_count}")
    print(f"   - Failed: {processed_count - success_count}")
    print(f"   - Total completed: {len(progress['processed'])}")
    print(f"   - Total failed: {len(progress['failed'])}")

    # Show some stats about saved files
    if success_count > 0:
        print(f"\n📁 Files saved in: {os.path.abspath(BASE_OUTPUT_DIR)}")

        # Count files by group
        for name in ALL_NAMES:
            safe_group = create_safe_filename(name)
            group_path = Path(BASE_OUTPUT_DIR) / safe_group
            if group_path.exists():
                file_count = sum(1 for _ in group_path.rglob("*.json"))
                print(f"   - {name}: {file_count} monthly files")

    print(f"\n📋 Progress file: {os.path.abspath(PROGRESS_FILE)}")
    print(f"📅 Earliest dates file: {os.path.abspath(EARLIEST_DATES_FILE)}")

    # Show earliest dates summary
    if earliest_dates_info["earliest_dates"]:
        print(f"\n📅 Earliest message dates found:")
        for group, date in sorted(earliest_dates_info["earliest_dates"].items()):
            print(f"   - {group}: {date}")
        print(f"\n💡 For backfill: You can fetch earlier data starting from dates above for each group")
    else:
        print(f"\n📅 No earliest dates recorded yet (no messages found or processed)")

    if len(progress["failed"]) > 0:
        print(f"\n⚠️  {len(progress['failed'])} tasks failed. You may want to retry them later.")
        print("Failed tasks are tracked in the progress file.")


if __name__ == "__main__":
    main()
