#!/usr/bin/env python3
"""
并行版本的微信聊天记录下载脚本
基于 load_chat_to_local_files.py，添加了多线程并行处理能力

新特性:
- 使用 ThreadPoolExecutor 并行处理多个 talker-month 组合
- 线程安全的进度跟踪和文件操作
- 智能并发控制，避免API过载
- 保持与原版本完全兼容的进度文件格式
- 改进的错误处理和重试机制
- 实时性能监控和ETA估算

性能提升: 预期 5-20倍 加速
"""

import concurrent.futures
import json
import os
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import requests

# ==================== 数据加载函数 ====================


def load_chatroom_names_from_json(json_file_path=None):
    """Load chatroom names from the latest chatroom JSON file"""
    try:
        # Find the latest chatroom file if not specified
        if json_file_path is None:
            from pathlib import Path
            api_data_dir = Path("api_data")
            chatroom_files = sorted(api_data_dir.glob("chatroom_*.json"), reverse=True)
            if chatroom_files:
                json_file_path = str(chatroom_files[0])
            else:
                print("⚠️  No chatroom files found in api_data/")
                return ["XBC Group"]

        with open(json_file_path, 'r', encoding='utf-8') as f:
            chatrooms = json.load(f)

        group_names = []
        for chatroom in chatrooms:
            nickname = chatroom.get('NickName', '').strip()
            if nickname:
                group_names.append(nickname)

        print(f"📁 Loaded {len(group_names)} chatroom names from {json_file_path}")
        return group_names

    except FileNotFoundError:
        print(f"⚠️  Chatroom file not found: {json_file_path}")
        return ["XBC Group"]
    except (json.JSONDecodeError, Exception) as e:
        print(f"⚠️  Error loading chatroom names: {e}")
        return ["XBC Group"]


def load_contact_usernames_from_json(json_file_path="api_data/contact_latest.json"):
    """Load contact usernames from the contact JSON file"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            contacts = json.load(f)

        contact_usernames = []
        for contact in contacts:
            username = contact.get('UserName', '').strip()
            # Skip chatrooms and unwanted contact types
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
        return ["daisy_zheng2011"]
    except (json.JSONDecodeError, Exception) as e:
        print(f"⚠️  Error loading contact usernames: {e}")
        return ["daisy_zheng2011"]


# Load names dynamically
GROUP_NAMES = load_chatroom_names_from_json()
INDIVIDUAL_NAMES = load_contact_usernames_from_json()
ALL_NAMES = GROUP_NAMES + INDIVIDUAL_NAMES

print("len(ALL_NAMES):", len(ALL_NAMES))

# Configuration
BASE_OUTPUT_DIR = "chat_history"
PROGRESS_FILE = "chat_download_progress.json"
EARLIEST_DATES_FILE = "group_earliest_dates.json"
CONTACT_CACHE_FILE = "contact_data_cache.json"  # 联系人数据缓存文件
DELAY_BETWEEN_REQUESTS = 0.5
YEARS_TO_FETCH = 10

# Parallel processing configuration
MAX_WORKERS = 8  # Start conservative, can be increased based on testing
BATCH_SIZE = 50  # Process tasks in batches to reduce lock contention
RETRY_ATTEMPTS = 3  # Number of retry attempts for failed requests
PROGRESS_SAVE_INTERVAL = 10  # Save progress every N completed tasks
CONTACT_CACHE_EXPIRY_DAYS = 30  # 联系人缓存过期天数


# ==================== 线程安全的进度跟踪器 ====================


class ThreadSafeProgressTracker:
    """线程安全的进度跟踪器"""

    def __init__(self, initial_progress, initial_earliest_dates):
        self.progress = initial_progress.copy()
        self.earliest_dates_info = initial_earliest_dates.copy()
        self.progress_lock = threading.Lock()
        self.earliest_lock = threading.Lock()
        self.stats_lock = threading.Lock()

        # 统计信息
        self.stats = {
            "processed_count": 0,
            "success_count": 0,
            "failed_count": 0,
            "start_time": datetime.now(),
            "last_save_time": datetime.now(),
        }

    def add_processed(self, task_id):
        """添加已处理的任务"""
        with self.progress_lock:
            if task_id not in self.progress["processed"]:
                self.progress["processed"].append(task_id)

        with self.stats_lock:
            self.stats["processed_count"] += 1
            self.stats["success_count"] += 1

    def add_failed(self, task_id):
        """添加失败的任务"""
        with self.progress_lock:
            if task_id not in self.progress["failed"]:
                self.progress["failed"].append(task_id)

        with self.stats_lock:
            self.stats["processed_count"] += 1
            self.stats["failed_count"] += 1

    def update_earliest_date(self, talker_name, date):
        """更新最早日期（线程安全）"""
        with self.earliest_lock:
            current = self.earliest_dates_info["earliest_dates"].get(talker_name)
            if current is None or date < current:
                self.earliest_dates_info["earliest_dates"][talker_name] = date
                return True
        return False

    def is_processed(self, task_id):
        """检查任务是否已处理"""
        with self.progress_lock:
            return task_id in self.progress["processed"] or task_id in self.progress["failed"]

    def get_processed_set(self):
        """获取已处理任务的集合（用于批量检查，性能更好）"""
        with self.progress_lock:
            processed_set = set(self.progress["processed"])
            failed_set = set(self.progress["failed"])
            return processed_set | failed_set

    def get_progress_copy(self):
        """获取进度的副本"""
        with self.progress_lock:
            return self.progress.copy()

    def get_earliest_dates_copy(self):
        """获取最早日期的副本"""
        with self.earliest_lock:
            return self.earliest_dates_info.copy()

    def get_stats_copy(self):
        """获取统计信息的副本"""
        with self.stats_lock:
            return self.stats.copy()

    def should_save_progress(self):
        """检查是否应该保存进度"""
        with self.stats_lock:
            now = datetime.now()
            if (now - self.stats["last_save_time"]).seconds >= 30:  # 每30秒保存一次
                self.stats["last_save_time"] = now
                return True
        return False


# ==================== 原有辅助函数 ====================


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


def load_contact_cache():
    """Load contact data cache from file"""
    if os.path.exists(CONTACT_CACHE_FILE):
        try:
            with open(CONTACT_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)

            # 验证缓存格式
            if not isinstance(cache_data, dict) or "cache_version" not in cache_data:
                print(f"Warning: Invalid contact cache format, starting fresh")
                return {
                    "cache_version": "1.0",
                    "contacts_with_data": set(),
                    "contacts_without_data": set(),
                    "last_updated": None,
                    "cache_age_days": 0,
                }

            # 转换列表为集合以提高查找性能
            contacts_with_data = set(cache_data.get("contacts_with_data", []))
            contacts_without_data = set(cache_data.get("contacts_without_data", []))

            cache_age_days = 0
            if cache_data.get("last_updated"):
                try:
                    last_updated = datetime.fromisoformat(cache_data["last_updated"])
                    cache_age_days = (datetime.now() - last_updated).days
                except ValueError:
                    pass

            print(f"📂 Loaded contact cache from {CONTACT_CACHE_FILE}")
            print(f"   - Contacts with data: {len(contacts_with_data)}")
            print(f"   - Contacts without data: {len(contacts_without_data)}")
            print(f"   - Cache age: {cache_age_days} days")

            # 检查缓存是否过期
            is_expired = cache_age_days > CONTACT_CACHE_EXPIRY_DAYS
            if is_expired:
                print(f"   ⚠️ Cache is expired (>{CONTACT_CACHE_EXPIRY_DAYS} days old), will re-check all contacts")
                return {
                    "cache_version": "1.0",
                    "contacts_with_data": set(),
                    "contacts_without_data": set(),
                    "last_updated": None,
                    "cache_age_days": 0,
                }

            return {
                "cache_version": cache_data.get("cache_version", "1.0"),
                "contacts_with_data": contacts_with_data,
                "contacts_without_data": contacts_without_data,
                "last_updated": cache_data.get("last_updated"),
                "cache_age_days": cache_age_days,
            }

        except (json.JSONDecodeError, FileNotFoundError, Exception) as e:
            print(f"Warning: Could not load contact cache {CONTACT_CACHE_FILE}: {e}")

    return {
        "cache_version": "1.0",
        "contacts_with_data": set(),
        "contacts_without_data": set(),
        "last_updated": None,
        "cache_age_days": 0,
    }


def save_contact_cache(contacts_with_data, contacts_without_data):
    """Save contact data cache to file"""
    cache_data = {
        "cache_version": "1.0",
        "contacts_with_data": list(contacts_with_data),
        "contacts_without_data": list(contacts_without_data),
        "last_updated": datetime.now().isoformat(),
        "total_contacts_checked": len(contacts_with_data) + len(contacts_without_data),
        "data_rate_percentage": (
            len(contacts_with_data) / (len(contacts_with_data) + len(contacts_without_data)) * 100
            if (len(contacts_with_data) + len(contacts_without_data)) > 0
            else 0
        ),
    }

    try:
        with open(CONTACT_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)
        print(f"💾 Contact cache saved to {CONTACT_CACHE_FILE}")
    except Exception as e:
        print(f"Warning: Could not save contact cache: {e}")


def generate_month_list():
    """Generate list of months for the last YEARS_TO_FETCH years (newest to oldest)"""
    months = []
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=365 * YEARS_TO_FETCH)

    current_year = start_date.year
    current_month = start_date.month

    while True:
        month_start = datetime(current_year, current_month, 1).date()

        if current_month == 12:
            next_month_start = datetime(current_year + 1, 1, 1).date()
        else:
            next_month_start = datetime(current_year, current_month + 1, 1).date()
        month_end = next_month_start - timedelta(days=1)

        if month_start > end_date:
            break

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

        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1

    return months[::-1]  # Reverse to start from newest month


def create_safe_filename(text):
    """Create a safe filename from text"""
    safe_chars = "".join(c for c in text if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
    return safe_chars.replace(' ', '_')


def get_output_path(talker_name, month_info):
    """Generate the output path for a given group and month"""
    safe_group = create_safe_filename(talker_name)
    folder_path = Path(BASE_OUTPUT_DIR) / safe_group / str(month_info['year'])
    folder_path.mkdir(parents=True, exist_ok=True)
    filename = f"chatlog_{month_info['month_key']}.json"
    return folder_path / filename


def fetch_chat_logs(
    base_url="http://localhost:5030", time_range="2025-07-28", talker=ALL_NAMES[0], format_type="json", limit=None
):
    """Fetch chat logs from the WeChat API with retry logic"""
    url = f"{base_url}/api/v1/chatlog"
    params = {"time": time_range, "talker": talker, "format": format_type}
    if limit is not None:
        params["limit"] = limit

    for attempt in range(RETRY_ATTEMPTS):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.ConnectionError:
            if attempt == 0:  # Only print on first attempt
                print(f"⚠️  Connection failed for {talker} (attempt {attempt + 1}/{RETRY_ATTEMPTS})")
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(2**attempt)  # Exponential backoff
            continue

        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error for {talker}: {e}")
            if e.response and e.response.status_code in [404, 403]:
                break  # Don't retry for client errors
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(2**attempt)
            continue

        except Exception as e:
            print(f"Request Error for {talker}: {e}")
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(2**attempt)
            continue

    return None


def check_contact_has_data(talker_name, date_range_start, date_range_end):
    """
    Check if a contact has any data in the specified date range
    Returns True if contact has data, False otherwise
    """
    date_range = f"{date_range_start}~{date_range_end}"

    try:
        # Use limit=1 to just check if any records exist
        result = fetch_chat_logs(time_range=date_range, talker=talker_name, limit=1)

        if result is None:
            return False

        # Check if there's meaningful data
        return has_meaningful_data(result)

    except Exception as e:
        print(f"⚠️  Error checking contact {talker_name}: {e}")
        return True  # If we can't check, assume there might be data


def has_meaningful_data(data):
    """Check if the chat data contains actual messages"""
    if data is None:
        return False

    if isinstance(data, list):
        return len(data) > 0

    if isinstance(data, dict):
        if 'messages' in data and 'metadata' in data:
            messages = data['messages']
            if isinstance(messages, list):
                return len(messages) > 0
            elif isinstance(messages, dict):
                return has_meaningful_data(messages)

        messages = data.get('messages', data.get('data', data.get('results', [])))
        if isinstance(messages, list):
            return len(messages) > 0
        return bool(data.get('content', '').strip())

    return False


def find_earliest_message_date(messages_data):
    """Find the earliest message date from chat data"""
    if not messages_data:
        return None

    earliest_time = None
    messages = []

    if isinstance(messages_data, list):
        messages = messages_data
    elif isinstance(messages_data, dict):
        if 'messages' in messages_data and isinstance(messages_data['messages'], list):
            messages = messages_data['messages']
        else:
            messages = messages_data.get('messages', messages_data.get('data', messages_data.get('results', [])))

    if not isinstance(messages, list) or len(messages) == 0:
        return None

    for message in messages:
        if isinstance(message, dict):
            time_str = message.get('time', message.get('timestamp', message.get('createTime', '')))
            if time_str:
                try:
                    if 'T' in time_str:
                        time_obj = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                    else:
                        time_obj = datetime.fromtimestamp(float(time_str))

                    if earliest_time is None or time_obj < earliest_time:
                        earliest_time = time_obj
                except (ValueError, TypeError):
                    continue

    return earliest_time.strftime("%Y-%m-%d") if earliest_time else None


def save_chat_data(data, talker_name, month_info):
    """Save chat logs data to a JSON file in organized folder structure"""
    try:
        output_path = get_output_path(talker_name, month_info)

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

        # 使用线程安全的文件写入
        temp_path = output_path.with_suffix('.tmp')
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, indent=2, ensure_ascii=False)

        # 原子性重命名
        temp_path.rename(output_path)
        return output_path

    except Exception as e:
        print(f"Error saving to file: {e}")
        return None


# ==================== 并行处理核心函数 ====================


def process_single_month_group_parallel(talker_name, month_info, progress_tracker):
    """
    并行版本的单个任务处理函数
    线程安全，支持重试和错误处理
    """
    task_id = f"{talker_name}_{month_info['month_key']}"
    thread_name = threading.current_thread().name

    # 检查是否已处理
    if progress_tracker.is_processed(task_id):
        return True

    # 检查文件是否已存在
    output_path = get_output_path(talker_name, month_info)
    if output_path.exists():
        progress_tracker.add_processed(task_id)
        return True

    date_range = f"{month_info['start_date']}~{month_info['end_date']}"

    try:
        # 获取聊天记录
        chat_logs = fetch_chat_logs(time_range=date_range, talker=talker_name)

        if chat_logs is not None:
            if has_meaningful_data(chat_logs):
                # 更新最早日期
                earliest_date = find_earliest_message_date(chat_logs)
                if earliest_date:
                    progress_tracker.update_earliest_date(talker_name, earliest_date)

                # 保存数据
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
                    print(
                        f"✅ [{thread_name}] Saved: {talker_name}/{month_info['month_key']} ({message_count} messages)"
                    )
                    progress_tracker.add_processed(task_id)
                    return True
                else:
                    print(f"❌ [{thread_name}] Failed to save: {talker_name}/{month_info['month_key']}")
                    progress_tracker.add_failed(task_id)
                    return False
            else:
                # 无数据，标记为已处理
                progress_tracker.add_processed(task_id)
                return True
        else:
            print(f"⚠️  [{thread_name}] API failed: {talker_name}/{month_info['month_key']}")
            progress_tracker.add_failed(task_id)
            return False

    except Exception as e:
        print(f"💥 [{thread_name}] Exception in {talker_name}/{month_info['month_key']}: {e}")
        progress_tracker.add_failed(task_id)
        return False


def print_progress_stats(progress_tracker, total_tasks, start_time):
    """打印进度统计信息"""
    stats = progress_tracker.get_stats_copy()
    elapsed = datetime.now() - start_time

    processed = stats["processed_count"]
    success = stats["success_count"]
    failed = stats["failed_count"]

    if elapsed.total_seconds() > 0:
        rate_per_second = processed / elapsed.total_seconds()
        rate_per_minute = rate_per_second * 60

        remaining = total_tasks - processed
        eta_seconds = remaining / rate_per_second if rate_per_second > 0 else 0
        eta_minutes = eta_seconds / 60

        progress_pct = (processed / total_tasks * 100) if total_tasks > 0 else 0

        print(f"\n📊 Progress Stats:")
        print(f"   ✅ Completed: {success}")
        print(f"   ❌ Failed: {failed}")
        print(f"   📈 Progress: {processed}/{total_tasks} ({progress_pct:.1f}%)")
        print(f"   ⚡ Rate: {rate_per_minute:.1f}/min ({rate_per_second:.2f}/sec)")
        print(f"   ⏰ ETA: {eta_minutes:.0f} minutes")
        print(f"   🕐 Elapsed: {elapsed}")


def cleanup_empty_folders(talker_name=None):
    """Remove empty year folders and empty talker folders"""
    base_path = Path(BASE_OUTPUT_DIR)
    if not base_path.exists():
        return

    removed_count = 0

    if talker_name:
        talker_folders = [base_path / create_safe_filename(talker_name)]
    else:
        talker_folders = [f for f in base_path.iterdir() if f.is_dir()]

    for talker_folder in talker_folders:
        if not talker_folder.exists():
            continue

        year_folders = [f for f in talker_folder.iterdir() if f.is_dir() and f.name.isdigit()]

        for year_folder in year_folders:
            json_files = list(year_folder.glob("*.json"))
            if len(json_files) == 0:
                try:
                    year_folder.rmdir()
                    removed_count += 1
                except OSError:
                    pass

        remaining_items = list(talker_folder.iterdir())
        if len(remaining_items) == 0:
            try:
                talker_folder.rmdir()
                removed_count += 1
            except OSError:
                pass

    if removed_count > 0:
        print(f"🧹 Cleanup: removed {removed_count} empty folders")

    return removed_count


# ==================== 主程序（并行版本）====================


def main_parallel():
    """并行版本的主函数"""
    print("🚀 Starting PARALLEL WeChat chat history download")
    print(f"🔥 Max Workers: {MAX_WORKERS}")
    print(f"📅 Fetching last {YEARS_TO_FETCH} years of chat history")
    print(f"👥 Total names: {len(ALL_NAMES)} ({len(GROUP_NAMES)} groups + {len(INDIVIDUAL_NAMES)} individuals)")
    print(f"📁 Output directory: {BASE_OUTPUT_DIR}")
    print("-" * 60)

    # 加载进度、最早日期和联系人缓存
    progress = load_progress()
    earliest_dates_info = load_earliest_dates()
    contact_cache = load_contact_cache()

    if progress["last_updated"]:
        print(f"📋 Loaded progress (last updated: {progress['last_updated']})")
        print(f"   - Already processed: {len(progress['processed'])} items")
        print(f"   - Failed: {len(progress['failed'])} items")

    # 初始化线程安全的进度跟踪器
    progress_tracker = ThreadSafeProgressTracker(progress, earliest_dates_info)

    # 生成月份列表和任务
    months = generate_month_list()
    print(f"📅 Generated {len(months)} months from {months[0]['month_key']} to {months[-1]['month_key']}")

    # 计算整个10年窗口的日期范围
    start_date = months[-1]['start_date']  # 最早月份的开始日期
    end_date = months[0]['end_date']  # 最新月份的结束日期
    print(f"📅 10-year window: {start_date} to {end_date}")

    # 预检查：筛选出有数据的联系人
    print(f"🔍 Pre-checking {len(ALL_NAMES):,} contacts for data existence...")

    # 使用专门的联系人缓存
    cached_with_data = contact_cache["contacts_with_data"]
    cached_without_data = contact_cache["contacts_without_data"]

    if cached_with_data or cached_without_data:
        print(f"📂 Using cached results:")
        print(f"   - Cached contacts with data: {len(cached_with_data)}")
        print(f"   - Cached contacts without data: {len(cached_without_data)}")
        print(f"   - Cache age: {contact_cache['cache_age_days']} days")

    contacts_with_data = list(cached_with_data.copy())
    contacts_without_data = list(cached_without_data.copy())
    precheck_failed = []
    newly_checked_count = 0

    # 只检查未缓存的联系人
    uncached_contacts = [name for name in ALL_NAMES if name not in cached_with_data and name not in cached_without_data]

    if uncached_contacts:
        print(f"🔍 Need to check {len(uncached_contacts)} new contacts...")

        for idx, talker_name in enumerate(uncached_contacts):
            print(f"🔍 Checking new contact {idx+1}/{len(uncached_contacts)}: {talker_name}...", end=" ")

            try:
                if check_contact_has_data(talker_name, start_date, end_date):
                    contacts_with_data.append(talker_name)
                    print("✅ Has data")
                else:
                    contacts_without_data.append(talker_name)
                    print("❌ No data")
                newly_checked_count += 1
            except Exception as e:
                precheck_failed.append(talker_name)
                contacts_with_data.append(talker_name)  # Include in processing to be safe
                print(f"⚠️ Check failed, will process: {e}")

    else:
        print("✅ All contacts already cached, no new checks needed!")

    print(f"\n📊 Contact Pre-check Results:")
    print(f"   ✅ Contacts with data: {len(contacts_with_data)}")
    print(f"   ❌ Contacts without data: {len(contacts_without_data)} (skipped)")
    print(f"   ⚠️ Pre-check failed: {len(precheck_failed)} (will process)")
    print(f"   🆕 Newly checked: {newly_checked_count}")
    print(f"   📈 Data rate: {len(contacts_with_data)/len(ALL_NAMES)*100:.1f}%")

    # Show some examples of skipped contacts if any
    if len(contacts_without_data) > 0:
        print(f"   📝 Examples of skipped contacts: {', '.join(contacts_without_data[:5])}")
        if len(contacts_without_data) > 5:
            print(f"      ... and {len(contacts_without_data)-5} more")

    # 保存更新的联系人缓存（仅在有新检查时保存）
    if newly_checked_count > 0 or not os.path.exists(CONTACT_CACHE_FILE):
        save_contact_cache(set(contacts_with_data), set(contacts_without_data))
        print(f"   💾 Contact cache updated with {newly_checked_count} new checks")

    # 生成所有任务（仅针对有数据的联系人）
    print(f"📋 Generating tasks for {len(contacts_with_data):,} contacts with data...")

    # 一次性获取所有已处理的任务集合，避免重复加锁
    processed_set = progress_tracker.get_processed_set()

    all_tasks = []
    processed_contacts = 0
    total_contacts = len(contacts_with_data)

    for talker_name in contacts_with_data:
        for month_info in months:
            task_id = f"{talker_name}_{month_info['month_key']}"
            if task_id not in processed_set:
                all_tasks.append((talker_name, month_info))

        processed_contacts += 1
        # 显示进度，避免用户以为程序卡住了
        if processed_contacts % 100 == 0 or processed_contacts == total_contacts:
            progress_pct = processed_contacts / total_contacts * 100
            print(
                f"📋 Processing contacts: {processed_contacts:,}/{total_contacts:,} ({progress_pct:.1f}%) - Found {len(all_tasks):,} pending tasks so far..."
            )

    print(f"✅ Task filtering completed - {len(all_tasks):,} tasks to process")

    total_tasks = len(all_tasks)
    total_possible_all = len(ALL_NAMES) * len(months)
    total_possible_with_data = len(contacts_with_data) * len(months)
    already_done = total_possible_with_data - total_tasks
    skipped_due_to_no_data = len(contacts_without_data) * len(months)

    print(f"\n📊 Task Overview:")
    print(f"   - Total possible tasks (all contacts): {total_possible_all:,}")
    print(f"   - Tasks skipped (no data): {skipped_due_to_no_data:,}")
    print(f"   - Tasks for contacts with data: {total_possible_with_data:,}")
    print(f"   - Already completed: {already_done:,}")
    print(f"   - Remaining to process: {total_tasks:,}")
    print(f"   🚀 Optimization: {skipped_due_to_no_data/total_possible_all*100:.1f}% tasks eliminated by pre-check")

    if total_tasks == 0:
        print("\n🎉 All tasks completed! No new work to do.")
        return

    print(f"\n⚡ Starting parallel processing with {MAX_WORKERS} workers...")
    print("-" * 60)

    start_time = datetime.now()
    completed_count = 0
    last_progress_print = 0

    try:
        # 使用 ThreadPoolExecutor 进行并行处理
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 提交所有任务
            future_to_task = {
                executor.submit(process_single_month_group_parallel, talker_name, month_info, progress_tracker): (
                    talker_name,
                    month_info,
                )
                for talker_name, month_info in all_tasks
            }

            # 处理完成的任务
            for future in concurrent.futures.as_completed(future_to_task):
                talker_name, month_info = future_to_task[future]
                completed_count += 1

                try:
                    success = future.result()

                    # 定期保存进度
                    if completed_count % PROGRESS_SAVE_INTERVAL == 0 or progress_tracker.should_save_progress():
                        current_progress = progress_tracker.get_progress_copy()
                        current_earliest = progress_tracker.get_earliest_dates_copy()
                        save_progress(current_progress)
                        save_earliest_dates(current_earliest)

                    # 定期打印进度
                    if completed_count - last_progress_print >= 20:  # 每20个任务打印一次
                        print_progress_stats(progress_tracker, total_tasks, start_time)
                        last_progress_print = completed_count

                    # 简单的进度指示
                    if completed_count % 5 == 0:
                        progress_pct = completed_count / total_tasks * 100
                        print(f"⏳ Progress: {completed_count}/{total_tasks} ({progress_pct:.1f}%)")

                except Exception as e:
                    print(f"❌ Task {talker_name}-{month_info['month_key']} generated exception: {e}")

            # 添加延迟避免API过载
            time.sleep(DELAY_BETWEEN_REQUESTS)

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user. Saving progress...")
        current_progress = progress_tracker.get_progress_copy()
        current_earliest = progress_tracker.get_earliest_dates_copy()
        save_progress(current_progress)
        save_earliest_dates(current_earliest)
        print("Progress saved. You can resume by running the script again.")
        return

    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        current_progress = progress_tracker.get_progress_copy()
        current_earliest = progress_tracker.get_earliest_dates_copy()
        save_progress(current_progress)
        save_earliest_dates(current_earliest)
        print("Progress saved.")
        raise

    # 最终保存和清理
    final_progress = progress_tracker.get_progress_copy()
    final_earliest = progress_tracker.get_earliest_dates_copy()
    save_progress(final_progress)
    save_earliest_dates(final_earliest)

    print(f"\n🧹 Running final cleanup...")
    cleanup_empty_folders()

    # 最终统计
    elapsed = datetime.now() - start_time
    final_stats = progress_tracker.get_stats_copy()

    print(f"\n🏁 Parallel processing completed!")
    print_progress_stats(progress_tracker, total_tasks, start_time)

    print(f"\n📊 Final Results:")
    print(f"   - Total completed in this run: {completed_count}")
    print(
        f"   - Success rate: {(final_stats['success_count']/completed_count*100):.1f}%"
        if completed_count > 0
        else "   - No tasks processed"
    )
    print(f"   - Total time: {elapsed}")
    print(
        f"   - Average rate: {(completed_count/elapsed.total_seconds())*60:.1f} tasks/minute"
        if elapsed.total_seconds() > 0
        else ""
    )

    # 显示文件统计
    if final_stats["success_count"] > 0:
        print(f"\n📁 Files saved in: {os.path.abspath(BASE_OUTPUT_DIR)}")

        # 按组统计文件
        file_counts = {}
        for name in ALL_NAMES[:10]:  # 只显示前10个，避免输出过长
            safe_group = create_safe_filename(name)
            group_path = Path(BASE_OUTPUT_DIR) / safe_group
            if group_path.exists():
                file_count = sum(1 for _ in group_path.rglob("*.json"))
                if file_count > 0:
                    file_counts[name] = file_count

        for name, count in list(file_counts.items())[:10]:
            print(f"   - {name}: {count} monthly files")

        if len(file_counts) > 10:
            print(f"   ... and {len(file_counts)-10} more groups")

    # 显示最早日期
    if final_earliest["earliest_dates"]:
        print(f"\n📅 Earliest message dates found:")
        sorted_dates = sorted(final_earliest["earliest_dates"].items())
        for name, date in sorted_dates[:10]:  # 只显示前10个
            print(f"   - {name}: {date}")
        if len(sorted_dates) > 10:
            print(f"   ... and {len(sorted_dates)-10} more")

    print(f"\n📋 Progress file: {os.path.abspath(PROGRESS_FILE)}")
    print(f"📅 Earliest dates file: {os.path.abspath(EARLIEST_DATES_FILE)}")
    print(f"📂 Contact cache file: {os.path.abspath(CONTACT_CACHE_FILE)}")
    print(f"   💡 To re-check all contacts, delete: {CONTACT_CACHE_FILE}")

    if final_stats["failed_count"] > 0:
        print(f"\n⚠️  {final_stats['failed_count']} tasks failed. You may want to retry them later.")


if __name__ == "__main__":
    main_parallel()
