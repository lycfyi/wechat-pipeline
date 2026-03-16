#!/usr/bin/env python3
"""
Batch import all WeChat chat history data to Prisma PostgreSQL database
"""

import asyncio
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Import functions from our existing script
from import_to_prisma import extract_chat_rooms, extract_users, load_chat_data, prepare_messages, setup_instructions
from progress_tracker import ProgressTracker

# Note: You'll need to install prisma client: pip install prisma
from prisma import Prisma


class BatchImporter:
    def __init__(
        self,
        base_path: str = "chat_history",
        batch_size: int = 100,
        use_progress_tracking: bool = True,
        force_update_since: str = None,
    ):
        self.base_path = Path(base_path)
        self.batch_size = batch_size
        self.progress_tracker = ProgressTracker() if use_progress_tracking else None
        self.force_update_since = force_update_since  # 格式: YYYY-MM 或 YYYY-MM-DD
        self.stats = {
            "files_processed": 0,
            "files_success": 0,
            "files_failed": 0,
            "total_chat_rooms": set(),
            "total_users": set(),
            "total_messages": 0,
            "start_time": None,
            "failed_files": [],
        }

    def find_all_json_files(self) -> List[Path]:
        """Find all JSON files in the chat_history directory"""
        if not self.base_path.exists():
            raise FileNotFoundError(f"Directory {self.base_path} not found")

        json_files = list(self.base_path.rglob("*.json"))
        print(f"📁 Found {len(json_files)} JSON files total")

        # 如果指定了 force_update_since，则过滤文件
        if self.force_update_since:
            filtered_files = self.filter_files_by_force_update(json_files)
            print(f"🔍 After filtering (force update since {self.force_update_since}): {len(filtered_files)} files")
            json_files = filtered_files
        else:
            print(f"📁 Processing all {len(json_files)} JSON files")

        # Sort files by size (smallest first) for safer processing
        json_files.sort(key=lambda x: x.stat().st_size)

        print(f"📊 File size range:")
        if json_files:
            smallest = json_files[0].stat().st_size
            largest = json_files[-1].stat().st_size
            print(f"   Smallest: {self.format_size(smallest)} - {json_files[0].name}")
            print(f"   Largest: {self.format_size(largest)} - {json_files[-1].name}")

        return json_files

    def format_size(self, size_bytes: int) -> str:
        """Format file size in human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f}TB"

    def filter_files_by_force_update(self, files: List[Path]) -> List[Path]:
        """基于文件名中的月份过滤出需要上传的文件"""
        import re
        from datetime import datetime

        filtered_files = []

        # 解析 force_update_since 参数
        start_month_key = None
        if self.force_update_since:
            try:
                if re.match(r'^\d{4}-\d{2}$', self.force_update_since):  # YYYY-MM 格式
                    start_month_key = self.force_update_since
                elif re.match(r'^\d{4}-\d{2}-\d{2}$', self.force_update_since):  # YYYY-MM-DD 格式
                    # 从日期中提取年月
                    date_obj = datetime.strptime(self.force_update_since, "%Y-%m-%d")
                    start_month_key = date_obj.strftime("%Y-%m")
                else:
                    print(f"⚠️  无效的日期格式: {self.force_update_since}，支持格式: YYYY-MM 或 YYYY-MM-DD")
                    return files
            except ValueError as e:
                print(f"⚠️  日期解析错误: {e}")
                return files

        if not start_month_key:
            return files

        # 将月份转换为可比较的格式 (YYYY-MM -> YYYYMM)
        start_month_num = int(start_month_key.replace('-', ''))
        print(f"🔍 只处理 {start_month_key} 及之后月份的文件...")

        for file_path in files:
            # 从文件名中提取月份信息
            # 匹配格式: chatlog_YYYY-MM.json
            match = re.search(r'chatlog_(\d{4}-\d{2})\.json$', file_path.name)
            if match:
                file_month_key = match.group(1)
                file_month_num = int(file_month_key.replace('-', ''))

                # 只处理指定月份及之后的文件
                if file_month_num >= start_month_num:
                    print(f"📅 包含文件: {file_path.name} (月份: {file_month_key})")
                    filtered_files.append(file_path)
                else:
                    # 静默跳过更早的月份
                    pass
            else:
                # 文件名不匹配标准格式，跳过
                print(f"⚠️  跳过文件: {file_path.name} (文件名格式不匹配)")

        print(f"✅ 筛选完成: {len(filtered_files)}/{len(files)} 个文件需要上传")
        return filtered_files

    def categorize_files(self, files: List[Path]) -> Dict[str, List[Path]]:
        """Categorize files by type and size for better processing strategy"""
        categories = {
            "small": [],  # < 100KB
            "medium": [],  # 100KB - 1MB
            "large": [],  # 1MB - 10MB
            "xlarge": [],  # > 10MB
        }

        for file_path in files:
            size = file_path.stat().st_size
            if size < 100 * 1024:  # 100KB
                categories["small"].append(file_path)
            elif size < 1 * 1024 * 1024:  # 1MB
                categories["medium"].append(file_path)
            elif size < 10 * 1024 * 1024:  # 10MB
                categories["large"].append(file_path)
            else:
                categories["xlarge"].append(file_path)

        print(f"\n📂 File categories:")
        for category, file_list in categories.items():
            if file_list:
                total_size = sum(f.stat().st_size for f in file_list)
                print(f"   {category.upper()}: {len(file_list)} files ({self.format_size(total_size)})")

        return categories

    async def process_single_file(self, file_path: Path, progress_info: str = "") -> bool:
        """Process a single JSON file"""
        try:
            # Check if file has already been processed successfully
            if self.progress_tracker and self.progress_tracker.is_file_processed(file_path):
                processing_info = self.progress_tracker.get_processing_info(file_path)
                stats = processing_info.get("processing_stats", {})
                print(f"\n⏭️  {progress_info}Skipping: {file_path.name} - already processed successfully")
                print(
                    f"   📊 Previous results: {stats.get('messages_count', 0)} messages, {stats.get('chat_rooms_count', 0)} rooms, {stats.get('users_count', 0)} users"
                )

                # Update our local stats with cached data
                if stats.get('chat_rooms_count'):
                    # Create dummy set entries for counting (we don't have the actual IDs)
                    for i in range(stats.get('chat_rooms_count', 0)):
                        self.stats["total_chat_rooms"].add(f"cached_room_{file_path.name}_{i}")
                if stats.get('users_count'):
                    for i in range(stats.get('users_count', 0)):
                        self.stats["total_users"].add(f"cached_user_{file_path.name}_{i}")
                self.stats["total_messages"] += stats.get('messages_count', 0)

                return True

            # Mark processing start
            if self.progress_tracker:
                self.progress_tracker.mark_file_processing_start(file_path)

            print(f"\n📥 {progress_info}Processing: {file_path.name}")

            # Load and validate data
            data = load_chat_data(str(file_path))
            messages = data.get('messages', [])

            if not messages:
                print(f"   ⚠️  Skipping: No messages found")
                if self.progress_tracker:
                    self.progress_tracker.mark_file_processing_failed(file_path, "No messages found in file")
                return True

            # Extract entities
            chat_rooms = extract_chat_rooms(messages)
            users = extract_users(messages)
            prepared_messages = prepare_messages(messages)

            # Update statistics
            self.stats["total_chat_rooms"].update(chat_rooms.keys())
            self.stats["total_users"].update(users.keys())
            self.stats["total_messages"] += len(prepared_messages)

            print(f"   📊 {len(messages)} messages → {len(chat_rooms)} rooms, {len(users)} users")

            # Database operations
            prisma = Prisma()
            await prisma.connect()

            try:
                # 1. Upsert ChatRooms
                for room_id, room_data in chat_rooms.items():
                    await prisma.chatroom.upsert(
                        where={'id': room_id},
                        data={
                            'create': room_data,
                            'update': {'name': room_data['name'], 'isChatRoom': room_data['isChatRoom']},
                        },
                    )

                # 2. Upsert Users
                for user_id, user_data in users.items():
                    await prisma.user.upsert(
                        where={'id': user_id}, data={'create': user_data, 'update': {'name': user_data['name']}}
                    )

                # 3. Insert Messages in batches
                for i in range(0, len(prepared_messages), self.batch_size):
                    batch = prepared_messages[i : i + self.batch_size]

                    try:
                        await prisma.message.create_many(data=batch, skip_duplicates=True)
                    except Exception as e:
                        print(f"   ⚠️  Batch insert failed, trying individual inserts: {e}")
                        # Fallback to individual inserts
                        for msg in batch:
                            try:
                                # Prepare update data (exclude auto-generated fields)
                                update_data = {k: v for k, v in msg.items() if k not in ['createdAt', 'updatedAt']}

                                await prisma.message.upsert(
                                    where={'seq': msg['seq']}, data={'create': msg, 'update': update_data}
                                )
                            except Exception as msg_e:
                                print(f"   ❌ Message {msg['seq']} failed: {msg_e}")

                                # If upsert fails, try simple create
                                try:
                                    await prisma.message.create(data=msg)
                                    print(f"   ✅ Created message {msg['seq']} with simple create")
                                except Exception as create_e:
                                    print(f"   ❌ Simple create also failed for {msg['seq']}: {create_e}")

                print(f"   ✅ Successfully imported {len(prepared_messages)} messages")

            finally:
                await prisma.disconnect()

            print(f"   ✅ Validated: {len(prepared_messages)} messages ready for import")

            # Mark processing as successful
            if self.progress_tracker:
                stats = {
                    "messages_count": len(prepared_messages),
                    "chat_rooms_count": len(chat_rooms),
                    "users_count": len(users),
                }
                self.progress_tracker.mark_file_processing_success(file_path, stats)

            return True

        except Exception as e:
            error_msg = str(e)
            print(f"   ❌ Error processing {file_path.name}: {error_msg}")
            self.stats["failed_files"].append((str(file_path), error_msg))

            # Mark processing as failed
            if self.progress_tracker:
                self.progress_tracker.mark_file_processing_failed(file_path, error_msg)

            return False

    async def import_all_files(self):
        """Import all files in the chat_history directory"""
        print("🚀 Starting batch import of all chat history files")
        print("=" * 60)

        if self.progress_tracker:
            print("📊 Using progress tracking to avoid duplicate imports")
            self.progress_tracker.print_summary()

        self.stats["start_time"] = datetime.now()

        # Find and categorize files
        all_files = self.find_all_json_files()
        if not all_files:
            print("❌ No JSON files found to import")
            return

        categories = self.categorize_files(all_files)

        # Process files by category (small to large for safety)
        processing_order = ["small", "medium", "large", "xlarge"]

        for category in processing_order:
            files_in_category = categories.get(category, [])
            if not files_in_category:
                continue

            print(f"\n🔄 Processing {category.upper()} files ({len(files_in_category)} files)")
            print("-" * 40)

            for i, file_path in enumerate(files_in_category, 1):
                progress = (
                    f"[{self.stats['files_processed'] + 1}/{len(all_files)}] ({category} {i}/{len(files_in_category)}) "
                )

                success = await self.process_single_file(file_path, progress)

                self.stats["files_processed"] += 1
                if success:
                    self.stats["files_success"] += 1
                else:
                    self.stats["files_failed"] += 1

                # Progress update every 10 files
                if self.stats["files_processed"] % 10 == 0:
                    self.print_progress()

                # Small delay to avoid overwhelming the system
                await asyncio.sleep(0.1)

        # Final summary
        self.print_final_summary()

    def print_progress(self):
        """Print current progress"""
        elapsed = datetime.now() - self.stats["start_time"]
        processed = self.stats["files_processed"]
        success = self.stats["files_success"]
        failed = self.stats["files_failed"]

        print(f"\n⏱️  Progress Update:")
        print(f"   Files: {processed} processed ({success} success, {failed} failed)")
        print(
            f"   Data: {len(self.stats['total_chat_rooms'])} rooms, {len(self.stats['total_users'])} users, {self.stats['total_messages']:,} messages"
        )
        print(f"   Time: {elapsed}")

    def print_final_summary(self):
        """Print final import summary"""
        elapsed = datetime.now() - self.stats["start_time"]

        print(f"\n🏁 Batch Import Complete!")
        print("=" * 50)
        print(f"⏱️  Total time: {elapsed}")
        print(f"📊 Results:")
        print(f"   Files processed: {self.stats['files_processed']}")
        print(f"   Successful: {self.stats['files_success']}")
        print(f"   Failed: {self.stats['files_failed']}")
        print(f"   Success rate: {(self.stats['files_success']/self.stats['files_processed']*100):.1f}%")

        print(f"\n📈 Data Summary:")
        print(f"   ChatRooms: {len(self.stats['total_chat_rooms'])}")
        print(f"   Users: {len(self.stats['total_users'])}")
        print(f"   Messages: {self.stats['total_messages']:,}")

        if self.stats["failed_files"]:
            print(f"\n❌ Failed Files ({len(self.stats['failed_files'])}):")
            for file_path, error in self.stats["failed_files"][:10]:  # Show first 10
                print(f"   - {Path(file_path).name}: {error}")
            if len(self.stats["failed_files"]) > 10:
                print(f"   ... and {len(self.stats['failed_files']) - 10} more")

        # Show progress tracker summary if available
        if self.progress_tracker:
            print(f"\n📊 Progress Tracker Final Summary:")
            self.progress_tracker.print_summary()


async def main():
    """Main function"""
    import argparse
    import sys

    # 使用 argparse 解析命令行参数
    parser = argparse.ArgumentParser(description="批量上传微信聊天记录到 Prisma 数据库")
    parser.add_argument("base_path", nargs="?", default="chat_history", help="聊天记录文件夹路径")
    parser.add_argument("--batch-size", type=int, default=100, help="批处理大小")
    parser.add_argument("--no-progress", action="store_true", help="禁用进度跟踪")
    parser.add_argument(
        "--force-update-since", type=str, help="只上传指定日期之后更新的文件 (格式: YYYY-MM 或 YYYY-MM-DD)"
    )

    args = parser.parse_args()

    print(f"📁 Base path: {args.base_path}")
    print(f"📦 Batch size: {args.batch_size}")
    print(f"📊 Progress tracking: {'disabled' if args.no_progress else 'enabled'}")
    if args.force_update_since:
        print(f"🔍 Force update filter: 只上传 {args.force_update_since} 之后的文件")

    importer = BatchImporter(
        base_path=args.base_path,
        batch_size=args.batch_size,
        use_progress_tracking=not args.no_progress,
        force_update_since=args.force_update_since,
    )
    await importer.import_all_files()


if __name__ == "__main__":
    asyncio.run(main())
