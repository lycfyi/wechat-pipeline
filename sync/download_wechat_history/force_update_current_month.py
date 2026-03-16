#!/usr/bin/env python3
"""
强制更新当前月份数据的工具脚本

这个脚本解决了当前月份数据不更新的问题：
- 删除当前月份的进度记录
- 可选择删除当前月份的文件
- 重新下载当前月份的数据

用法:
    python force_update_current_month.py --help
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to Python path to allow imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def load_json_file(file_path):
    """加载 JSON 文件"""
    if not os.path.exists(file_path):
        return None

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, Exception) as e:
        print(f"❌ 无法加载文件 {file_path}: {e}")
        return None


def save_json_file(file_path, data):
    """保存 JSON 文件"""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"❌ 无法保存文件 {file_path}: {e}")
        return False


def get_current_month_key():
    """获取当前月份的键值"""
    now = datetime.now()
    return f"{now.year}-{now.month:02d}"


def create_safe_filename(text):
    """Create a safe filename from text"""
    safe_chars = "".join(c for c in text if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
    return safe_chars.replace(' ', '_')


def find_current_month_tasks(progress_data, current_month_key):
    """查找当前月份的任务"""
    if not progress_data:
        return []

    current_month_tasks = []

    # 检查已处理的任务
    for task in progress_data.get("processed", []):
        if task.endswith(f"_{current_month_key}"):
            current_month_tasks.append(task)

    # 检查失败的任务
    for task in progress_data.get("failed", []):
        if task.endswith(f"_{current_month_key}"):
            current_month_tasks.append(task)

    return current_month_tasks


def find_current_month_files(base_output_dir, current_month_key):
    """查找当前月份的文件"""
    base_path = Path(base_output_dir)
    if not base_path.exists():
        return []

    current_month_files = []
    filename_pattern = f"chatlog_{current_month_key}.json"

    # 递归查找所有当前月份的文件
    for file_path in base_path.rglob(filename_pattern):
        current_month_files.append(file_path)

    return current_month_files


def remove_progress_entries(progress_data, tasks_to_remove):
    """从进度数据中移除指定任务"""
    removed_count = 0

    # 从已处理列表中移除
    if "processed" in progress_data:
        original_count = len(progress_data["processed"])
        progress_data["processed"] = [task for task in progress_data["processed"] if task not in tasks_to_remove]
        removed_count += original_count - len(progress_data["processed"])

    # 从失败列表中移除
    if "failed" in progress_data:
        original_count = len(progress_data["failed"])
        progress_data["failed"] = [task for task in progress_data["failed"] if task not in tasks_to_remove]
        removed_count += original_count - len(progress_data["failed"])

    return removed_count


def main():
    parser = argparse.ArgumentParser(description="强制更新当前月份的微信聊天记录")
    parser.add_argument("--delete-files", action="store_true", help="删除当前月份的现有文件（谨慎使用）")
    parser.add_argument("--dry-run", action="store_true", help="模拟运行，显示将要执行的操作但不实际执行")
    parser.add_argument(
        "--progress-file",
        default="chat_download_progress.json",
        help="进度文件路径 (默认: chat_download_progress.json)",
    )
    parser.add_argument("--output-dir", default="chat_history", help="输出目录 (默认: chat_history)")

    args = parser.parse_args()

    print("🔄 强制更新当前月份数据工具")
    print("=" * 50)

    # 获取当前月份
    current_month_key = get_current_month_key()
    print(f"📅 当前月份: {current_month_key}")

    # 加载进度文件
    print(f"📋 检查进度文件: {args.progress_file}")
    progress_data = load_json_file(args.progress_file)

    if progress_data is None:
        print("❌ 无法加载进度文件")
        return 1

    # 查找当前月份的任务
    current_month_tasks = find_current_month_tasks(progress_data, current_month_key)
    print(f"📊 找到 {len(current_month_tasks)} 个当前月份的任务记录")

    if current_month_tasks:
        print("📝 当前月份任务示例:")
        for task in current_month_tasks[:5]:  # 只显示前5个
            print(f"   - {task}")
        if len(current_month_tasks) > 5:
            print(f"   ... 还有 {len(current_month_tasks) - 5} 个任务")

    # 查找当前月份的文件
    print(f"📁 检查输出目录: {args.output_dir}")
    current_month_files = find_current_month_files(args.output_dir, current_month_key)
    print(f"📊 找到 {len(current_month_files)} 个当前月份的文件")

    if current_month_files:
        print("📄 当前月份文件示例:")
        for file_path in current_month_files[:5]:  # 只显示前5个
            stat = file_path.stat()
            mod_time = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            size_kb = stat.st_size / 1024
            print(f"   - {file_path} ({size_kb:.1f}KB, 修改时间: {mod_time})")
        if len(current_month_files) > 5:
            print(f"   ... 还有 {len(current_month_files) - 5} 个文件")

    # 检查是否有任何操作需要执行
    if not current_month_tasks and not current_month_files:
        print("\n✅ 没有找到当前月份的进度记录或文件，无需清理")
        return 0

    if args.dry_run:
        print(f"\n🔍 模拟运行 - 以下是将要执行的操作:")

        if current_month_tasks:
            print(f"   🗑️  将从进度文件中移除 {len(current_month_tasks)} 个任务记录")

        if args.delete_files and current_month_files:
            print(f"   🗑️  将删除 {len(current_month_files)} 个当前月份的文件")
        elif current_month_files:
            print(f"   ⚠️  保留 {len(current_month_files)} 个当前月份的文件 (使用 --delete-files 删除)")

        print(f"\n💡 移除 --dry-run 参数来执行实际操作")
        return 0

    # 执行实际操作
    print(f"\n⚠️  即将执行以下操作:")
    if current_month_tasks:
        print(f"   - 从进度文件中移除 {len(current_month_tasks)} 个任务记录")
    if args.delete_files and current_month_files:
        print(f"   - 删除 {len(current_month_files)} 个当前月份的文件")

    print(f"\n确认执行吗？输入 'yes' 继续，其他任何键取消:")

    try:
        confirmation = input().strip().lower()
        if confirmation != 'yes':
            print("❌ 已取消")
            return 0
    except KeyboardInterrupt:
        print("\n❌ 已取消")
        return 0

    operations_performed = 0

    # 从进度文件中移除当前月份的任务
    if current_month_tasks:
        print(f"\n🗑️  移除进度记录...")
        removed_count = remove_progress_entries(progress_data, current_month_tasks)

        if removed_count > 0:
            # 保存更新后的进度文件
            if save_json_file(args.progress_file, progress_data):
                print(f"✅ 从进度文件中移除了 {removed_count} 个任务记录")
                operations_performed += 1
            else:
                print(f"❌ 无法保存进度文件")
                return 1
        else:
            print(f"⚠️  没有实际移除任何进度记录")

    # 删除当前月份的文件（如果指定）
    if args.delete_files and current_month_files:
        print(f"\n🗑️  删除文件...")
        deleted_count = 0

        for file_path in current_month_files:
            try:
                file_path.unlink()
                print(f"   ✅ 删除: {file_path}")
                deleted_count += 1
            except Exception as e:
                print(f"   ❌ 无法删除 {file_path}: {e}")

        if deleted_count > 0:
            print(f"✅ 删除了 {deleted_count} 个文件")
            operations_performed += 1

    # 总结
    print(f"\n🎉 操作完成！")
    if operations_performed > 0:
        print(f"📝 执行了 {operations_performed} 项操作")
        print(f"💡 现在可以重新运行并行下载脚本来下载当前月份的最新数据")
        print(f"   命令: python download_wechat_history/run_parallel.py")
    else:
        print(f"⚠️  没有执行任何操作")

    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code or 0)
