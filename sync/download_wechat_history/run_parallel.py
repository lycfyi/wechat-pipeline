#!/usr/bin/env python3
"""
并行版本快速启动脚本
提供简单的命令行界面来启动并行版本，支持不同配置
"""

import argparse
import os
import sys

# Add parent directory to Python path to allow imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from download_wechat_history.parallel_config import PERFORMANCE_PROFILES, get_auto_config, print_config_recommendation


def merge_chat_data(existing_data, new_data):
    """
    合并聊天数据，进行消息的 union 操作

    Args:
        existing_data: 现有的聊天数据 (dict)
        new_data: 新获取的聊天数据 (list or dict)

    Returns:
        dict: 合并后的聊天数据
    """
    from datetime import datetime

    # 处理新数据格式
    if isinstance(new_data, list):
        new_messages = new_data
    elif isinstance(new_data, dict):
        new_messages = new_data.get('messages', new_data.get('data', new_data.get('results', [])))
    else:
        new_messages = []

    # 获取现有消息
    existing_messages = existing_data.get('messages', [])

    # 创建消息字典用于去重，使用 seq 作为唯一标识
    message_dict = {}

    # 添加现有消息
    for msg in existing_messages:
        if isinstance(msg, dict) and 'seq' in msg:
            message_dict[msg['seq']] = msg

    # 添加新消息，自动去重
    for msg in new_messages:
        if isinstance(msg, dict) and 'seq' in msg:
            message_dict[msg['seq']] = msg

    # 按时间排序消息
    merged_messages = list(message_dict.values())
    merged_messages.sort(key=lambda x: x.get('seq', 0))

    # 计算日期范围
    if merged_messages:
        # 从消息中提取时间范围
        times = []
        for msg in merged_messages:
            time_str = msg.get('time', '')
            if time_str:
                try:
                    # 解析时间格式 "2025-08-02T13:05:49-04:00"
                    if 'T' in time_str:
                        date_part = time_str.split('T')[0]
                        times.append(date_part)
                except:
                    continue

        if times:
            times.sort()
            date_range = f"{times[0]}~{times[-1]}"
        else:
            # 如果无法解析时间，使用现有的日期范围
            date_range = existing_data.get('metadata', {}).get('date_range', '')
    else:
        date_range = existing_data.get('metadata', {}).get('date_range', '')

    # 更新元数据
    merged_data = {
        "metadata": {
            "talker_name": existing_data.get('metadata', {}).get('talker_name', ''),
            "month": existing_data.get('metadata', {}).get('month', ''),
            "date_range": date_range,
            "fetched_at": datetime.now().isoformat(),
            "message_count": len(merged_messages),
            "merged_at": datetime.now().isoformat(),
            "original_count": len(existing_messages),
            "new_count": len(new_messages),
            "final_count": len(merged_messages),
        },
        "messages": merged_messages,
    }

    return merged_data


def save_merged_chat_data(data, talker_name, month_info, base_output_dir="chat_history"):
    """
    保存合并后的聊天数据，如果文件已存在则进行合并
    """
    import json
    from datetime import datetime
    from pathlib import Path

    try:
        # 构造文件路径
        safe_name = create_safe_filename(talker_name)
        year, month = month_info['month_key'].split('-')
        output_dir = Path(base_output_dir) / safe_name / year
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"chatlog_{month_info['month_key']}.json"

        # 检查文件是否已存在
        if output_path.exists():
            print(f"📄 发现现有文件，准备合并: {output_path}")
            try:
                with open(output_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)

                # 合并数据
                merged_data = merge_chat_data(existing_data, data)

                original_count = existing_data.get('metadata', {}).get('message_count', 0)
                new_count = (
                    len(data)
                    if isinstance(data, list)
                    else len(data.get('messages', data.get('data', data.get('results', []))))
                )
                final_count = merged_data['metadata']['message_count']

                print(f"🔄 合并数据: 原有 {original_count} 条 + 新增 {new_count} 条 = 最终 {final_count} 条")

            except Exception as e:
                print(f"⚠️  读取现有文件失败，将创建新文件: {e}")
                # 如果读取失败，创建新数据
                merged_data = {
                    "metadata": {
                        "talker_name": talker_name,
                        "month": month_info['month_key'],
                        "date_range": f"{month_info['start_date']}~{month_info['end_date']}",
                        "fetched_at": datetime.now().isoformat(),
                        "message_count": (
                            len(data)
                            if isinstance(data, list)
                            else len(data.get('messages', data.get('data', data.get('results', []))))
                        ),
                    },
                    "messages": (
                        data
                        if isinstance(data, list)
                        else data.get('messages', data.get('data', data.get('results', [])))
                    ),
                }
        else:
            # 文件不存在，创建新数据
            print(f"📝 创建新文件: {output_path}")
            merged_data = {
                "metadata": {
                    "talker_name": talker_name,
                    "month": month_info['month_key'],
                    "date_range": f"{month_info['start_date']}~{month_info['end_date']}",
                    "fetched_at": datetime.now().isoformat(),
                    "message_count": (
                        len(data)
                        if isinstance(data, list)
                        else len(data.get('messages', data.get('data', data.get('results', []))))
                    ),
                },
                "messages": (
                    data if isinstance(data, list) else data.get('messages', data.get('data', data.get('results', [])))
                ),
            }

        # 保存合并后的数据
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f, indent=2, ensure_ascii=False)

        print(f"✅ 保存完成: {output_path} ({merged_data['metadata']['message_count']} 条消息)")
        return output_path

    except Exception as e:
        print(f"❌ 保存文件时出错: {e}")
        return None


def create_safe_filename(text):
    """Create a safe filename from text"""
    safe_chars = "".join(c for c in text if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
    return safe_chars.replace(' ', '_')


def find_month_and_after_tasks(progress_data, start_month_key):
    """查找指定月份及之后的所有任务"""
    if not progress_data:
        return []

    target_tasks = []

    # 将月份转换为可比较的格式 (YYYY-MM -> YYYYMM)
    start_month_num = int(start_month_key.replace('-', ''))

    # 检查已处理的任务
    for task in progress_data.get("processed", []):
        # 任务格式通常是: contact_name_YYYY-MM
        if '_' in task:
            parts = task.split('_')
            if len(parts) >= 2:
                month_part = parts[-1]  # 获取最后一部分，应该是月份
                if len(month_part) == 7 and '-' in month_part:  # YYYY-MM 格式
                    try:
                        task_month_num = int(month_part.replace('-', ''))
                        if task_month_num >= start_month_num:
                            target_tasks.append(task)
                    except ValueError:
                        continue

    # 检查失败的任务
    for task in progress_data.get("failed", []):
        if '_' in task:
            parts = task.split('_')
            if len(parts) >= 2:
                month_part = parts[-1]
                if len(month_part) == 7 and '-' in month_part:  # YYYY-MM 格式
                    try:
                        task_month_num = int(month_part.replace('-', ''))
                        if task_month_num >= start_month_num:
                            target_tasks.append(task)
                    except ValueError:
                        continue

    return target_tasks


def process_single_month_group_merge(talker_name, month_info):
    """
    处理单个月份任务的合并版本
    专门用于 force update 时的数据合并
    """
    import os
    import sys

    # 确保能导入 API 函数
    sys.path.insert(0, os.path.dirname(__file__))

    try:
        # 导入 API 函数
        from load_chat_to_local_files_parallel import fetch_chat_logs, has_meaningful_data

        date_range = f"{month_info['start_date']}~{month_info['end_date']}"
        print(f"🔄 合并模式处理 {talker_name} for {month_info['month_key']} ({date_range})...")

        # 获取聊天记录
        chat_logs = fetch_chat_logs(time_range=date_range, talker=talker_name)

        if chat_logs is not None:
            if has_meaningful_data(chat_logs):
                # 使用合并保存函数
                saved_path = save_merged_chat_data(chat_logs, talker_name, month_info)
                if saved_path:
                    return True
                else:
                    print(f"❌ 合并保存失败: {talker_name}/{month_info['month_key']}")
                    return False
            else:
                print(f"📭 无有效数据: {talker_name}/{month_info['month_key']}")
                return True
        else:
            print(f"⚠️  API请求失败: {talker_name}/{month_info['month_key']}")
            return False

    except Exception as e:
        print(f"❌ 处理异常: {talker_name}/{month_info['month_key']}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="微信聊天记录并行下载工具")
    parser.add_argument(
        "--profile",
        choices=list(PERFORMANCE_PROFILES.keys()) + ["auto"],
        default="auto",
        help="性能配置档案 (默认: auto - 自动选择)",
    )
    parser.add_argument("--workers", type=int, help="工作线程数 (覆盖配置档案设置)")
    parser.add_argument("--test", action="store_true", help="运行性能测试而不是实际下载")
    parser.add_argument("--dry-run", action="store_true", help="模拟运行，显示将要处理的任务数量")
    parser.add_argument("--config-info", action="store_true", help="显示配置信息和建议")
    parser.add_argument(
        "--force-start-month", type=str, help="强制从指定月份开始重新下载数据 (格式: YYYY-MM, 例如: 2025-01)"
    )

    args = parser.parse_args()

    # 强制从指定月份开始合并更新
    if args.force_start_month:
        import re
        from datetime import datetime

        # 验证月份格式
        if not re.match(r'^\d{4}-\d{2}$', args.force_start_month):
            print("❌ 月份格式错误，请使用 YYYY-MM 格式，例如: 2025-01")
            return 1

        try:
            # 验证日期是否有效
            datetime.strptime(args.force_start_month + "-01", "%Y-%m-%d")
        except ValueError:
            print("❌ 无效的月份，请检查年月是否正确")
            return 1

        print(f"🔄 强制从 {args.force_start_month} 开始合并更新数据...")
        print("📋 使用合并模式：新数据将与现有文件进行 union 合并")

        try:
            # 导入必要的模块
            sys.path.insert(0, os.path.dirname(__file__))
            from load_chat_to_local_files_parallel import ALL_NAMES, generate_month_list

            # 生成月份列表
            months = generate_month_list()
            target_month_key = args.force_start_month

            # 将月份转换为可比较的格式 (YYYY-MM -> YYYYMM)
            start_month_num = int(target_month_key.replace('-', ''))

            # 筛选出需要处理的月份
            target_months = []
            for month_info in months:
                month_num = int(month_info['month_key'].replace('-', ''))
                if month_num >= start_month_num:
                    target_months.append(month_info)

            print(
                f"📅 需要处理的月份: {len(target_months)} 个 (从 {target_months[-1]['month_key']} 到 {target_months[0]['month_key']})"
            )

            # 生成需要合并的任务列表
            merge_tasks = []
            for talker_name in ALL_NAMES:
                for month_info in target_months:
                    merge_tasks.append((talker_name, month_info))

            print(f"📊 总计需要合并的任务: {len(merge_tasks)} 个")
            print(f"👥 联系人数量: {len(ALL_NAMES)}")

            # 确认开始合并
            print(f"\n⚠️  即将开始合并模式处理，将重新获取并合并数据")
            print(f"   现有文件不会被删除，新数据将与现有数据合并")
            print(f"   按 Ctrl+C 可以随时停止")
            print(f"   确认开始吗？输入 'yes' 继续，其他任何键取消:")

            try:
                confirmation = input().strip().lower()
                if confirmation != 'yes':
                    print("❌ 已取消")
                    return
            except KeyboardInterrupt:
                print("\n❌ 已取消")
                return

            # 开始合并处理
            print(f"\n🚀 开始合并模式处理...")
            successful_tasks = 0
            failed_tasks = 0

            for i, (talker_name, month_info) in enumerate(merge_tasks, 1):
                print(f"\n[{i}/{len(merge_tasks)}] 处理: {talker_name} - {month_info['month_key']}")

                try:
                    success = process_single_month_group_merge(talker_name, month_info)
                    if success:
                        successful_tasks += 1
                    else:
                        failed_tasks += 1

                    # 每10个任务显示一次进度
                    if i % 10 == 0:
                        progress_pct = i / len(merge_tasks) * 100
                        print(
                            f"⏳ 进度: {i}/{len(merge_tasks)} ({progress_pct:.1f}%) - 成功: {successful_tasks}, 失败: {failed_tasks}"
                        )

                except KeyboardInterrupt:
                    print(f"\n⚠️  用户中断，已处理 {i}/{len(merge_tasks)} 个任务")
                    break
                except Exception as e:
                    print(f"❌ 任务异常: {e}")
                    failed_tasks += 1

            # 完成总结
            print(f"\n🎉 合并处理完成!")
            print(f"📊 总任务: {len(merge_tasks)}")
            print(f"✅ 成功: {successful_tasks}")
            print(f"❌ 失败: {failed_tasks}")

            return

        except Exception as e:
            print(f"❌ 无法执行合并更新: {e}")
            return 1

    # 显示配置信息
    if args.config_info:
        from full_sync_up_task.parallel_config import print_config_recommendation

        # 从数据文件加载联系人数量
        try:
            from full_sync_up_task.load_chat_to_local_files_parallel import ALL_NAMES

            total_contacts = len(ALL_NAMES)
        except ImportError:
            total_contacts = 42335  # 默认值

        print_config_recommendation(total_contacts)
        return

    # 运行性能测试
    if args.test:
        from test_parallel_performance import run_comprehensive_test

        print("🧪 运行并行性能测试...")
        run_comprehensive_test()
        return

    # 准备并行下载
    print("🚀 启动并行微信聊天记录下载")
    print("=" * 50)

    # 导入并行版本
    try:
        sys.path.append(os.path.join(os.path.dirname(__file__), 'full_sync_up_task'))
        import load_chat_to_local_files_parallel as parallel_module
    except ImportError as e:
        print(f"❌ 无法导入并行模块: {e}")
        print("请确保 load_chat_to_local_files_parallel.py 存在")
        return 1

    # 确定配置
    total_contacts = len(parallel_module.ALL_NAMES)

    if args.profile == "auto":
        profile_name = get_auto_config(total_contacts)
        print(f"🎯 自动选择配置: {profile_name} (基于 {total_contacts:,} 个联系人)")
    else:
        profile_name = args.profile
        print(f"🎯 使用指定配置: {profile_name}")

    config = PERFORMANCE_PROFILES[profile_name]

    # 应用用户指定的工作线程数
    if args.workers:
        config = config.copy()  # 创建副本避免修改原配置
        config['max_workers'] = args.workers
        print(f"🔧 工作线程数调整为: {args.workers}")

    # 更新模块配置
    parallel_module.MAX_WORKERS = config['max_workers']
    parallel_module.BATCH_SIZE = config['batch_size']
    parallel_module.DELAY_BETWEEN_REQUESTS = config['delay_between_requests']
    parallel_module.RETRY_ATTEMPTS = config['retry_attempts']
    parallel_module.PROGRESS_SAVE_INTERVAL = config['progress_save_interval']

    print(f"⚙️  配置详情:")
    print(f"   工作线程数: {config['max_workers']}")
    print(f"   批处理大小: {config['batch_size']}")
    print(f"   请求延迟: {config['delay_between_requests']}s")
    print(f"   重试次数: {config['retry_attempts']}")

    # 模拟运行
    if args.dry_run:
        months = parallel_module.generate_month_list()
        total_possible_tasks = total_contacts * len(months)

        # 模拟进度检查
        progress = parallel_module.load_progress()
        completed_tasks = len(progress.get("processed", []))
        failed_tasks = len(progress.get("failed", []))
        remaining_tasks = total_possible_tasks - completed_tasks - failed_tasks

        print(f"\n📊 任务概览 (模拟运行):")
        print(f"   总联系人: {total_contacts:,}")
        print(f"   月份数: {len(months)}")
        print(f"   总可能任务: {total_possible_tasks:,}")
        print(f"   已完成: {completed_tasks:,}")
        print(f"   已失败: {failed_tasks:,}")
        print(f"   待处理: {remaining_tasks:,}")

        # 预估时间
        if remaining_tasks > 0:
            estimated_time_hours = remaining_tasks * 0.5 / config['max_workers'] / 3600
            if estimated_time_hours < 1:
                time_str = f"{estimated_time_hours * 60:.0f} 分钟"
            elif estimated_time_hours < 24:
                time_str = f"{estimated_time_hours:.1f} 小时"
            else:
                time_str = f"{estimated_time_hours / 24:.1f} 天"

            print(f"   预估处理时间: {time_str}")

        print(f"\n💡 这是模拟运行，没有实际处理任务")
        print(f"   移除 --dry-run 参数开始实际下载")
        return

    # 最终确认
    print(f"\n⚠️  即将开始并行下载，处理 {total_contacts:,} 个联系人")
    print(f"   按 Ctrl+C 可以随时安全停止（进度会自动保存）")
    print(f"   确认开始吗？输入 'yes' 继续，其他任何键取消:")

    try:
        confirmation = input().strip().lower()
        if confirmation != 'yes':
            print("❌ 已取消")
            return
    except KeyboardInterrupt:
        print("\n❌ 已取消")
        return

    # 开始并行处理
    print("\n🚀 开始并行处理...")
    try:
        parallel_module.main_parallel()
        print("\n🎉 并行处理完成!")
    except KeyboardInterrupt:
        print("\n⚠️  用户中断，进度已保存")
    except Exception as e:
        print(f"\n💥 处理过程中发生错误: {e}")
        print("进度已保存，可以稍后重新运行")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code or 0)
