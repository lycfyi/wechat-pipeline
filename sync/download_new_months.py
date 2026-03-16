#!/usr/bin/env python3
"""
精准下载脚本：只对活跃群（有近期数据的）下载新月份
避免对 18K 群全量扫描，只处理有 2026-02 数据的 ~250 个活跃群
"""
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from download_wechat_history.load_chat_to_local_files import (
    fetch_chat_logs,
    has_meaningful_data,
    save_chat_data,
    create_safe_filename,
    cleanup_empty_folders,
)

BASE_OUTPUT_DIR = Path("chat_history")
BASE_URL = "http://localhost:5030"
DELAY = 0.3  # seconds between requests

def get_active_talkers(reference_month="2026-02"):
    """从有近期数据的文件夹中获取活跃对话名称"""
    pattern = f"chatlog_{reference_month}.json"
    talkers = []
    for folder in BASE_OUTPUT_DIR.iterdir():
        if not folder.is_dir():
            continue
        # 找该 talker 的所有月份文件，提取 talker 名称
        for f in folder.rglob(pattern):
            # 从文件夹名反推 talker 名（只需要文件夹名称）
            talker_dir = f.parent.parent  # chat_history/{talker_name}/
            talkers.append(talker_dir.name)
            break
    return list(set(talkers))

def get_talker_name_from_dir(dir_name, chatroom_json="api_data/chatroom_latest.json"):
    """从目录名（safe filename）尝试反查真实群名"""
    # 简单策略：目录名本身就是 talker 名（用于 API 查询）
    # safe_filename 只是替换了特殊字符，群名作为 API 参数需要原始名
    # 由于无法完美反查，直接用目录名查 API
    return dir_name.replace('_', ' ')

def generate_months_to_download():
    """生成需要下载的月份列表（2026-03 到当前月）"""
    months = []
    today = datetime.now().date()
    current = datetime(2026, 3, 1).date()
    
    while current <= today:
        year = current.year
        month = current.month
        
        if month == 12:
            next_month = datetime(year + 1, 1, 1).date()
        else:
            next_month = datetime(year, month + 1, 1).date()
        
        month_end = min(next_month - timedelta(days=1), today)
        
        months.append({
            'year': year,
            'month': month,
            'start_date': current.strftime("%Y-%m-%d"),
            'end_date': month_end.strftime("%Y-%m-%d"),
            'month_key': f"{year}-{month:02d}",
        })
        current = next_month
    
    return months

def main():
    print("🎯 精准增量下载：只处理活跃群的新月份")
    print("=" * 60)
    
    # 获取活跃群列表
    active_dirs = get_active_talkers("2026-02")
    print(f"📊 活跃群数量（有 2026-02 数据）: {len(active_dirs)}")
    
    # 从 chatroom API 获取真实群名映射
    # 读取最新的 chatroom JSON 文件
    import glob
    chatroom_files = sorted(glob.glob("api_data/chatroom_*.json"), reverse=True)
    chatroom_map = {}  # safe_name -> real_name
    
    if chatroom_files:
        with open(chatroom_files[0]) as f:
            chatrooms = json.load(f)
        for room in chatrooms:
            nickname = room.get('NickName', '').strip()
            if nickname:
                safe = create_safe_filename(nickname)
                chatroom_map[safe] = nickname
    
    # 同样处理联系人
    contact_files = sorted(glob.glob("api_data/contact_*.json"), reverse=True)
    if contact_files:
        with open(contact_files[0]) as f:
            contacts = json.load(f)
        for contact in contacts:
            username = contact.get('UserName', '').strip()
            if username:
                safe = create_safe_filename(username)
                chatroom_map[safe] = username
    
    print(f"📋 群名映射表: {len(chatroom_map)} 条")
    
    # 生成需要下载的月份
    months = generate_months_to_download()
    print(f"📅 需要下载的月份: {[m['month_key'] for m in months]}")
    
    if not months:
        print("✅ 没有需要下载的新月份！")
        return
    
    total_tasks = len(active_dirs) * len(months)
    print(f"📊 总任务: {len(active_dirs)} 群 × {len(months)} 月 = {total_tasks}")
    print("-" * 60)
    
    success = 0
    skipped = 0
    failed = 0
    
    start_time = datetime.now()
    
    for i, dir_name in enumerate(sorted(active_dirs)):
        # 获取真实 talker 名
        talker_name = chatroom_map.get(dir_name, dir_name)
        
        for month_info in months:
            # 检查文件是否已存在
            output_path = BASE_OUTPUT_DIR / dir_name / str(month_info['year']) / f"chatlog_{month_info['month_key']}.json"
            
            if output_path.exists():
                skipped += 1
                continue
            
            date_range = f"{month_info['start_date']}~{month_info['end_date']}"
            
            try:
                chat_logs = fetch_chat_logs(
                    base_url=BASE_URL,
                    time_range=date_range,
                    talker=talker_name
                )
                
                if chat_logs and has_meaningful_data(chat_logs):
                    saved = save_chat_data(chat_logs, talker_name, month_info)
                    if saved:
                        msg_count = len(chat_logs) if isinstance(chat_logs, list) else len(chat_logs.get('messages', []))
                        print(f"  ✅ [{i+1}/{len(active_dirs)}] {talker_name} {month_info['month_key']}: {msg_count} 条")
                        success += 1
                    else:
                        failed += 1
                else:
                    # 无数据，跳过
                    skipped += 1
                
                time.sleep(DELAY)
                
            except Exception as e:
                print(f"  ❌ {talker_name} {month_info['month_key']}: {e}")
                failed += 1
    
    elapsed = datetime.now() - start_time
    print("\n" + "=" * 60)
    print(f"🏁 下载完成！耗时: {elapsed}")
    print(f"   ✅ 有数据: {success}")
    print(f"   ⏭️  跳过(无数据/已存在): {skipped}")
    print(f"   ❌ 失败: {failed}")
    
    # 清理空文件夹
    cleanup_empty_folders()
    
    # 输出最终文件统计
    print("\n📊 最终文件统计:")
    for month in ['2026-02', '2026-03', '2026-04', '2026-05']:
        count = len(list(BASE_OUTPUT_DIR.rglob(f"chatlog_{month}.json")))
        print(f"   {month}: {count} 个文件")

if __name__ == "__main__":
    main()
