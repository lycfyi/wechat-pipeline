#!/usr/bin/env python3
"""
Fetch WeChat API data and save to JSON files
获取微信API数据并保存为JSON文件
"""

import csv
import json
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional

import requests


def parse_session_data(text: str) -> List[Dict]:
    """
    解析会话数据的特殊格式
    格式: 用户名(群ID) 时间戳
          消息内容
          空行
    """
    lines = text.strip().split('\n')
    messages = []
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        # 跳过空行
        if not line:
            i += 1
            continue

        # 检查是否是用户信息行 (包含括号和时间戳)
        if '(' in line and ')' in line and any(char.isdigit() for char in line):
            # 解析用户信息
            try:
                # 格式: username(chatroom_id) timestamp
                parts = line.rsplit(' ', 2)  # 从右边分割，获取最后的日期时间
                if len(parts) >= 3:
                    user_info = ' '.join(parts[:-2])
                    date_part = parts[-2]
                    time_part = parts[-1]
                    timestamp = f"{date_part} {time_part}"

                    # 提取用户名和群ID
                    if '(' in user_info and ')' in user_info:
                        username = user_info[: user_info.rfind('(')].strip()
                        chatroom_id = user_info[user_info.rfind('(') + 1 : user_info.rfind(')')].strip()
                    else:
                        username = user_info
                        chatroom_id = ""

                    # 收集消息内容（下一行开始，直到空行或下一个用户信息）
                    content_lines = []
                    i += 1
                    while i < len(lines):
                        content_line = lines[i]
                        # 如果遇到空行或下一个用户信息行，停止收集
                        if not content_line.strip():
                            break
                        if '(' in content_line and ')' in content_line and any(char.isdigit() for char in content_line):
                            # 这可能是下一个用户信息行，回退一步
                            i -= 1
                            break
                        content_lines.append(content_line)
                        i += 1

                    message = {
                        'username': username,
                        'chatroom_id': chatroom_id,
                        'timestamp': timestamp,
                        'content': '\n'.join(content_lines).strip(),
                    }
                    messages.append(message)
                else:
                    i += 1
            except Exception as e:
                print(f"   ⚠️  解析用户信息行出错: {line[:100]}... - {e}")
                i += 1
        else:
            i += 1

    return messages


def parse_csv_to_dict(csv_text: str) -> List[Dict]:
    """
    将CSV文本解析为字典列表

    Args:
        csv_text: CSV格式的文本数据

    Returns:
        字典列表，每行数据作为一个字典
    """
    if not csv_text.strip():
        return []

    # 使用StringIO来模拟文件对象，处理包含换行符的字段
    csv_file = StringIO(csv_text)
    reader = csv.DictReader(csv_file, quoting=csv.QUOTE_ALL)

    # 转换为列表，处理可能的编码问题
    result = []
    for row in reader:
        # 清理字段中的特殊字符
        cleaned_row = {}
        for key, value in row.items():
            if key and value:
                # 清理键和值中的换行符和特殊字符
                clean_key = str(key).strip().replace('\n', '').replace('\r', '')
                clean_value = str(value).strip().replace('\n', ' ').replace('\r', ' ')
                cleaned_row[clean_key] = clean_value
            elif key:
                cleaned_row[str(key).strip()] = str(value) if value else ""

        if cleaned_row:  # 只添加非空行
            result.append(cleaned_row)

    return result


def fetch_and_save_api_data(
    base_url: str, output_dir: str = "api_data", headers: Optional[Dict] = None, save_format: str = "json"
):
    """
    获取三个API接口的CSV数据并保存为文件

    Args:
        base_url: API的基础URL，例如 "http://localhost:8080"
        output_dir: 输出目录
        headers: 请求头，如果需要认证等
        save_format: 保存格式 ("csv", "json", "both")
    """

    # 确保输出目录存在
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # API接口
    apis = {
        "contact": "/api/v1/contact",  # 联系人列表
        "chatroom": "/api/v1/chatroom",  # 群聊列表
        # "session": "/api/v1/session",  # 会话列表 - 暂时不需要
    }

    # 生成时间戳用于文件命名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"🚀 开始获取API数据，基础URL: {base_url}")
    print(f"📁 输出目录: {output_path.absolute()}")

    results = {}

    for api_name, endpoint in apis.items():
        print(f"\n📡 正在获取 {api_name} 数据...")

        try:
            url = f"{base_url.rstrip('/')}{endpoint}"
            print(f"   URL: {url}")

            # 发送GET请求
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()

            # 获取CSV文本响应
            csv_text = response.text

            # 解析数据 - 根据API类型选择不同的解析器
            try:
                if api_name == "session":
                    # session API返回的是特殊的聊天记录格式
                    data_list = parse_session_data(csv_text)
                    print(f"   🔧 使用session专用解析器")
                else:
                    # contact和chatroom使用标准CSV解析
                    data_list = parse_csv_to_dict(csv_text)
                    print(f"   🔧 使用标准CSV解析器")
            except Exception as parse_error:
                print(f"   ⚠️  数据解析出错: {parse_error}")
                print(f"   📄 原始数据前300字符: {csv_text[:300]}...")
                data_list = []

            saved_files = []

            # 保存CSV文件
            if save_format in ["csv", "both"]:
                csv_filename = f"{api_name}_{timestamp}.csv"
                csv_file_path = output_path / csv_filename

                with open(csv_file_path, 'w', encoding='utf-8', newline='') as f:
                    f.write(csv_text)

                saved_files.append(str(csv_file_path))
                print(f"   ✅ CSV保存到: {csv_file_path}")

            # 保存JSON文件（转换后的）
            if save_format in ["json", "both"]:
                json_filename = f"{api_name}_{timestamp}.json"
                json_file_path = output_path / json_filename

                with open(json_file_path, 'w', encoding='utf-8') as f:
                    json.dump(data_list, f, ensure_ascii=False, indent=2)

                saved_files.append(str(json_file_path))
                print(f"   ✅ JSON保存到: {json_file_path}")

            # 显示数据概要
            print(f"   📊 数据条数: {len(data_list)}")
            if data_list:
                print(f"   📋 字段: {list(data_list[0].keys())}")

            results[api_name] = {
                'success': True,
                'files': saved_files,
                'record_count': len(data_list),
                'fields': list(data_list[0].keys()) if data_list else [],
            }

        except requests.exceptions.RequestException as e:
            print(f"   ❌ 请求失败: {e}")
            results[api_name] = {'success': False, 'error': str(e)}
        except csv.Error as e:
            print(f"   ❌ CSV解析失败: {e}")
            results[api_name] = {'success': False, 'error': f"CSV解析错误: {e}"}
        except Exception as e:
            print(f"   ❌ 未知错误: {e}")
            results[api_name] = {'success': False, 'error': f"未知错误: {e}"}

    # 保存执行结果摘要
    summary_file = output_path / f"fetch_summary_{timestamp}.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump({'timestamp': timestamp, 'base_url': base_url, 'results': results}, f, ensure_ascii=False, indent=2)

    print(f"\n📋 执行摘要已保存到: {summary_file}")

    # 显示总结
    successful = sum(1 for r in results.values() if r['success'])
    total = len(results)
    print(f"\n🎯 总结: {successful}/{total} 个API调用成功")

    return results


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='获取微信API数据并保存为文件')
    parser.add_argument('base_url', help='API基础URL，例如: http://localhost:8080')
    parser.add_argument('--output-dir', '-o', default='api_data', help='输出目录 (默认: api_data)')
    parser.add_argument(
        '--format',
        '-f',
        choices=['csv', 'json', 'both'],
        default='both',
        help='保存格式: csv (仅CSV), json (仅JSON), both (两种格式)',
    )
    parser.add_argument('--token', '-t', help='认证token (如果需要)')
    parser.add_argument('--header', '-H', action='append', help='自定义请求头，格式: "key: value"')

    args = parser.parse_args()

    # 处理请求头
    headers = {}
    if args.token:
        headers['Authorization'] = f'Bearer {args.token}'

    if args.header:
        for header in args.header:
            if ':' in header:
                key, value = header.split(':', 1)
                headers[key.strip()] = value.strip()

    # 执行API数据获取
    try:
        results = fetch_and_save_api_data(
            base_url=args.base_url,
            output_dir=args.output_dir,
            headers=headers if headers else None,
            save_format=args.format,
        )

        # 如果有失败的API调用，退出码为1
        if any(not r['success'] for r in results.values()):
            exit(1)

    except KeyboardInterrupt:
        print("\n❌ 用户中断操作")
        exit(1)
    except Exception as e:
        print(f"\n❌ 程序执行失败: {e}")
        exit(1)


if __name__ == "__main__":
    main()
