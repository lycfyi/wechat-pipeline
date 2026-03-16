import json
import os
import asyncio
import aiohttp
from datetime import datetime
from typing import List


class AsyncWeChatLogProcessor:
    def __init__(self, api_key: str, base_url: str = "https://api.apicore.ai/v1", max_concurrent: int = 50):
        self.api_key = api_key
        self.base_url = base_url
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.json_schema = {
            "type": "object",
            "properties": {
                "group_info": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "date": {"type": "string"},
                        "total_messages": {"type": "integer"},
                        "valuable_messages": {"type": "integer"}
                    },
                    "required": ["name", "date", "total_messages", "valuable_messages"],
                    "additionalProperties": False
                },
                "valuable_information": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "time_range": {"type": "string"},
                            "participants": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                            "background": {"type": "string"},
                            "core_information": {"type": "string"},
                            "discussion_result": {"type": "string"},
                            "shared_resources": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "type": {"type": "string"},
                                        "name": {"type": "string"},
                                        "url": {"type": "string"},
                                        "description": {"type": "string"},
                                        "shared_by": {"type": "string"}
                                    },
                                    "required": ["type", "name", "shared_by"],
                                    "additionalProperties": False
                                }
                            },
                            "tags": {
                                "type": "array",
                                "items": {"type": "string"}
                            }
                        },
                        "required": ["time_range", "participants", "background", "core_information", "discussion_result", "shared_resources", "tags"],
                        "additionalProperties": False
                    }
                },
                "summary": {
                    "type": "object",
                    "properties": {
                        "key_topics": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "actionable_insights": {"type": "integer"},
                        "resource_count": {"type": "integer"},
                        "most_active_contributor": {"type": "string"}
                    },
                    "required": ["key_topics", "actionable_insights", "resource_count"],
                    "additionalProperties": False
                }
            },
            "required": ["group_info", "valuable_information", "summary"],
            "additionalProperties": False
        }
    
    def extract_messages_from_json(self, file_path: str):
        """提取JSON文件中的微信消息"""
        print(f"📖 读取文件: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        # 提取时间、发送者、内容
        messages = []
        for item in raw_data:
            if item.get('content') and item.get('type') == 1:  # 只处理文本消息
                message = {
                    'time': item['time'],
                    'sender_name': item['senderName'],
                    'content': item['content']
                }
                messages.append(message)
        
        # 从文件路径提取群名和日期
        path_parts = file_path.split('/')
        group_name = path_parts[-2]  # 群名
        date = path_parts[-1].replace('.json', '')  # 日期
        
        return {
            'file_path': file_path,
            'group_name': group_name,
            'date': date,
            'messages': messages,
            'total_messages': len(messages)
        }
    
    async def send_to_reverse_api_async(self, session: aiohttp.ClientSession, extracted_data):
        """异步发送数据到逆向API"""
        async with self.semaphore:
            try:
                print(f"🚀 发送数据到逆向API: {extracted_data['group_name']} ({extracted_data['date']})")
                
                # 格式化消息文本
                message_text = "\n".join([
                    f"[{msg['time']}] {msg['sender_name']}: {msg['content']}" 
                    for msg in extracted_data['messages']
                ])
                
                # 构建API请求
                url = f"{self.base_url}/chat/completions"
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}"
                }
                
                data = {
                    "model": "gpt-4o",
                    "stream": False,
                    "response_format": {
                        "type": "json_schema",
                        "json_schema": {
                            "name": "wechat_analysis",
                            "strict": True,
                            "schema": self.json_schema
                        }
                    },
                    "messages": [
                        {
                            "role": "system", 
                            "content": "你是一个微信群聊信息提取专家。请从群聊记录中提取有价值的信息，忽略闲聊和无关内容。分析标准：1. 重点关注：技术讨论、商业机会、资源分享、行业动态、具体建议、有明确结论的讨论 2. 忽略：日常问候、表情包、重复内容、纯抱怨。对于每条有价值信息，提供时间范围、参与者、背景、核心信息、讨论结果、分享资源、相关标签。请严格按照提供的JSON schema格式返回结果。"
                        },
                        {
                            "role": "user", 
                            "content": f"""
请分析以下微信群聊记录，提取有价值的信息：

群名：{extracted_data['group_name']}
日期：{extracted_data['date']}
消息总数：{extracted_data['total_messages']}

聊天记录：
{message_text}
"""
                        }
                    ]
                }
                
                print(f"🔍 请求数据: model={data['model']}, response_format={data['response_format']}")
                
                async with session.post(url, headers=headers, json=data) as response:
                    if response.status == 200:
                        response_text = await response.text()
                        print(f"🔍 原始API响应: {response_text[:200]}...")  # 调试输出
                        
                        try:
                            result = json.loads(response_text)
                        except json.JSONDecodeError as e:
                            print(f"❌ JSON解析失败: {extracted_data['group_name']} - {e}")
                            print(f"完整响应: {response_text}")
                            return {
                                'group_name': extracted_data['group_name'],
                                'date': extracted_data['date'],
                                'status': 'failed',
                                'error': f"JSON解析失败: {e}"
                            }
                        
                        # 检查是否有拒绝回答
                        if result.get("choices", [{}])[0].get("message", {}).get("refusal"):
                            print(f"❌ API拒绝回答: {extracted_data['group_name']} - {result['choices'][0]['message']['refusal']}")
                            return {
                                'group_name': extracted_data['group_name'],
                                'date': extracted_data['date'],
                                'status': 'refused',
                                'error': result['choices'][0]['message']['refusal']
                            }
                        
                        # 处理API响应内容
                        content = result["choices"][0]["message"]["content"]
                        print(f"🔍 Content类型: {type(content)}, 内容前100字符: {str(content)[:100]}")
                        
                        if isinstance(content, dict):
                            # 如果content已经是字典（真正的结构化输出）
                            analysis_json = content
                        elif isinstance(content, str):
                            # 如果content是字符串，尝试提取JSON内容
                            try:
                                # 先尝试直接解析
                                analysis_json = json.loads(content)
                            except json.JSONDecodeError:
                                # 如果失败，尝试从markdown代码块中提取JSON
                                import re
                                # 查找```json 和 ``` 之间的内容
                                json_pattern = r'```json\s*\n(.*?)\n```'
                                match = re.search(json_pattern, content, re.DOTALL)
                                if match:
                                    json_str = match.group(1).strip()
                                    try:
                                        analysis_json = json.loads(json_str)
                                        print(f"✅ 成功从markdown代码块中提取JSON: {extracted_data['group_name']}")
                                    except json.JSONDecodeError as e:
                                        print(f"❌ markdown中的JSON格式错误: {e}")
                                        print(f"提取的JSON内容: {json_str[:200]}...")
                                        return {
                                            'group_name': extracted_data['group_name'],
                                            'date': extracted_data['date'],
                                            'status': 'failed',
                                            'error': f'JSON格式错误: {e}'
                                        }
                                else:
                                    # 找不到JSON代码块，尝试查找数组格式
                                    array_pattern = r'(\[.*?\])'
                                    match = re.search(array_pattern, content, re.DOTALL)
                                    if match:
                                        json_str = match.group(1).strip()
                                        try:
                                            # 这里得到的是valuable_information数组，需要构造完整结构
                                            valuable_info = json.loads(json_str)
                                            analysis_json = {
                                                "group_info": {
                                                    "name": extracted_data['group_name'],
                                                    "date": extracted_data['date'],
                                                    "total_messages": extracted_data['total_messages'],
                                                    "valuable_messages": len(valuable_info)
                                                },
                                                "valuable_information": valuable_info,
                                                "summary": {
                                                    "key_topics": [],
                                                    "actionable_insights": len(valuable_info),
                                                    "resource_count": 0
                                                }
                                            }
                                            print(f"✅ 成功从数组格式中提取JSON: {extracted_data['group_name']}")
                                        except json.JSONDecodeError as e:
                                            print(f"❌ 数组JSON格式错误: {e}")
                                            return {
                                                'group_name': extracted_data['group_name'],
                                                'date': extracted_data['date'],
                                                'status': 'failed',
                                                'error': f'JSON格式错误: {e}'
                                            }
                                    else:
                                        print(f"⚠️ 无法从响应中提取JSON，跳过该群: {extracted_data['group_name']}")
                                        print(f"响应内容: {content[:300]}...")
                                        return {
                                            'group_name': extracted_data['group_name'],
                                            'date': extracted_data['date'],
                                            'status': 'failed',
                                            'error': '无法从响应中提取JSON内容'
                                        }
                        else:
                            analysis_json = content
                            
                        print(f"✅ API响应成功: {extracted_data['group_name']} ({extracted_data['date']})")
                        
                        # 单独保存每个群的分析结果
                        self.save_group_result(analysis_json, extracted_data['group_name'], extracted_data['date'])
                        
                        return {
                            'group_name': extracted_data['group_name'],
                            'date': extracted_data['date'],
                            'total_messages': extracted_data['total_messages'],
                            'analysis': analysis_json,
                            'status': 'success'
                        }
                    else:
                        error_text = await response.text()
                        print(f"❌ API请求失败: {extracted_data['group_name']} - {response.status}: {error_text}")
                        return {
                            'group_name': extracted_data['group_name'],
                            'date': extracted_data['date'],
                            'status': 'failed',
                            'error': f"HTTP {response.status}: {error_text}"
                        }
                        
            except Exception as e:
                print(f"❌ 请求异常: {extracted_data['group_name']} - {e}")
                return {
                    'group_name': extracted_data['group_name'],
                    'date': extracted_data['date'],
                    'status': 'failed',
                    'error': str(e)
                }
            
            # 添加延迟避免过快请求
            await asyncio.sleep(0.1)
    
    async def process_all_logs_async(self, raw_dir: str = "./raw"):
        """异步并发处理所有日志文件"""
        print("🔄 开始异步并发处理微信日志...")
        
        # 收集所有JSON文件
        all_files = []
        for root, dirs, files in os.walk(raw_dir):
            for file in files:
                if file.endswith('.json'):
                    file_path = os.path.join(root, file)
                    all_files.append(file_path)
        
        print(f"发现 {len(all_files)} 个JSON文件，开始并发处理...")
        
        # 提取所有文件的数据
        all_extracted_data = []
        for file_path in all_files:
            try:
                extracted_data = self.extract_messages_from_json(file_path)
                all_extracted_data.append(extracted_data)
            except Exception as e:
                print(f"❌ 提取数据失败 {file_path}: {e}")
                all_extracted_data.append({
                    'file_path': file_path,
                    'group_name': 'unknown',
                    'date': 'unknown',
                    'status': 'failed',
                    'error': f"数据提取失败: {e}"
                })
        
        # 并发发送所有请求
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=300),  # 5分钟超时
            connector=aiohttp.TCPConnector(limit=100)   # 连接池限制
        ) as session:
            tasks = [
                self.send_to_reverse_api_async(session, data) 
                for data in all_extracted_data if data.get('messages')
            ]
            
            print(f"🚀 开始并发执行 {len(tasks)} 个API请求（最大并发: {self.max_concurrent}）...")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 处理异常结果
            final_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    print(f"❌ 任务异常: {result}")
                    final_results.append({
                        'status': 'failed',
                        'error': str(result)
                    })
                else:
                    final_results.append(result)
        
        # 保存结果
        self.save_results(final_results)
        return final_results
    
    def save_group_result(self, analysis_json, group_name, date):
        """保存单个群的分析结果"""
        # 创建输出目录
        output_dir = "./analysis"
        os.makedirs(output_dir, exist_ok=True)
        
        # 安全的文件名（替换特殊字符）
        safe_group_name = group_name.replace('/', '_').replace('\\', '_').replace(':', '_')
        output_file = f"{output_dir}/{safe_group_name}_{date}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(analysis_json, f, ensure_ascii=False, indent=2)
        
        print(f"💾 群聊分析结果已保存到: {output_file}")
    
    def save_results(self, results, output_file: str = "analysis_results.json"):
        """保存处理结果摘要"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"💾 所有结果摘要已保存到: {output_file}")


# 原有监控功能保持不变
import requests
from datetime import timedelta

# 可配置监控对象列表
MONITOR_TARGETS = [
    '⛴️ 出海去社区会员群2️⃣',
    '哥飞的朋友们⑧',
    # 在这里添加更多监控对象
]

def get_current_time():
    """
    获取当前时间
    """
    now = datetime.now()
    print(f"当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    return now

def get_yesterday_date():
    """
    获取昨日日期
    
    Returns:
        str: 昨日日期，格式为 YYYY-MM-DD
    """
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

def create_directory_if_not_exists(directory_path):
    """
    如果目录不存在则创建
    
    Args:
        directory_path (str): 目录路径
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"创建目录: {directory_path}")

def save_chatlog_to_file(data, target_name, date_str):
    """
    将聊天记录保存到文件
    
    Args:
        data (dict): 聊天记录数据
        target_name (str): 监控对象名称
        date_str (str): 日期字符串，格式为 YYYY-MM-DD
    """
    # 创建保存路径
    save_dir = f"./raw/{target_name}"
    create_directory_if_not_exists(save_dir)
    
    # 文件路径
    file_path = f"{save_dir}/{date_str}.json"
    
    try:
        # 保存JSON文件
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"聊天记录已保存到: {file_path}")
        return True
        
    except Exception as e:
        print(f"保存文件失败: {e}")
        return False

def get_chatlog_for_date(talker_name, date_str, api_url='http://localhost:5030/api/v1/chatlog'):
    """
    获取指定日期的聊天记录
    
    Args:
        talker_name (str): 聊天对象的名称
        date_str (str): 日期字符串，格式为 YYYY-MM-DD
        api_url (str): API端点URL
    
    Returns:
        dict: 聊天记录数据，JSON格式
    """
    print(f"\n正在获取 {talker_name} 在 {date_str} 的聊天记录...")
    
    # 请求参数
    params = {
        'time': date_str,
        'talker': talker_name,
        'format': 'json'
    }
    
    try:
        # 发送GET请求
        print(f"请求URL: {api_url}")
        print(f"请求参数: {params}")
        response = requests.get(api_url, params=params)
        print(f"实际请求URL: {response.url}")
        print(f"响应状态码: {response.status_code}")
        
        # 检查响应状态
        response.raise_for_status()
        
        # 解析JSON响应
        data = response.json()
        
        print(f"成功获取聊天记录，消息数量: {len(data) if isinstance(data, list) else '未知'}")
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"请求错误: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON解析错误: {e}")
        print(f"原始响应: {response.text}")
        return None

def monitor_all_targets():
    """
    监控所有配置的对象，拉取昨日完整聊天记录
    """
    print("=== 开始监控所有配置对象 ===")
    
    # 获取昨日日期
    yesterday = get_yesterday_date()
    print(f"目标日期: {yesterday}")
    
    success_count = 0
    total_count = len(MONITOR_TARGETS)
    
    for target in MONITOR_TARGETS:
        print(f"\n--- 处理监控对象: {target} ---")
        
        # 获取聊天记录
        chatlog_data = get_chatlog_for_date(target, yesterday)
        
        if chatlog_data is not None:
            # 保存到文件
            if save_chatlog_to_file(chatlog_data, target, yesterday):
                success_count += 1
                print(f"✓ {target} 处理成功")
            else:
                print(f"✗ {target} 保存失败")
        else:
            print(f"✗ {target} 获取数据失败")
    
    print(f"\n=== 监控完成 ===")
    print(f"成功处理: {success_count}/{total_count} 个对象")

def get_past_24h_chatlog(talker_name, api_url='http://localhost:5030/api/v1/chatlog'):
    """
    获取指定对象过去24小时的聊天记录 (保留原有功能)
    等同于: curl -Gs --data-urlencode "time=YYYY-MM-DD~YYYY-MM-DD" --data-urlencode "talker=名称" --data-urlencode "format=json" http://localhost:5030/api/v1/chatlog
    
    Args:
        talker_name (str): 聊天对象的名称
        api_url (str): API端点URL
    
    Returns:
        dict: 聊天记录数据，JSON格式
    """
    
    # 获取当前时间
    current_time = get_current_time()
    
    # 计算24小时前的时间
    past_24h = current_time - timedelta(hours=24)
    
    print(f"查询时间范围: {past_24h.strftime('%Y-%m-%d')} 到 {current_time.strftime('%Y-%m-%d')}")
    print(f"查询对象: {talker_name}")
    
    # 请求参数 - 模仿curl --data-urlencode的格式
    params = {
        'time': past_24h,
        'talker': talker_name,
        'format': 'json'
    }
    
    try:
        # 发送GET请求
        print(f"正在请求: {api_url}")
        print(f"请求参数: {params}")
        response = requests.get(api_url, params=params)
        print(f"实际请求URL: {response.url}")
        print(f"响应状态码: {response.status_code}")
        
        # 检查响应状态
        response.raise_for_status()
        
        # 解析JSON响应
        data = response.json()
        
        print("\n=== 聊天记录 (JSON格式) ===")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"请求错误: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON解析错误: {e}")
        print(f"原始响应: {response.text}")
        return None

def send_chatlog_request():
    """
    发送聊天日志请求 (保留原有功能)
    """
    
    # 请求参数
    params = {
        'time': '2025-07-18',
        'talker': 'abbie0-7-8-9',
        'format': 'json'
    }
    
    # API端点
    url = 'http://localhost:5030/api/v1/chatlog'
    
    try:
        # 发送GET请求
        response = requests.get(url, params=params)
        
        # 检查响应状态
        response.raise_for_status()
        
        # 解析JSON响应（相当于 | jq .）
        data = response.json()
        
        # 格式化输出JSON
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"请求错误: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON解析错误: {e}")
        print(f"原始响应: {response.text}")
        return None

async def async_extract_and_analyze():
    """异步并发提取记录并发送给逆向API"""
    # 配置API密钥（保持不变）
    API_KEY = "sk-Hb9mIfVmwNszMSFE4hYFIuatOTBMrPk36Z1MXMt1uUBgz5mg"
    
    # 创建异步处理器
    processor = AsyncWeChatLogProcessor(API_KEY, max_concurrent=50)
    
    # 异步并发处理所有日志文件
    results = await processor.process_all_logs_async()
    
    # 打印处理摘要
    successful_count = len([r for r in results if r.get('status') == 'success'])
    failed_count = len([r for r in results if r.get('status') == 'failed'])
    
    print(f"\n📊 并发处理完成！")
    print(f"  ✅ 成功: {successful_count} 个文件")
    print(f"  ❌ 失败: {failed_count} 个文件")
    
    if successful_count > 0:
        print(f"\n成功分析的群聊:")
        for result in results:
            if result.get('status') == 'success':
                print(f"  - {result['group_name']} ({result['date']}): {result['total_messages']} 条消息")


def auto_extract_and_analyze():
    """启动异步并发处理"""
    asyncio.run(async_extract_and_analyze())


if __name__ == "__main__":
    import sys
    
    monitor_all_targets()
    auto_extract_and_analyze()