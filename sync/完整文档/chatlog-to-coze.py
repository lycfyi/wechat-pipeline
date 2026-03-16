#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
微信聊天记录自动同步到扣子知识库脚本
支持按日期、对话者同步聊天记录到扣子平台
"""

import os
import sys
import json
import logging
import argparse
import requests
import base64
from datetime import datetime, timedelta
from dateutil import parser as date_parser
from typing import List, Dict, Optional

# 尝试导入扣子官方SDK
try:
    from cozepy import Coze, TokenAuth, COZE_CN_BASE_URL
    COZE_SDK_AVAILABLE = True
except ImportError:
    COZE_SDK_AVAILABLE = False
    print("警告: 未安装扣子官方SDK，请运行: pip install cozepy")

# 本地chatlog服务的地址和端口
CHATLOG_API_BASE = "http://127.0.0.1:5030"

# 扣子API配置
# 直接在这里填写你的API配置信息
COZE_API_TOKEN = "pat_L5jmYzkzOP3xIXaMYaEAG6v2WuVYkoZtBPc7FX3uDOVeMJUK8Rx4MSuWFGSH4kOx"  # 替换为你的扣子API token
COZE_DATASET_ID = "7534256796384559143"  # 替换为你的数据集ID

# 默认监听的聊天对象
# 可以修改为你常用的群组名称或个人名称
DEFAULT_TALKERS = [
    "【深海圈】海外AI产品 -交流1群",
    "⛴️ 出海去社区会员群2️⃣", 
    "哥飞的朋友们⑧",
    "乔木话痨群",
    "GoSail出海交流群",
    "dontbesilent自媒体 AI 课程",
    "【2】AI产品蝗虫团",
    "Vibe coding交流群③-零基础AI编程"

]

# 扣子API基础配置
COZE_API_BASE = "https://api.coze.cn"
COZE_KNOWLEDGE_API_URL = "https://api.coze.cn/open_api/knowledge/document/create"


class ChatlogSyncer:
    """聊天记录同步器"""
    
    def __init__(self):
        self.setup_logging()
        self.session = requests.Session()
        
        # 初始化扣子SDK客户端（可选）
        if COZE_SDK_AVAILABLE:
            try:
                self.coze_client = Coze(
                    auth=TokenAuth(token=COZE_API_TOKEN),
                    base_url=COZE_CN_BASE_URL
                )
                self.logger.info("✅ 扣子SDK已初始化（使用中国版API）")
            except Exception as e:
                self.coze_client = None
                self.logger.warning(f"⚠️ 扣子SDK初始化失败: {e}")
        else:
            self.coze_client = None
            self.logger.info("💡 使用直接API调用模式（建议安装: pip install cozepy）")
    
    def setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def check_chatlog_service(self) -> bool:
        """检查chatlog服务是否可用"""
        try:
            # 直接测试API端点而不是健康检查端点
            test_params = {
                'time': '2025-08-02',
                'talker': 'test',
                'format': 'json'
            }
            response = requests.get(
                f"{CHATLOG_API_BASE}/api/v1/chatlog",
                params=test_params,
                timeout=10
            )
            if response.status_code == 200:
                self.logger.info("✅ Chatlog服务运行正常")
                return True
            else:
                self.logger.error(f"❌ Chatlog服务健康检查失败: {response.status_code}")
                return False
        except requests.RequestException as e:
            self.logger.error(f"❌ 无法连接到Chatlog服务: {e}")
            return False
    
    def get_chatlog_data(self, talker: str, date: str) -> Optional[List[Dict]]:
        """从chatlog API获取聊天数据"""
        try:
            params = {
                'time': date,
                'talker': talker,
                'format': 'json'
            }
            
            response = requests.get(
                f"{CHATLOG_API_BASE}/api/v1/chatlog",
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if data and isinstance(data, list) and len(data) > 0:
                    self.logger.info(f"📥 获取到 {talker} 在 {date} 的 {len(data)} 条聊天记录")
                    return data
                else:
                    self.logger.info(f"📭 {talker} 在 {date} 没有聊天记录")
                    return None
            else:
                self.logger.error(f"❌ 获取聊天数据失败: {response.status_code} - {response.text}")
                return None
                
        except requests.RequestException as e:
            self.logger.error(f"❌ 请求chatlog API失败: {e}")
            return None
    
    def format_chat_data(self, chat_data: List[Dict], talker: str, date: str) -> str:
        """格式化聊天数据为可读文本"""
        if not chat_data:
            return ""
        
        # 获取聊天室信息
        first_msg = chat_data[0] if chat_data else {}
        is_chat_room = first_msg.get('isChatRoom', False)
        talker_name = first_msg.get('talkerName', talker)
        
        formatted_lines = [
            f"# {talker_name} - {date} 聊天记录",
            f"聊天类型: {'群聊' if is_chat_room else '私聊'}",
            f"同步时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"消息总数: {len(chat_data)}",
            "",
            "---",
            ""
        ]
        
        for msg in chat_data:
            # 解析新的JSON格式字段
            time_str = msg.get('time', '')
            sender_name = msg.get('senderName', '未知')
            content = msg.get('content', '')
            msg_type = msg.get('type', 1)  # 新格式中type是数字
            is_self = msg.get('isSelf', False)
            
            # 格式化时间
            if time_str:
                try:
                    # 解析ISO格式时间: "2025-07-16T21:04:10+08:00"
                    from dateutil import parser as date_parser
                    dt = date_parser.parse(time_str)
                    formatted_time = dt.strftime('%H:%M:%S')
                except:
                    formatted_time = time_str
            else:
                formatted_time = "未知时间"
            
            # 添加发送者标识
            sender_display = f"{sender_name}{'(我)' if is_self else ''}"
            
            # 根据消息类型格式化内容
            if msg_type == 1:  # 文本消息
                formatted_lines.append(f"[{formatted_time}] {sender_display}: {content}")
            elif msg_type == 3:  # 图片消息
                formatted_lines.append(f"[{formatted_time}] {sender_display}: [图片]")
            elif msg_type == 49:  # 文件/链接消息
                formatted_lines.append(f"[{formatted_time}] {sender_display}: [文件/链接] {content}")
            elif msg_type == 34:  # 语音消息
                formatted_lines.append(f"[{formatted_time}] {sender_display}: [语音消息]")
            elif msg_type == 47:  # 表情包
                formatted_lines.append(f"[{formatted_time}] {sender_display}: [表情包]")
            elif msg_type == 10000:  # 系统消息
                formatted_lines.append(f"[{formatted_time}] 系统消息: {content}")
            else:
                formatted_lines.append(f"[{formatted_time}] {sender_display}: [类型{msg_type}] {content}")
            
            formatted_lines.append("")
        
        # 在聊天记录末尾添加特殊分隔符，确保整天的记录不被分割
        formatted_lines.append("===END_OF_CHAT_DAY===")
        return "\n".join(formatted_lines)
    
    def upload_to_coze_dataset(self, content: str, filename: str) -> bool:
        """上传内容到扣子知识库"""
        try:
            # 直接使用知识库文档创建API
            self.logger.info(f"📤 上传文件到扣子知识库: {filename}")
            
            # 将内容转换为Base64编码
            content_bytes = content.encode('utf-8')
            file_base64 = base64.b64encode(content_bytes).decode('utf-8')
            
            # 构建请求数据
            request_data = {
                "dataset_id": COZE_DATASET_ID,
                "document_bases": [
                    {
                        "name": filename,
                        "source_info": {
                            "file_base64": file_base64,
                            "file_type": "txt",
                            "document_source": 0  # 本地文件上传
                        }
                    }
                ],
                "chunk_strategy": {
                    "chunk_type": 1,  # 自定义分段
                    "separator": "===END_OF_CHAT_DAY===",  # 使用特殊分隔符
                    "max_tokens": 2000,  # 使用最大值
                    "remove_extra_spaces": False,
                    "remove_urls_emails": False
                },
                "format_type": 0  # 文档类型
            }
            
            headers = {
                "Authorization": f"Bearer {COZE_API_TOKEN}",
                "Content-Type": "application/json",
                "Agw-Js-Conv": "str"
            }
            
            response = requests.post(
                COZE_KNOWLEDGE_API_URL,
                json=request_data,
                headers=headers,
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    document_infos = result.get('document_infos', [])
                    if document_infos:
                        doc_info = document_infos[0]
                        document_id = doc_info.get('document_id')
                        status = doc_info.get('status')
                        char_count = doc_info.get('char_count')
                        slice_count = doc_info.get('slice_count')
                        
                        self.logger.info(f"✅ 文件上传成功!")
                        self.logger.info(f"   文档ID: {document_id}")
                        self.logger.info(f"   处理状态: {'处理完毕' if status == 1 else '处理中' if status == 0 else '处理失败'}")
                        self.logger.info(f"   字符数: {char_count}")
                        self.logger.info(f"   分段数: {slice_count}")
                        return True
                    else:
                        self.logger.error(f"❌ 上传响应中没有文档信息")
                        return False
                else:
                    error_msg = result.get('msg', '未知错误')
                    self.logger.error(f"❌ 上传失败: {error_msg}")
                    return False
            else:
                self.logger.error(f"❌ API请求失败: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 上传到扣子知识库时出错: {e}")
            return False
    
    def validate_config(self) -> bool:
        """验证配置信息"""
        try:
            self.logger.info("🔍 验证配置信息...")
            
            # 验证API Token
            if not COZE_API_TOKEN or COZE_API_TOKEN.startswith("pat_"):
                if not COZE_API_TOKEN or len(COZE_API_TOKEN) < 20:
                    self.logger.error("❌ 扣子API Token配置错误，请检查COZE_API_TOKEN设置")
                    return False
            
            # 验证数据集ID
            if not COZE_DATASET_ID or len(COZE_DATASET_ID) < 10:
                self.logger.error("❌ 扣子数据集ID配置错误，请检查COZE_DATASET_ID设置")
                return False
            
            self.logger.info(f"✅ 配置验证通过 (数据集ID: {COZE_DATASET_ID[:10]}...)")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 验证配置时出错: {e}")
            return False
    
    def sync_single_talker_date(self, talker: str, date: str) -> bool:
        """同步单个对话者指定日期的聊天记录"""
        self.logger.info(f"🔄 开始同步 {talker} 在 {date} 的聊天记录")
        
        # 获取聊天数据
        chat_data = self.get_chatlog_data(talker, date)
        if not chat_data:
            self.logger.info(f"📭 {talker} 在 {date} 没有聊天记录")
            return True  # 没有数据不算失败
        
        # 格式化数据
        formatted_content = self.format_chat_data(chat_data, talker, date)
        if not formatted_content:
            self.logger.warning(f"⚠️  格式化聊天数据为空: {talker} - {date}")
            return True
        
        # 生成文件名
        safe_talker = talker.replace('/', '_').replace('\\', '_')
        filename = f"chatlog_{safe_talker}_{date}.txt"
        
        # 上传到扣子
        success = self.upload_to_coze_dataset(formatted_content, filename)
        
        if success:
            self.logger.info(f"✅ 同步完成: {talker} - {date}")
        else:
            self.logger.error(f"❌ 同步失败: {talker} - {date}")
        
        return success
    
    def sync_batch(self, talkers: List[str], dates: List[str]) -> Dict[str, int]:
        """批量同步多个对话者和日期"""
        results = {'success': 0, 'failed': 0, 'skipped': 0}
        
        total_tasks = len(talkers) * len(dates)
        current_task = 0
        
        for talker in talkers:
            for date in dates:
                current_task += 1
                self.logger.info(f"📊 进度: {current_task}/{total_tasks}")
                
                try:
                    if self.sync_single_talker_date(talker, date):
                        results['success'] += 1
                    else:
                        results['failed'] += 1
                except Exception as e:
                    self.logger.error(f"❌ 同步出错 {talker} - {date}: {e}")
                    results['failed'] += 1
        
        return results
    
    def list_available_talkers(self) -> List[str]:
        """列出可用的对话者列表"""
        # 这里需要根据chatlog API的具体实现来获取对话者列表
        # 暂时返回默认配置的对话者
        return DEFAULT_TALKERS


def parse_date_range(date_str: str) -> List[str]:
    """解析日期范围字符串"""
    if '..' in date_str:
        # 日期范围 "2025-01-01..2025-01-07"
        start_str, end_str = date_str.split('..', 1)
        start_date = date_parser.parse(start_str).date()
        end_date = date_parser.parse(end_str).date()
        
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        return dates
    else:
        # 单个日期
        return [date_parser.parse(date_str).date().strftime('%Y-%m-%d')]


def main():
    parser = argparse.ArgumentParser(description='微信聊天记录同步到扣子知识库')
    parser.add_argument('--date', 
                       default=datetime.now().strftime('%Y-%m-%d'),
                       help='同步日期 (YYYY-MM-DD) 或日期范围 (YYYY-MM-DD..YYYY-MM-DD)，默认今天')
    parser.add_argument('--talkers', nargs='+', 
                       default=DEFAULT_TALKERS,
                       help='指定同步的对话者列表')
    parser.add_argument('--list-talkers', action='store_true',
                       help='列出可用的对话者')
    parser.add_argument('--check', action='store_true',
                       help='检查服务状态')
    
    args = parser.parse_args()
    
    syncer = ChatlogSyncer()
    
    # 检查服务状态
    if args.check:
        syncer.check_chatlog_service()
        return
    
    # 列出对话者
    if args.list_talkers:
        talkers = syncer.list_available_talkers()
        print("可用的对话者:")
        for i, talker in enumerate(talkers, 1):
            print(f"{i}. {talker}")
        return
    
    # 验证配置信息
    if not syncer.validate_config():
        print("❌ 配置验证失败，请检查配置信息")
        sys.exit(1)
    
    # 检查服务是否可用（忽略错误，直接尝试获取数据）
    # if not syncer.check_chatlog_service():
    #     print("❌ Chatlog服务不可用，请检查服务状态")
    #     sys.exit(1)
    print("🔍 跳过健康检查，直接尝试获取数据...")
    
    # 解析日期
    try:
        dates = parse_date_range(args.date)
    except Exception as e:
        print(f"❌ 日期格式错误: {e}")
        sys.exit(1)
    
    # 执行同步
    print(f"🚀 开始同步任务")
    print(f"📅 日期: {', '.join(dates)}")
    print(f"👥 对话者: {', '.join(args.talkers)}")
    
    results = syncer.sync_batch(args.talkers, dates)
    
    # 显示结果
    print(f"\n📊 同步完成!")
    print(f"✅ 成功: {results['success']}")
    print(f"❌ 失败: {results['failed']}")
    print(f"⏭️  跳过: {results['skipped']}")


if __name__ == '__main__':
    main()