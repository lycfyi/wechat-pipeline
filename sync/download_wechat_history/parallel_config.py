#!/usr/bin/env python3
"""
并行版本配置文件
用于调整并行处理参数，优化性能
"""

# ==================== 性能调优配置 ====================

# 并行工作线程数配置
PERFORMANCE_PROFILES = {
    "conservative": {
        "max_workers": 4,
        "batch_size": 25,
        "delay_between_requests": 1.0,
        "retry_attempts": 3,
        "progress_save_interval": 5,
        "description": "保守配置，适合API限制严格的情况",
    },
    "balanced": {
        "max_workers": 8,
        "batch_size": 50,
        "delay_between_requests": 0.5,
        "retry_attempts": 3,
        "progress_save_interval": 10,
        "description": "平衡配置，推荐用于大多数情况",
    },
    "aggressive": {
        "max_workers": 16,
        "batch_size": 100,
        "delay_between_requests": 0.2,
        "retry_attempts": 2,
        "progress_save_interval": 20,
        "description": "激进配置，适合高性能服务器和宽松API限制",
    },
    "maximum": {
        "max_workers": 32,
        "batch_size": 200,
        "delay_between_requests": 0.1,
        "retry_attempts": 1,
        "progress_save_interval": 50,
        "description": "最大性能配置，需要强大硬件和无API限制",
    },
}


# 根据联系人数量自动选择配置
def get_auto_config(total_contacts):
    """根据联系人数量自动选择最优配置"""
    if total_contacts < 1000:
        return "conservative"
    elif total_contacts < 10000:
        return "balanced"
    elif total_contacts < 30000:
        return "aggressive"
    else:
        return "maximum"


# 内存使用预估（MB）
def estimate_memory_usage(max_workers, avg_response_size_kb=50):
    """估算内存使用量"""
    base_memory = 100  # 基础内存使用 (MB)
    worker_memory = max_workers * avg_response_size_kb / 1024  # 每个工作线程的内存
    return base_memory + worker_memory


# API限制检测配置
API_RATE_LIMITS = {
    "requests_per_second": 10,  # 每秒请求数限制
    "requests_per_minute": 500,  # 每分钟请求数限制
    "concurrent_connections": 20,  # 并发连接数限制
    "timeout_seconds": 30,  # 请求超时时间
}

# 监控和告警配置
MONITORING_CONFIG = {
    "error_rate_threshold": 0.1,  # 错误率阈值 (10%)
    "memory_usage_threshold": 0.8,  # 内存使用阈值 (80%)
    "progress_report_interval": 30,  # 进度报告间隔 (秒)
    "auto_adjust_workers": True,  # 是否自动调整工作线程数
}


def print_config_recommendation(total_contacts):
    """打印配置建议"""
    profile_name = get_auto_config(total_contacts)
    profile = PERFORMANCE_PROFILES[profile_name]

    print(f"\n🎯 配置建议 (基于 {total_contacts:,} 个联系人):")
    print(f"   推荐配置: {profile_name}")
    print(f"   描述: {profile['description']}")
    print(f"   工作线程数: {profile['max_workers']}")
    print(f"   批处理大小: {profile['batch_size']}")
    print(f"   请求延迟: {profile['delay_between_requests']}s")
    print(f"   预估内存使用: {estimate_memory_usage(profile['max_workers']):.1f} MB")

    # 性能预估
    estimated_time = estimate_processing_time(total_contacts, profile['max_workers'])
    print(f"   预估处理时间: {estimated_time}")


def estimate_processing_time(total_contacts, max_workers, months=120, avg_request_time=0.5):
    """估算处理时间"""
    total_tasks = total_contacts * months
    sequential_time = total_tasks * avg_request_time
    parallel_time = sequential_time / max_workers

    # 考虑并行开销
    overhead_factor = 1.2
    actual_time = parallel_time * overhead_factor

    hours = actual_time / 3600
    if hours < 1:
        return f"{actual_time/60:.1f} 分钟"
    elif hours < 24:
        return f"{hours:.1f} 小时"
    else:
        return f"{hours/24:.1f} 天"


if __name__ == "__main__":
    # 显示所有配置选项
    print("🚀 并行处理配置选项:")
    print("=" * 50)

    for name, config in PERFORMANCE_PROFILES.items():
        print(f"\n📊 {name.upper()} 配置:")
        print(f"   工作线程数: {config['max_workers']}")
        print(f"   批处理大小: {config['batch_size']}")
        print(f"   请求延迟: {config['delay_between_requests']}s")
        print(f"   重试次数: {config['retry_attempts']}")
        print(f"   预估内存: {estimate_memory_usage(config['max_workers']):.1f} MB")
        print(f"   说明: {config['description']}")

    # 示例推荐
    print("\n" + "=" * 50)
    for contacts in [500, 5000, 20000, 50000]:
        print_config_recommendation(contacts)
