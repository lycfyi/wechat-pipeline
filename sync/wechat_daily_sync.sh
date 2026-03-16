#!/bin/bash
# =============================================================================
# wechat_daily_sync.sh - 微信聊天记录每日增量同步
# 功能: 下载最近2个月的聊天记录，上传到 Prisma DB，Telegram 通知
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${SCRIPT_DIR}/.venv/bin/python3"
LOG_FILE="${SCRIPT_DIR}/logs/wechat_sync_$(date +%Y%m%d_%H%M%S).log"
CHATLOG_URL="http://localhost:5030"
TELEGRAM_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
TELEGRAM_CHAT_ID="${TELEGRAM_NOTIFY_CHAT_ID:-513735155}"

# Colors for console output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

mkdir -p "${SCRIPT_DIR}/logs"

log() {
    echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

send_telegram() {
    local message="$1"
    if [ -n "$TELEGRAM_TOKEN" ]; then
        curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage" \
            -d "chat_id=${TELEGRAM_CHAT_ID}" \
            -d "text=${message}" \
            -d "parse_mode=HTML" > /dev/null 2>&1 || true
    fi
}

# ── 检查 chatlog server 是否在运行 ──
check_chatlog_server() {
    if curl -s --max-time 5 "${CHATLOG_URL}/api/v1/chatroom" > /dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# ── 计算本月和上月的 YYYY-MM ──
get_sync_months() {
    CURRENT_MONTH=$(date +%Y-%m)
    LAST_MONTH=$(date -v-1m +%Y-%m 2>/dev/null || date -d "1 month ago" +%Y-%m)
    echo "${LAST_MONTH} ${CURRENT_MONTH}"
}

# ── 步骤1: 拉取最新群聊/联系人列表 ──
step_fetch_api_data() {
    log "📡 Step 1: 获取最新群聊和联系人列表..."
    cd "$SCRIPT_DIR"
    $PYTHON fetch_api_data.py "$CHATLOG_URL" \
        --output-dir api_data \
        --format both >> "$LOG_FILE" 2>&1
    log "✅ API 数据获取完成"
}

# ── 步骤2: 下载最近2个月的聊天记录 ──
step_download_chatlogs() {
    local force_month="$1"
    log "📥 Step 2: 下载聊天记录 (从 ${force_month} 开始合并)..."
    cd "$SCRIPT_DIR"

    # 使用 yes 自动确认交互提示
    echo "yes" | $PYTHON download_wechat_history/run_parallel.py \
        --force-start-month "$force_month" \
        --profile balanced >> "$LOG_FILE" 2>&1 || {
        log "⚠️  下载步骤有部分失败，继续上传..."
    }
    log "✅ 聊天记录下载完成"
}

# ── 步骤3: 上传到 Prisma DB ──
step_upload_to_db() {
    local since_month="$1"
    log "⬆️  Step 3: 上传到数据库 (从 ${since_month} 开始)..."
    cd "$SCRIPT_DIR"

    UPLOAD_OUTPUT=$($PYTHON upload_to_prisma_db/batch_import_all.py chat_history \
        --force-update-since "$since_month" \
        --no-progress 2>&1 | tee -a "$LOG_FILE")

    # 提取统计数据
    MESSAGES_IMPORTED=$(echo "$UPLOAD_OUTPUT" | grep -oE 'Messages: [0-9,]+' | tail -1 | grep -oE '[0-9,]+' || echo "?")
    FILES_PROCESSED=$(echo "$UPLOAD_OUTPUT" | grep -oE 'Files processed: [0-9]+' | grep -oE '[0-9]+' || echo "?")
    log "✅ 上传完成: 处理 ${FILES_PROCESSED} 个文件"
}

# ── 步骤4: 查询 DB 验证最新条目 ──
step_verify_db() {
    log "🔍 Step 4: 验证数据库..."

    DB_STATS=$(PGPASSWORD=gmu4K8wEY2efGP5k90il1VX7I3T6JLBh psql \
        "postgresql://root@sjc1.clusters.zeabur.com:30929/postgres" \
        -t -c "
        SELECT
            COUNT(*) as total,
            MAX(time) as latest
        FROM messages
        WHERE time >= NOW() - INTERVAL '60 days';
        " 2>/dev/null || echo "DB query failed")

    log "📊 DB 验证结果: ${DB_STATS}"
}

# ── 主流程 ──
main() {
    START_TIME=$(date +%s)
    log "🚀 微信聊天记录每日同步开始"
    log "📅 日期: $(date '+%Y-%m-%d %H:%M:%S %Z')"

    # 检查 chatlog server
    if ! check_chatlog_server; then
        log "❌ chatlog server 未运行 (${CHATLOG_URL})"
        send_telegram "❌ 微信同步失败：chatlog server 未运行，请检查"
        exit 1
    fi
    log "✅ chatlog server 已连接"

    # 计算月份
    read -r LAST_MONTH CURRENT_MONTH <<< "$(get_sync_months)"
    log "📅 同步月份: ${LAST_MONTH} ~ ${CURRENT_MONTH}"

    # 执行步骤
    step_fetch_api_data
    step_download_chatlogs "$LAST_MONTH"
    step_upload_to_db "$LAST_MONTH"
    step_verify_db

    # 计算耗时
    END_TIME=$(date +%s)
    DURATION=$(( END_TIME - START_TIME ))
    DURATION_MIN=$(( DURATION / 60 ))

    log "🎉 同步完成！耗时 ${DURATION_MIN} 分钟"

    # 发送成功通知（通过 OpenClaw message tool 更可靠）
    # Telegram 通知由 cron job 的 delivery 处理
    echo "SYNC_SUCCESS:${LAST_MONTH}:${CURRENT_MONTH}:${DURATION_MIN}min"
}

main "$@"
