## 📚 项目概述

这个项目用于自动化同步微信群聊天记录到数据库（PostgreSQL），支持：

- 🔄 **自动下载**：从微信 API 批量下载聊天记录到本地 JSON 文件
- 💾 **智能存储**：上传到 PostgreSQL 数据库（Supabase/Zeabur），支持增量更新
- 🔍 **数据查询**：通过 Prisma ORM 查询和分析聊天记录
- 🚚 **数据迁移**：支持数据库间的高效迁移（见 `migration_tools/`）

### 主要功能模块

- **`download_wechat_history/`** - 从微信 API 下载聊天记录
- **`upload_to_prisma_db/`** - 将本地数据上传到数据库
- **`migration_tools/`** - 数据库迁移工具（Supabase → Zeabur）
- **`prisma/`** - 数据库 schema 定义

---

## 周期性维护任务 ⏰

**建议频率：每周或每两周执行一次**

定期同步微信聊天记录到数据库，确保数据完整性：

### 步骤：

1. **下载最新的微信聊天记录到本地（智能合并模式）**

   ```bash
   # 从当前月份开始合并更新（推荐）
   python download_wechat_history/run_parallel.py --force-start-month 2025-09

   # 从更早的月份开始合并（例如从8月开始）
   python download_wechat_history/run_parallel.py --force-start-month 2025-08
   ```

2. **上传到 Prisma/Supabase 数据库（智能过滤）**

   ```bash
   # 只上传最近合并更新的文件（推荐）
   python upload_to_prisma_db/batch_import_all.py --force-update-since 2025-08

   # 或者上传指定日期之后的所有文件
   python upload_to_prisma_db/batch_import_all.py --force-update-since 2025-08-15

   # 上传所有文件（不推荐，会重新检查所有历史文件）
   python upload_to_prisma_db/batch_import_all.py
   ```

### 注意事项：

- `--force-start-month` 使用**智能合并模式**：新数据与现有 JSON 文件进行 union 合并，自动去重
- `--force-update-since` 使用**精确月份过滤**：基于文件名只上传指定月份及之后的文件，完全避免扫描旧数据
- 现有文件不会被删除，只会补充缺失的消息数据
- 合并基于消息的 `seq` 字段进行去重，确保数据完整性
- 上传过滤基于文件名格式 `chatlog_YYYY-MM.json`，无需读取文件内容，效率极高
- 建议在网络稳定时执行，避免中断
- 可以查看 `chat_download_progress.json` 和 `chat_db_upload_progress.json` 了解同步进度
- 月份格式必须是 `YYYY-MM`（例如：2025-01, 2025-09）

---

## 如何运行：

### 🚀 完整工作流程（推荐）

```bash
# 1. 合并更新8月份及之后的数据
python download_wechat_history/run_parallel.py --force-start-month 2025-08

# 2. 只上传更新过的文件到数据库
python upload_to_prisma_db/batch_import_all.py --force-update-since 2025-08
```

### 📋 分步骤运行

1. **下载到本地文件：**

   ```bash
   python download_wechat_history/run_parallel.py --force-start-month 2025-09
   ```

2. **上传到 Supabase（智能过滤）：**
   ```bash
   python upload_to_prisma_db/batch_import_all.py --force-update-since 2025-09
   ```

---

## 🚚 数据库迁移

如需在数据库之间迁移数据（如从 Supabase 迁移到 Zeabur），请使用 `migration_tools/` 中的工具：

```bash
cd migration_tools

# 查看详细说明
cat README.md

# 测试数据库连接
python test_zeabur_connection.py

# 执行迁移（推荐使用 Python 脚本，有进度监控）
python migrate_db_advanced.py

# 或使用 Shell 脚本（更简单）
./migrate_db_dump.sh
```

**迁移工具特点：**

- ✅ 使用 PostgreSQL 原生 `pg_dump`/`pg_restore`，速度快（500k 行 ~2-5 分钟）
- ✅ 自动压缩和进度监控
- ✅ 自动验证迁移数据完整性
- ✅ 支持断点续传和错误恢复

详细说明请查看 `migration_tools/README.md`

---

## API 案例

curl -Gs \
--data-urlencode "time=2025-08-10" \
--data-urlencode "talker=⛴️ 出海去社区会员群 2️⃣" \
--data-urlencode "format=json" \
http://localhost:5030/api/v1/chatlog | jq .
