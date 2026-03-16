
# 概览
## 1. chatlog部署部分
下载chatlog
关掉电脑的SIP
先手动打开一下chatlog，获取到微信聊天记录地址
用chatlog解密一下
启动chatlog的http服务
配置后台常驻服务

## 2. 自动同步coze部分
创建一个智能体，给他配一个知识库。
拿到知识库的id + coze的token
放好py脚本
配置后台常驻服务

# 微信聊天记录自动化同步系统部署指南

**目标：一次配置，后台常驻，灵活同步微信聊天记录到扣子知识库，支持智能问答。**

---

## **0. 前置条件**

|**条件**|**说明**|
|---|---|
|macOS 12+|Apple Silicon 或 Intel 均可。|
|WeChat mac **4.0.3.80**|[下载地址](https://github.com/zsbai/wechat-versions/releases?page=2)|
|临时关闭 SIP|需要 sudo、临时关闭 SIP。|
|Python 3.8+|系统自带或通过 Homebrew 安装|

---

## **1. 安装 chatlog 并加入 $PATH**

```bash
# 克隆或下载 release 压缩包
cd ~/Dev_project
git clone https://github.com/sjzar/chatlog.git
cd chatlog

# 移动到全局可用位置
sudo cp chatlog /opt/homebrew/bin/

# 给可执行文件执行权限
sudo chmod +x /opt/homebrew/bin/chatlog
```

验证安装：
```bash
chatlog --help
```

---

## **2. 临时关闭 SIP 获取 AES 密钥**

macOS 默认不允许第三方读取微信数据库的解密密钥，必须暂时关闭 SIP。

### **2.1 进入恢复模式**
- **Intel Mac**：开机时长按 ⌘+R
- **Apple Silicon**：关机后长按电源键 → **Options** → 继续

### **2.2 关闭 SIP**
打开顶部菜单 **Utilities ▸ Terminal**，执行：

```bash
csrutil disable
```

然后重启进入正常系统。

### **2.3 重新开启 SIP（操作完成后）**
```bash
csrutil enable
reboot
```

---

## **3. 获取微信数据目录和密钥**

### **3.1 查看数据目录**
```bash
chatlog
```

记录输出的数据目录路径，例如：
```
/Users/panpan/Library/Containers/com.tencent.xinWeChat/Data/Documents/xwechat_files/wxid_tjyzfvnvdu0w21_3a08
```

### **3.2 获取解密密钥**
```bash
# 获取 AES 密钥（需要 sudo）
sudo chatlog key
```

记录输出的 32 字节密钥，例如：
```
90a66226f62f47528ef9b03ae9e0b0e9d01ee4b714da4e13a0dfe66b07b5a491
```

---

## **4. 解密微信数据库**

```bash
chatlog decrypt \
  --data-dir "/Users/panpan/Library/Containers/com.tencent.xinWeChat/Data/Documents/xwechat_files/wxid_tjyzfvnvdu0w21_3a08" \
  --work-dir "/Users/panpan/Documents/chatlog/wxid_tjyzfvnvdu0w21_3a08" \
  --key "90a66226f62f47528ef9b03ae9e0b0e9d01ee4b714da4e13a0dfe66b07b5a491" \
  --platform darwin \
  --version 4
```

> **注意**：以后如果微信数据有大规模变动（例如清空、迁移），可以再跑一次 decrypt。

---

## **5. 启动 chatlog HTTP 服务**

### **5.1 命令行测试**
```bash
chatlog server \
  -a 127.0.0.1:5030 \
  -d "/Users/panpan/Library/Containers/com.tencent.xinWeChat/Data/Documents/xwechat_files/wxid_tjyzfvnvdu0w21_3a08" \
  -w "/Users/panpan/Documents/chatlog/wxid_tjyzfvnvdu0w21_3a08" \
  -p darwin \
  -v 4
```

### **5.2 验证服务**
```bash
# 检查服务健康状态
curl http://localhost:5030/healthz

# 测试 API 调用
curl -Gs \
  --data-urlencode "time=2025-08-01" \
  --data-urlencode "talker=Chinmo" \
  --data-urlencode "format=json" \
  http://localhost:5030/api/v1/chatlog | jq .
```

---

## **6. 配置后台常驻服务**

### **6.1 创建服务启动脚本**

创建 `~/scripts/chatlog-run.sh`：
```bash
#!/bin/bash
exec /opt/homebrew/bin/chatlog server \
  -a 127.0.0.1:5030 \
  -d "/Users/panpan/Library/Containers/com.tencent.xinWeChat/Data/Documents/xwechat_files/wxid_tjyzfvnvdu0w21_3a08" \
  -w "/Users/panpan/Documents/chatlog/wxid_tjyzfvnvdu0w21_3a08" \
  -p darwin \
  -v 4
```

```bash
chmod +x ~/scripts/chatlog-run.sh
```

### **6.2 创建 LaunchAgent 配置**

创建 `~/Library/LaunchAgents/com.neo.chatlog.plist`：
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>               <string>com.neo.chatlog</string>
    <key>ProgramArguments</key>    <array>
        <string>/Users/panpan/scripts/chatlog-run.sh</string>
    </array>
    <key>RunAtLoad</key>           <true/>
    <key>KeepAlive</key>           <true/>
    <key>StandardOutPath</key>     <string>/tmp/chatlog.out</string>
    <key>StandardErrorPath</key>   <string>/tmp/chatlog.err</string>
</dict>
</plist>
```

### **6.3 载入并验证服务**
```bash
# 载入服务
launchctl unload ~/Library/LaunchAgents/com.neo.chatlog.plist 2>/dev/null
launchctl load -w ~/Library/LaunchAgents/com.neo.chatlog.plist

# 验证服务状态
launchctl list | grep com.neo.chatlog          # 应看到 PID
lsof -i :5030                                  # 看到 chatlog 正在监听
tail -f /tmp/chatlog.out                       # 查看服务日志
```

### **6.4 配置自动解密服务**

创建 `~/scripts/chatlog-decrypt-hourly.sh`：
```bash
#!/bin/bash
/opt/homebrew/bin/chatlog decrypt \
  -d "/Users/panpan/Library/Containers/com.tencent.xinWeChat/Data/Documents/xwechat_files/wxid_tjyzfvnvdu0w21_3a08" \
  -w "/Users/panpan/Documents/chatlog/wxid_tjyzfvnvdu0w21_3a08" \
  -k "90a66226f62f47528ef9b03ae9e0b0e9d01ee4b714da4e13a0dfe66b07b5a491" \
  -p darwin -v 4
```

```bash
chmod +x ~/scripts/chatlog-decrypt-hourly.sh
```

创建 `~/Library/LaunchAgents/com.neo.chatlog.decrypt.plist`：
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>           <string>com.neo.chatlog.decrypt</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Users/panpan/scripts/chatlog-decrypt-hourly.sh</string>
    </array>
    <key>StartInterval</key>   <integer>3600</integer>
    <key>StandardOutPath</key> <string>/tmp/chatlog-decrypt.out</string>
    <key>StandardErrorPath</key><string>/tmp/chatlog-decrypt.err</string>
</dict>
</plist>
```

载入解密服务：
```bash
launchctl unload ~/Library/LaunchAgents/com.neo.chatlog.decrypt.plist 2>/dev/null   
launchctl load -w ~/Library/LaunchAgents/com.neo.chatlog.decrypt.plist
launchctl list | grep com.neo.chatlog.decrypt
```

---

## **7. 扣子平台配置**

### **7.1 注册扣子账号**
1. 访问 [https://www.coze.cn](https://www.coze.cn)
2. 注册并登录账号
3. 进入扣子开发平台

### **7.2 创建智能体（Bot）**
1. 在扣子首页点击「创建Bot」
2. 填写Bot信息：
   - **名称**：聊天记录助手
   - **描述**：专门回答微信聊天记录相关问题的智能助手
   - **头像**：选择合适的头像
3. 点击「创建」

### **7.3 创建知识库**
1. 在Bot编辑页面，点击左侧「知识库」
2. 点击「创建知识库」
3. 选择知识库类型：
   - **名称**：微信聊天记录库
   - **描述**：存储每日微信聊天记录的知识库
   - **数据源类型**：本地文件上传
4. 点击「创建」并记录知识库ID

### **7.4 获取API凭证**

#### **创建Personal Access Token**
1. 访问 [https://www.coze.cn/open/oauth/pats](https://www.coze.cn/open/oauth/pats)
2. 点击「创建令牌」
3. 填写令牌信息：
   - **名称**：chatlog-sync-token
   - **过期时间**：选择合适的过期时间
   - **权限范围**：选择以下权限
     - Chat
     - Bot Management
     - Dataset Read/Write
4. 点击「创建」并**安全保存**生成的token


确定好2个重要配置
```bash
# 扣子API配置
COZE_API_TOKEN="your_personal_access_token_here"
COZE_DATASET_ID="your_dataset_id_here"
```

---

## **8. 安装同步脚本依赖**

```bash
# 安装Python依赖
pip3 install cozepy requests python-dateutil
```

---

## **9. 创建同步脚本**

把python 脚本放到 `~/scripts/chatlog-to-coze.py`，支持多种同步方式。

---

## **10. 配置定时任务**

### **10.1 创建基础定时同步**

创建 `~/Library/LaunchAgents/com.neo.chatlog.sync.plist`：
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    名字可以自己取
    <string>com.neo.chatlog.sync</string>
    
    <key>ProgramArguments</key>
    <array>
    这里要换成你自己的地址
        <string>/Users/panpan/scripts/chatlog-env/bin/python3</string>
        <string>/Users/panpan/scripts/chatlog-to-coze.py</string>
    </array>
    
    <!-- 每天晚上8点执行 -->
	这儿可以随便改，例如改成1h一次，让ai改
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>20</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    
    <key>StandardOutPath</key>
    <string>/tmp/chatlog-sync.out</string>
    <key>StandardErrorPath</key>
    <string>/tmp/chatlog-sync.err</string>
    
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin</string>
    </dict>
</dict>
</plist>
```

### **10.2 载入定时任务**
```bash
# 先关
launchctl unload ~/Library/LaunchAgents/com.neo.chatlog.sync.plist

# 载入定时任务
launchctl load -w ~/Library/LaunchAgents/com.neo.chatlog.sync.plist

# 查看任务状态
launchctl list | grep com.neo.chatlog.sync

# 手动测试运行
launchctl start com.neo.chatlog.sync

# 查看日志
tail -f /tmp/chatlog-sync.out
```

---

## **11. 验证和使用**

### **11.1 验证同步**
```bash
# 查看定时任务状态
launchctl list | grep chatlog

# 查看同步日志
tail -50 /tmp/chatlog-sync.out

# 手动测试同步
python3 ~/scripts/chatlog-to-coze.py --list-talkers
```

### **11.2 管理命令**
```bash
# 停止定时任务
launchctl unload ~/Library/LaunchAgents/com.neo.chatlog.sync.plist

# 重新载入定时任务
launchctl load -w ~/Library/LaunchAgents/com.neo.chatlog.sync.plist

# 查看任务详情
launchctl print user/$(id -u)/com.neo.chatlog.sync
```

