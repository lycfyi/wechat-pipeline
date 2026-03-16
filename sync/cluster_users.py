#!/usr/bin/env python3
"""
WeChat Group Users Clustering
用 TF-IDF + K-Means 对群聊陌生人快速聚类，零 token 消耗。

用法:
  python cluster_users.py                     # 默认 20 类，只看陌生人
  python cluster_users.py --k 15              # 指定 K
  python cluster_users.py --all               # 包含已私聊联系人
  python cluster_users.py --min-msgs 20       # 最低消息门槛
  python cluster_users.py --out report.json   # 输出 JSON
  python cluster_users.py --days 180          # 最近 N 天

输出: 终端表格 + 可选 JSON
"""

import os
import sys
import json
import argparse
import warnings
warnings.filterwarnings("ignore")

import psycopg2
import jieba
jieba.setLogLevel("ERROR")
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans, MiniBatchKMeans
from sklearn.decomposition import TruncatedSVD
from collections import defaultdict
from datetime import datetime

# ── DB ──────────────────────────────────────────────────────────────
DB_URL = os.environ.get(
    "DIRECT_URL",
    "postgresql://root:gmu4K8wEY2efGP5k90il1VX7I3T6JLBh@sjc1.clusters.zeabur.com:30929/postgres"
)

STOP_WORDS = set("""
我 你 他 她 它 我们 你们 他们 的 了 是 在 有 和 也 都 但 就 这 那 个
一个 一 不 没有 可以 什么 这个 那个 说 做 来 去 会 要 能 好 大 小 多
哈哈 哈哈哈 😂 嗯 啊 哦 嗯嗯 对对 好的 谢谢 好棒 厉害 牛 OK ok
接龙 结算 invoice 人民币 稳定币 报名 签到 打卡 今日 分享 大家 群友
所有人 @所有人 链接 点击 扫码 二维码 欢迎 加入 介绍 一下
""".split())

# ── SQL ──────────────────────────────────────────────────────────────
QUERY_STRANGERS = """
WITH private_contacts AS (
    SELECT DISTINCT cr.id FROM chat_rooms cr WHERE cr."isChatRoom" = false
)
SELECT
    u.id,
    u.name,
    COUNT(m.seq) AS msg_count,
    MAX(m.time)::date AS last_active,
    STRING_AGG(m.content, ' ' ORDER BY m.time) AS corpus
FROM messages m
JOIN chat_rooms cr ON m.talker_id = cr.id
JOIN users u ON m.sender_id = u.id
WHERE cr."isChatRoom" = true
  AND m.is_self = false
  AND m.time > NOW() - INTERVAL '{days} days'
  AND u.id NOT IN (SELECT id FROM private_contacts)
  AND LENGTH(m.content) > 5
  AND m.content NOT LIKE '%接龙%'
  AND m.content NOT LIKE '%结算%'
  AND m.content NOT LIKE '%invoice%'
GROUP BY u.id, u.name
HAVING COUNT(m.seq) >= {min_msgs}
"""

QUERY_ALL = """
SELECT
    u.id,
    u.name,
    COUNT(m.seq) AS msg_count,
    MAX(m.time)::date AS last_active,
    STRING_AGG(m.content, ' ' ORDER BY m.time) AS corpus
FROM messages m
JOIN chat_rooms cr ON m.talker_id = cr.id
JOIN users u ON m.sender_id = u.id
WHERE cr."isChatRoom" = true
  AND m.is_self = false
  AND m.time > NOW() - INTERVAL '{days} days'
  AND LENGTH(m.content) > 5
GROUP BY u.id, u.name
HAVING COUNT(m.seq) >= {min_msgs}
"""


def tokenize_zh(text):
    """jieba 分词，过滤停用词和纯英文单词"""
    words = jieba.cut(text, cut_all=False)
    return " ".join(
        w for w in words
        if len(w) >= 2
        and w not in STOP_WORDS
        and not w.isdigit()
        and not all(c.isascii() for c in w)  # 保留中文，过滤纯英文
    )


def fetch_users(days=180, min_msgs=10, include_contacts=False):
    print(f"📡 连接数据库，拉取最近 {days} 天，消息≥{min_msgs} 的用户...")
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    tmpl = QUERY_ALL if include_contacts else QUERY_STRANGERS
    sql = tmpl.format(days=days, min_msgs=min_msgs)
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()

    users = []
    for wxid, name, msg_count, last_active, corpus in rows:
        if not corpus or len(corpus.strip()) < 10:
            continue
        users.append({
            "id": wxid,
            "name": name or wxid,
            "msg_count": msg_count,
            "last_active": str(last_active),
            "corpus": corpus,
        })

    print(f"✅ 拉到 {len(users)} 个用户")
    return users


def cluster(users, k=20):
    print(f"🔢 TF-IDF 向量化...")
    tokenized = [tokenize_zh(u["corpus"]) for u in users]

    vec = TfidfVectorizer(
        max_features=5000,
        min_df=2,
        max_df=0.85,
        ngram_range=(1, 2),
    )
    X = vec.fit_transform(tokenized)
    print(f"   特征矩阵: {X.shape}")

    # SVD 降维（可选，加速大数据集）
    if X.shape[0] > 500:
        print(f"   SVD 降维...")
        svd = TruncatedSVD(n_components=min(100, X.shape[1]-1), random_state=42)
        X = svd.fit_transform(X)

    print(f"⚙️  K-Means 聚类 (k={k})...")
    km = MiniBatchKMeans(n_clusters=k, random_state=42, n_init=5, batch_size=256)
    labels = km.fit_predict(X)

    # 提取每个 cluster 的关键词（用原始 TF-IDF 矩阵）
    vec2 = TfidfVectorizer(max_features=5000, min_df=2, max_df=0.85, ngram_range=(1, 2))
    X2 = vec2.fit_transform(tokenized)
    feat_names = vec2.get_feature_names_out()

    clusters = defaultdict(list)
    for i, user in enumerate(users):
        user["cluster"] = int(labels[i])
        clusters[int(labels[i])].append(i)

    # 每个 cluster 的 top 关键词
    cluster_keywords = {}
    for cid, idxs in clusters.items():
        cluster_vec = X2[idxs].sum(axis=0).A1
        top_idx = cluster_vec.argsort()[::-1][:12]
        kws = [feat_names[i] for i in top_idx if feat_names[i] not in STOP_WORDS][:8]
        cluster_keywords[cid] = kws

    return users, clusters, cluster_keywords


def print_report(users, clusters, cluster_keywords, top_n=8):
    print("\n" + "="*70)
    print(f"📊 聚类结果 — {len(users)} 人 → {len(clusters)} 类")
    print("="*70)

    # 按 cluster 大小排序
    sorted_clusters = sorted(clusters.items(), key=lambda x: -len(x[1]))

    for cid, idxs in sorted_clusters:
        kws = cluster_keywords.get(cid, [])
        kw_str = " · ".join(kws[:6]) if kws else "（关键词不足）"
        members = [users[i] for i in idxs]
        members.sort(key=lambda u: -u["msg_count"])

        print(f"\n【Cluster {cid:02d}】 {len(idxs)}人  🏷  {kw_str}")
        print("  " + "-"*60)
        for u in members[:top_n]:
            print(f"  {u['name'][:18]:20s}  消息:{u['msg_count']:4d}  最近:{u['last_active']}")
        if len(members) > top_n:
            print(f"  ... 还有 {len(members)-top_n} 人")


def save_report(users, clusters, cluster_keywords, path):
    out = []
    for cid, idxs in sorted(clusters.items(), key=lambda x: -len(x[1])):
        members = [users[i] for i in idxs]
        members.sort(key=lambda u: -u["msg_count"])
        out.append({
            "cluster_id": cid,
            "size": len(idxs),
            "keywords": cluster_keywords.get(cid, []),
            "members": [
                {"name": u["name"], "id": u["id"],
                 "msg_count": u["msg_count"], "last_active": u["last_active"]}
                for u in members
            ],
        })
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n💾 JSON 报告已保存: {path}")


def main():
    parser = argparse.ArgumentParser(description="WeChat群聊用户聚类")
    parser.add_argument("--k", type=int, default=20, help="聚类数量 (默认20)")
    parser.add_argument("--days", type=int, default=180, help="最近N天数据 (默认180)")
    parser.add_argument("--min-msgs", type=int, default=10, help="最低消息条数门槛 (默认10)")
    parser.add_argument("--all", action="store_true", help="包含已私聊联系人")
    parser.add_argument("--out", type=str, default="", help="输出JSON路径")
    parser.add_argument("--top", type=int, default=8, help="每类显示前N人")
    args = parser.parse_args()

    start = datetime.now()
    users = fetch_users(days=args.days, min_msgs=args.min_msgs, include_contacts=args.__dict__["all"])

    if len(users) < args.k:
        print(f"⚠️  用户数({len(users)})小于 k({args.k})，自动调整 k={max(5, len(users)//3)}")
        args.k = max(5, len(users)//3)

    users, clusters, cluster_keywords = cluster(users, k=args.k)
    print_report(users, clusters, cluster_keywords, top_n=args.top)

    if args.out:
        save_report(users, clusters, cluster_keywords, args.out)

    elapsed = (datetime.now() - start).seconds
    print(f"\n⏱  总耗时: {elapsed}s")


if __name__ == "__main__":
    main()
