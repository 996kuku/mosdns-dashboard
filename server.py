#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MosDNS 仪表盘服务端 —— 部署在内网一台 Linux 主机上
------------------------------------------------------
通过 SSH 直连路由器上的 MosDNS stats_collector API (127.0.0.1:9091) 取数，
对外提供 HTTP：
    GET /               页面
    GET /api/snapshot   MosDNS 统计快照（带 10 秒缓存）
    GET /api/query?name=域名   让路由器的 MosDNS 真机解析一次，返回应答 + 命中规则/上游
    GET /health         健康检查

仅用 Python 标准库，无第三方依赖。
"""
import base64
import gzip
import hashlib
import hmac
import json
import os
import queue
import re
import secrets
import socket
import ssl
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, quote, unquote

# ---------- 配置 ----------
LISTEN_HOST = os.environ.get("DASH_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("DASH_PORT", "8088"))
TLS_CERT = os.environ.get("DASH_TLS_CERT", "/opt/mosdns-dashboard/ssl/server.pem")
TLS_KEY  = os.environ.get("DASH_TLS_KEY",  "/opt/mosdns-dashboard/ssl/server.key")
MOSDNS_HOST = os.environ.get("MOSDNS_HOST", "192.168.1.1")
MOSDNS_USER = os.environ.get("MOSDNS_USER", "root")
SSH_KEY = os.environ.get("MOSDNS_SSH_KEY", "/root/.ssh/id_ed25519_mosdns")
STATS_API = "http://127.0.0.1:9091/plugins/stats_collector/api/v1"
MOSDNS_PORT = 5335
CACHE_TTL = 10           # 快照缓存秒数（普通 /api/snapshot 用；SSE 不走这个缓存）
# ---- 实时推送（SSE）----
# PUSH_INTERVAL 是"有人在看面板"时的采集间隔；没人看时降到 PUSH_IDLE_INTERVAL，
# 免得白白拿 SSH 去烦路由器（路由器是主网关，能少一次是一次）。
PUSH_INTERVAL = float(os.environ.get("PUSH_INTERVAL", "2"))
PUSH_IDLE_INTERVAL = float(os.environ.get("PUSH_IDLE_INTERVAL", "10"))
SSE_HEARTBEAT = 15       # 心跳间隔（秒）：保活 + 让中间代理别把长连接掐了
LOG_KEEP = 20000         # 本地累积保留的原始日志条数（约 5.5 小时；MosDNS 单次最多只给 500 条）
LOG_PAGE = 500           # 单次向 MosDNS 拉取的条数上限
LOG_DEFAULT = 300        # 首屏 /api/snapshot 默认返回的日志条数
# ---- 分层时序存储 ----
SAMPLE_MIN = 60          # 分钟级采样间隔（秒）
SAMPLE_HOUR = 3600       # 小时级采样间隔（秒）
KEEP_MIN_DAYS = 7        # 分钟聚合保留天数
KEEP_HOUR_DAYS = 400     # 小时聚合保留天数
PRUNE_INTERVAL = 86400   # 清理任务间隔（秒）
ANS_LIMIT = 30           # 每条日志保留的应答数（2026-10-03 由 3 调到 30：
                         # 原值导致弹窗里的「应答结果」被截断，实测有 52% 的记录应答 >3 条、最多 26 条）
# 后台累积拉取间隔（秒）。2026-10-03 由 15 调到 5：
# 实测单次 SSH(logs) 148ms + AGH querylog 202ms，5 秒间隔下路由器侧每分钟多耗约 2.8 秒 CPU
# （占单核 13.9%，而路由器是 8 核、常态 load 0.07，余量充足）。
# 想改回省电模式就调回 15（可用环境变量 INGEST_INTERVAL 覆盖，不必改文件）。
INGEST_INTERVAL = int(os.environ.get("INGEST_INTERVAL", "5"))
TOP_SHOW = 50            # 页面榜单展示条数（热门 / 拦截都是 50）
TOP_FETCH = 150          # 向 MosDNS 拉取的条数；多拉一些，基线差分过滤掉后仍够 50 条
AGH_PAGE = 500           # 单次从 AGH querylog 拉取的条数（它没有 500 的硬上限，但没必要太多）

HERE = os.path.dirname(os.path.abspath(__file__))
INDEX = os.path.join(HERE, "index.html")
STATIC_DIR = os.path.join(HERE, "static")
SEP = "@@@MOSDNS-SPLIT@@@"
AGH_CONF = os.path.join(HERE, "agh.conf")
# 设备名映射（可选）：一行一条 "192.168.1.200=我的台式机"，放在 devices.conf 里
DEV_CONF = os.path.join(HERE, "devices.conf")
# 登录认证：auth.conf（AUTH_ENABLE=1 时生效；账号/密码哈希/会话密钥都在里面，权限 600）
AUTH_CONF = os.path.join(HERE, "auth.conf")
LOGIN_PAGE = os.path.join(HERE, "login.html")
SESS_COOKIE = "mdash_sess"
SESS_DAYS = 7                    # 登录状态保持天数
PBKDF2_ROUNDS = 200000
FAIL_MAX = 6                     # 同一来源连续失败多少次后临时锁定
FAIL_LOCK = 300                  # 锁定时长（秒）
# 页面里自定义鼠标图标用的静态资源后缀（只放行图片，不做通用文件服务）
STATIC_EXT = {".png": "image/png", ".svg": "image/svg+xml",
              ".ico": "image/x-icon", ".jpg": "image/jpeg",
              ".jpeg": "image/jpeg", ".webp": "image/webp"}
START_TS = time.time()

_lock = threading.Lock()
_cache = {"ts": 0.0, "data": None}

# ---- 实时推送（SSE）的全局状态 ----
# 注意：这些必须定义在 push_loop / Handler 之前，否则后台线程起来时会 NameError。
# 每个元素是一个 queue.Queue，对应一个打开着的页面。
_sse_clients = set()
_sse_lock = threading.Lock()
_last_sig = {"v": None}


# ---------- SSH ----------
def ssh(cmd, timeout=25):
    full = ["ssh", "-i", SSH_KEY,
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=8",
            "-o", "LogLevel=ERROR",
            "%s@%s" % (MOSDNS_USER, MOSDNS_HOST), cmd]
    try:
        p = subprocess.run(full, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return None, "ssh timeout"
    except Exception as e:
        return None, str(e)
    if p.returncode != 0:
        return None, p.stderr.decode("utf-8", "replace").strip()[:300]
    return p.stdout.decode("utf-8", "replace"), None


def _jload(txt):
    txt = (txt or "").strip()
    if not txt:
        return None
    try:
        return json.loads(txt)
    except Exception:
        return None


# ---------- 上游服务器 ----------
# 上游清单的**权威来源是路由器上的 MosDNS 配置**（/var/etc/mosdns.json 里 forward 插件的
# upstreams）。只看日志是不行的 —— 日志只会出现「实际被用过」的上游，
# 配置里配了但一次没用过的（比如备用上游）就漏掉了。
# 配置缓存秒数：避免每次快照都去 SSH 路由器（主网关，负担要省着用）。
# 2026-10-03 用户要求「清单也实时」：600 → 60，改完路由器的 MosDNS 配置最多 1 分钟
# 就能在面板反映出来，不用再重启面板。代价是每 60 秒多一次到路由器的 SSH
# （只读一个 json + getaddrinfo，实测开销可忽略）。仍可用环境变量覆盖。
UP_CFG_TTL = int(os.environ.get("UP_CFG_TTL", "60"))
_up_cfg = {"ts": 0.0, "data": None, "err": ""}
_up_lock = threading.Lock()

# 在路由器上跑的**只读**脚本：读配置 + 把域名形式的上游解析成 IP。
# 用 base64 传输是为了绕开 ssh -> sh -> python 三层的引号转义地狱。
# ⚠️ 这个脚本只 open(..., "r") 读配置，不做任何写操作 / 不改路由器的任何状态。
_UP_CFG_PY = """
import json, socket
def is_ip(s):
    try:
        socket.inet_aton(s); return True
    except Exception:
        return ":" in s
try:
    j = json.load(open("/var/etc/mosdns.json"))
except Exception as e:
    print(json.dumps({"error": str(e)})); raise SystemExit
fwd = []
for p in j.get("plugins", []):
    if p.get("type") != "forward":
        continue
    tag = p.get("tag") or ""
    args = p.get("args") or {}
    for u in (args.get("upstreams") or []):
        if isinstance(u, dict):
            fwd.append({"tag": tag, "addr": u.get("addr") or "",
                        "bootstrap": u.get("bootstrap") or "",
                        "http3": bool(u.get("enable_http3"))})
ips = {}
for u in fwd:
    h = u["addr"]
    for pre in ("https://", "http://", "tls://", "quic://",
                "udp://", "tcp://", "h3://"):
        if h.startswith(pre):
            h = h[len(pre):]
            break
    h = h.split("/")[0]
    if h.count(":") > 1:
        continue
    if h.count(":") == 1:
        h = h.split(":")[0]
    if not h or h in ips or is_ip(h):
        continue
    try:
        r = socket.getaddrinfo(h, None, socket.AF_INET)
        ips[h] = sorted(set(x[4][0] for x in r))
    except Exception:
        ips[h] = []
print(json.dumps({"forward": fwd, "dns": ips}, ensure_ascii=False))
"""


def read_upstream_cfg(force=False):
    """读路由器的 MosDNS 上游配置（带缓存）。返回 (data, err)。"""
    now = time.time()
    with _up_lock:
        if (not force and _up_cfg["data"] is not None
                and (now - _up_cfg["ts"]) < UP_CFG_TTL):
            return _up_cfg["data"], _up_cfg["err"]
    b64 = base64.b64encode(_UP_CFG_PY.encode("utf-8")).decode("ascii")
    out, err = ssh('echo "%s" | base64 -d | python3' % b64, 25)
    data = _jload(out)
    if not (data and isinstance(data.get("forward"), list)):
        # 取不到就退回上一次的缓存（有总比没有好），把错误如实带出去给前端提示。
        # ⚠️ **必须同时刷新 ts**：否则 SSH 持续失败时 ts 永远是旧值，「缓存过期」恒为真，
        #    采集线程（5s 一轮）会每一次都发起 SSH，形成重试风暴，反而加重路由器负担。
        #    刷新后失败也会等满一个 TTL 才重试。
        with _up_lock:
            old = _up_cfg["data"]
            _up_cfg["err"] = err or "读取上游配置失败"
            _up_cfg["ts"] = now
        return (old or {"forward": [], "dns": {}}), (err or "读取上游配置失败")
    with _up_lock:
        _up_cfg["data"] = data
        _up_cfg["ts"] = now
        _up_cfg["err"] = ""
    return data, ""


def _parse_upstream(addr):
    """把 MosDNS 的 upstream addr 拆成 主机 / 端口 / 传输类型 / 是否加密。
    路由器上实际出现的形态（2026-10-03 读配置确认）：
        https://dns.alidns.com/dns-query   DoH  443  加密
        tls://8.8.8.8                      DoT  853  加密
        114.114.114.114                    裸 IP 53   明文（默认 UDP）
        udp://1.1.1.1 / tcp://1.1.1.1      显式协议，53
    """
    s = (addr or "").strip()
    if not s:
        return {"host": "", "port": 53, "proto": "-", "secure": False}
    scheme = ""
    m = re.match(r"^([A-Za-z0-9+.-]+)://", s)
    if m:
        scheme = m.group(1).lower()
        s = s[m.end():]
    s = s.split("/")[0]                     # 去掉 DoH 的 /dns-query 路径
    host, port = s, None
    if s.startswith("["):                   # IPv6 字面量 [::1]:53
        i = s.find("]")
        if i > 0:
            host = s[1:i]
            rest = s[i + 1:]
            if rest.startswith(":") and rest[1:].isdigit():
                port = int(rest[1:])
    elif s.count(":") == 1:
        h2, p2 = s.split(":", 1)
        if p2.isdigit():
            host, port = h2, int(p2)
    if scheme in ("https", "h2", "h3", "doh"):
        proto, secure, dport = "DoH", True, 443
    elif scheme in ("tls", "dot"):
        proto, secure, dport = "DoT", True, 853
    elif scheme in ("quic", "doq"):
        proto, secure, dport = "DoQ", True, 853
    elif scheme == "tcp":
        proto, secure, dport = "TCP", False, 53
    else:                                   # udp:// 或裸地址
        proto, secure, dport = "UDP", False, 53
    return {"host": host, "port": port or dport, "proto": proto, "secure": secure}


def read_upstream_stats():
    """按上游聚合本地日志库。返回 (dict[upstream -> 统计], 数据窗口)。

    ⚠️ 口径说明：聚合的是**面板自己累积的 logs 表**，不是 MosDNS 的累计值。
       好处是「清除重计」清空本地日志后这张表会跟着归零，和面板其它统计语义一致；
       代价是它只覆盖本地日志库的窗口（LOG_KEEP 条），不是真正的「今天/近 7 天」。
       窗口范围会一起返回，前端如实标注出来，不假装是全量。
    """
    out, win = {}, (None, None)
    try:
        c = db()
        with _db_lock:
            # ⚠️ 「成功」的口径必须是**上游正常应答**，不是「查到了 IP」。
            #   NXDOMAIN 是 DNS 协议里正常的「该域名确实不存在」响应 —— 上游明明好好回话了，
            #   把它算成失败会让成功率被系统性低估。2026-10-03 实测：dns.google 的 5 次调用
            #   全是 NXDOMAIN，于是表格里显示「全部失败 0.0%」，可它压根一直是通的。
            #   真正的上游失败是 SERVFAIL / REFUSED / 超时这类，NOT IN 兜住即可。
            rows = c.execute(
                "SELECT upstream, COUNT(*), "
                "SUM(CASE WHEN status IN ('NOERROR','NXDOMAIN') THEN 1 ELSE 0 END), "
                "AVG(elapsed), MIN(ts), MAX(ts) FROM logs "
                "WHERE upstream IS NOT NULL AND upstream<>'' AND upstream<>'cache' "
                "GROUP BY upstream ORDER BY COUNT(*) DESC").fetchall()
            w = c.execute("SELECT MIN(ts), MAX(ts), COUNT(*) FROM logs").fetchone()
        for up, cnt, ok, avg_ms, first, last in rows:
            out[up] = {"c": cnt, "ok": ok or 0,
                       "ms": round(float(avg_ms or 0), 2),
                       "first": first, "last": last}
        if w:
            win = (w[0], w[1], w[2] or 0)
    except Exception as e:
        sys.stderr.write("[upstream] 聚合失败: %s\n" % e)
    return out, win


def read_upstreams():
    """组装「上游服务器」表格的数据：配置清单（权威）+ 日志聚合（统计）。"""
    cfg, cfg_err = read_upstream_cfg()
    dns_map = cfg.get("dns") or {}
    stats, win = read_upstream_stats()

    # 日志里的 upstream 取值形态实测有：完整 addr（https://dns.alidns.com/dns-query）、
    # 裸 IP（114.114.115.115）、cache（已排除）、空串。先按 addr 原文精确匹配，
    # 不中再退一步按主机名/IP 匹配，避免因为 http3/h2 这类写法差异漏掉统计。
    by_host = {}
    for up, st in stats.items():
        by_host.setdefault(_parse_upstream(up)["host"], {}).update(st)

    rows, seen, used = [], {}, set()
    for u in (cfg.get("forward") or []):
        addr = (u.get("addr") or "").strip()
        if not addr:
            continue
        p = _parse_upstream(addr)
        st = stats.get(addr) or by_host.get(p["host"]) or {}
        for k in (addr, p["host"]):
            if k in stats:
                used.add(k)
        # ⚠️ 同一个 addr 在配置里会出现多次，**一律合并成一行**：
        #   ① 同一个 forward 里一条 enable_http3:true + 一条不带 → 同一上游的两种连接方式
        #      （QUIC/UDP 的 h3 与 TCP 的 h2）。2026-10-03 曾拆成两行，但用户指出
        #      「成功率/延迟/次数都一样，分拆没有作用」—— 这是对的：concurrent 并发调用
        #      导致次数必然相等，而日志只记 addr 导致统计无法按协议分开，拆开只是视觉噪音。
        #      → 改为合并一行，用 http3 / http2 两个布尔量表达「两种连接方式都在」，
        #        前端在类型列显示「h3+h2」徽章。
        #   ② 同一个 addr 被多个 forward 引用（alidns 被 forward_local /
        #      forward_xinfeng_udp / forward_stream_media 三个引用）→ 同一个上游，必须合并。
        h3 = bool(u.get("http3"))
        if addr in seen:
            if h3:
                seen[addr]["http3"] = True
            else:
                seen[addr]["http2"] = True
            continue
        ip = p["host"] if re.match(r"^[\d.]+$", p["host"]) else ""
        if not ip:
            got = dns_map.get(p["host"]) or []
            ip = ", ".join(got[:2]) if got else ""
        row = {
            "addr": addr, "tag": u.get("tag") or "",
            "host": p["host"], "ip": ip, "port": p["port"],
            "proto": p["proto"], "secure": p["secure"],
            "http3": h3, "http2": (not h3),
            "bootstrap": u.get("bootstrap") or "",
            "c": st.get("c", 0), "ok": st.get("ok", 0),
            "ms": st.get("ms"), "first": st.get("first"), "last": st.get("last"),
        }
        seen[addr] = row
        rows.append(row)

    # 日志里出现过、但配置里已经删掉的上游：不再进表格。
    # 2026-10-03 用户把 114.114.114.114 / 114.114.115.115 / tls://8.8.8.8 从路由器配置里
    # 清掉了，但历史日志里还留着它们的调用记录 —— 如果照旧列出来，用户「配置已经删了，
    # 表里怎么还有」就成了 BUG。所以主表严格只反映「当前配置」。
    # 但这份流量不该凭空消失：单独收进 orphans 字段，接口里仍可查，表格下方给一句灰色小字。
    orphans = []
    for up, st in stats.items():
        if up in used:
            continue
        p = _parse_upstream(up)
        orphans.append({
            "addr": up, "tag": "(不在当前配置中)",
            "host": p["host"],
            "ip": p["host"] if re.match(r"^[\d.]+$", p["host"]) else "",
            "port": p["port"], "proto": p["proto"], "secure": p["secure"],
            "http3": False, "bootstrap": "",
            "c": st.get("c", 0), "ok": st.get("ok", 0),
            "ms": st.get("ms"), "first": st.get("first"), "last": st.get("last"),
        })

    # 缓存命中不走上游，但不说清楚会被误读成「上游一次没用过」
    cached = 0
    try:
        c = db()
        with _db_lock:
            r = c.execute("SELECT COUNT(*) FROM logs WHERE upstream='cache'").fetchone()
            cached = (r[0] if r else 0) or 0
    except Exception:
        pass
    return {
        "rows": rows,
        "orphans": orphans,
        # 清单最后一次**成功**读到路由器配置的时间。前端显示出来，用户一眼能看出
        # 这份清单有多新（TTL 调到 60s 后，这个时间戳应该最多 1 分钟前）。
        # 直接给格式化好的字符串：前端 new Date() 会按浏览器时区走，
        # 万一浏览器时区不是 +08:00 就会显示成别的时间，不如服务端定死。
        "cfg_ts": (time.strftime("%H:%M:%S", time.localtime(_up_cfg["ts"]))
                   if _up_cfg["ts"] else None),
        "cached": cached,
        "win_from": win[0], "win_to": win[1], "win_n": win[2] if len(win) > 2 else 0,
        "error": cfg_err,
    }


# ---------- 采集 ----------
def collect():
    """一次 SSH 拉齐 stats / top / history / 缓存条数；日志走本地累积库"""
    errors = []
    # /top 默认只给 10 条，必须带 ?limit= 才能拿更多（与 logs 的 ?limit 同理）
    cmd = ('curl -s --noproxy "*" -m 10 "%s/stats"; echo "%s"; '
           'curl -s --noproxy "*" -m 10 "%s/top?limit=%d"; echo "%s"; '
           'curl -s --noproxy "*" -m 10 "%s/history"; echo "%s"; '
           'curl -s --noproxy "*" -m 10 "http://127.0.0.1:9091/metrics" '
           '| grep mosdns_cache_size_current'
           % (STATS_API, SEP, STATS_API, TOP_FETCH, SEP, STATS_API, SEP))
    out, err = ssh(cmd, 30)
    if err:
        errors.append("ssh: %s" % err)

    parts = (out or "").split(SEP)
    while len(parts) < 4:
        parts.append("")
    stats = _jload(parts[0])
    top = _jload(parts[1])
    history = _jload(parts[2])
    for n, v in (("stats", stats), ("top", top), ("history", history)):
        if v is None:
            errors.append("%s: 未取到" % n)

    logs, log_total = read_logs(LOG_DEFAULT)
    if log_total == 0:
        errors.append("logs: 本地累积库为空（后台线程可能尚未跑完首次采集）")

    cache_size = None
    for line in (parts[3] or "").splitlines():
        if line.startswith("mosdns_cache_size_current"):
            try:
                cache_size = int(float(line.split()[-1]))
            except Exception:
                pass

    return {
        "host": MOSDNS_HOST,
        "synced_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stats": stats or {},
        "top_domains": [{"d": (x.get("domain") or "").rstrip("."), "c": x.get("count", 0)}
                        for x in ((top or {}).get("top_domains") or [])[:TOP_FETCH]],
        "top_blocked": [{"d": (x.get("domain") or "").rstrip("."), "c": x.get("count", 0)}
                        for x in ((top or {}).get("top_blocked") or [])[:TOP_FETCH]],
        "top_clients": [{"ip": x.get("client_ip", ""), "c": x.get("count", 0)}
                        for x in ((top or {}).get("top_clients") or [])[:15]],
        "history": (history or {}).get("points") or [],
        "logs": logs,
        "log_total": log_total,
        "log_keep": LOG_KEEP,
        "cache_size": cache_size,
        # 上游服务器表：配置清单走 10 分钟缓存（不额外压路由器），统计走本地日志库
        "upstreams": read_upstreams(),
        "errors": errors,
    }


# ---------- 日志累积 ----------
# MosDNS 单次最多只吐 500 条（?limit=5000 也一样），要看 2000 条只能自己攒。
# 后台线程每 INGEST_INTERVAL 秒拉一次，按 id 去重写进 SQLite，只保留最近 LOG_KEEP 条。
DB_PATH = os.path.join(HERE, "logs.db")
_db_lock = threading.Lock()
_conn = None


def db():
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.execute("""CREATE TABLE IF NOT EXISTS logs(
            id TEXT PRIMARY KEY, ts TEXT, domain TEXT, qtype TEXT, status TEXT,
            blocked INTEGER, cached INTEGER, elapsed REAL, upstream TEXT,
            rule TEXT, client TEXT, answers TEXT, total INTEGER)""")
        _conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(ts)")
        # 真实客户端日志（来自 AGH，含客户端 IP / MAC）——MosDNS 那层拿不到
        _conn.execute("""CREATE TABLE IF NOT EXISTS alogs(
            id TEXT PRIMARY KEY, ts TEXT, domain TEXT, qtype TEXT,
            client TEXT, mac TEXT, status TEXT, blocked INTEGER, cached INTEGER,
            elapsed REAL, upstream TEXT, rule TEXT, reason TEXT, answers TEXT)""")
        _conn.execute("CREATE INDEX IF NOT EXISTS idx_alogs_ts ON alogs(ts)")
        _conn.execute("CREATE INDEX IF NOT EXISTS idx_alogs_client ON alogs(client)")
        # 分钟级聚合：存的是「这一分钟的增量」，不是累计值，查询时直接 SUM
        _conn.execute("""CREATE TABLE IF NOT EXISTS stats_min(
            ts TEXT PRIMARY KEY,
            d_total INTEGER, d_cached INTEGER, d_blocked INTEGER,
            avg_ms REAL, total INTEGER)""")
        # 小时级聚合：由 stats_min rollup 而来，保留 400 天，供 7 天 / 30 天视图使用
        _conn.execute("""CREATE TABLE IF NOT EXISTS stats_hour(
            ts TEXT PRIMARY KEY,
            d_total INTEGER, d_cached INTEGER, d_blocked INTEGER,
            avg_ms REAL, total INTEGER, n INTEGER)""")
        # 小时级域名榜：同样存增量
        _conn.execute("""CREATE TABLE IF NOT EXISTS top_hour(
            ts TEXT, kind TEXT, d TEXT, c INTEGER,
            PRIMARY KEY(ts, kind, d))""")
        _conn.execute("CREATE INDEX IF NOT EXISTS idx_th_kind ON top_hour(kind, ts)")
        # 域名累计（永久）
        _conn.execute("""CREATE TABLE IF NOT EXISTS domain_total(
            d TEXT PRIMARY KEY, c INTEGER DEFAULT 0, blocked INTEGER DEFAULT 0,
            first_seen TEXT, last_seen TEXT)""")
        _conn.execute("CREATE INDEX IF NOT EXISTS idx_dt_c ON domain_total(c DESC)")
        _conn.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
        _conn.commit()
    return _conn


def _row_of(it):
    ans = (it.get("answers") or [])[:ANS_LIMIT]
    return (
        it.get("id") or "",
        it.get("timestamp") or "",
        (it.get("domain") or "").rstrip("."),
        it.get("qtype") or "",
        it.get("status") or "",
        1 if it.get("is_blocked") else 0,
        1 if it.get("is_cached") else 0,
        float(it.get("elapsed_ms") or 0),
        it.get("upstream") or "",
        it.get("rule") or "",
        it.get("client_ip") or "",
        json.dumps([{"t": a.get("type"), "v": (a.get("data") or "").rstrip(".")}
                    for a in ans], ensure_ascii=False),
        len(it.get("answers") or []),
    )


def ingest():
    out, err = ssh('curl -s --noproxy "*" -m 12 "%s/logs?limit=%d"'
                   % (STATS_API, LOG_PAGE), 25)
    j = _jload(out)
    items = (j or {}).get("items") or []
    if not items:
        return 0, err
    rows = [_row_of(x) for x in items if x.get("id")]
    c = db()
    with _db_lock:
        c.executemany("INSERT OR IGNORE INTO logs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        c.execute("DELETE FROM logs WHERE id NOT IN "
                  "(SELECT id FROM logs ORDER BY ts DESC, id DESC LIMIT ?)", (LOG_KEEP,))
        c.commit()
    return len(rows), err


def _agh_ts(s):
    """AGH 的 querylog 里 time 是 UTC（如 2026-10-02T18:31:25Z），
    而 MosDNS 那张 logs 表用的是本地时间，两边放一起会差 8 小时，
    所以这里统一转成本地时间再入库。"""
    s = (s or "").strip()
    if not s:
        return ""
    try:
        t = s.replace("Z", "+00:00")
        # 有些版本会带 9 位纳秒，fromisoformat 只吃 6 位
        t = re.sub(r"\.(\d{6})\d+", r".\1", t)
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return s.replace("T", " ")[:19]


def _arow_of(it, macs):
    """AGH querylog 记录 → alogs 表一行。id 用 时间+客户端+域名+类型 拼，
    AGH 不提供稳定 id，这样能天然去重。"""
    q = it.get("question") or {}
    dom = (q.get("name") or "").rstrip(".")
    qt = q.get("type") or ""
    cl = it.get("client") or ""
    ts = _agh_ts(it.get("time"))
    ans = (it.get("answer") or [])[:ANS_LIMIT]
    blocked = 1 if (it.get("rule") or it.get("reason") == "FilteredBlackList") else 0
    return (
        "%s|%s|%s|%s" % (ts, cl, dom, qt),
        ts, dom, qt, cl, macs.get(cl, ""),
        it.get("status") or "",
        blocked, 1 if it.get("cached") else 0,
        float(it.get("elapsedMs") or 0),
        it.get("upstream") or "",
        it.get("rule") or "", it.get("reason") or "",
        json.dumps([{"t": a.get("type"), "v": (a.get("value") or "").rstrip(".")}
                    for a in ans], ensure_ascii=False),
    )


def ingest_agh():
    """从 AGH 拉最近的查询日志，落到 alogs（含真实客户端 IP / MAC）"""
    j = agh_querylog(limit=AGH_PAGE)
    if not j:
        return 0, _agh.get("err") or "拉取失败"
    items = (j.get("data") or [])
    if not items:
        return 0, None
    macs = read_macs()
    rows = [_arow_of(x, macs) for x in items
            if (x.get("question") or {}).get("name")]
    conn = db()
    with _db_lock:
        conn.executemany("INSERT OR IGNORE INTO alogs VALUES "
                         "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        conn.execute("DELETE FROM alogs WHERE id NOT IN "
                     "(SELECT id FROM alogs ORDER BY ts DESC, id DESC LIMIT ?)",
                     (LOG_KEEP,))
        conn.commit()
    return len(rows), None


# MAC 用「实时补」而不是「入库时固化」：设备换网卡/换 IP 后，
# 靠写入那一刻的快照会一直显示旧 MAC，而且入库时查不到邻居就永久留空。
_mac_cache = {"ts": 0.0, "map": {}}


def macs_now():
    if time.time() - _mac_cache["ts"] > 60:
        _mac_cache["map"] = read_macs()
        _mac_cache["ts"] = time.time()
    return _mac_cache["map"]


def read_alogs(n=LOG_DEFAULT):
    conn = db()
    with _db_lock:
        cur = conn.execute(
            "SELECT id,ts,domain,qtype,client,mac,status,blocked,cached,"
            "elapsed,upstream,rule,reason,answers "
            "FROM alogs ORDER BY ts DESC, id DESC LIMIT ?", (int(n),))
        rows = cur.fetchall()
        total = conn.execute("SELECT COUNT(*) FROM alogs").fetchone()[0]
    dev = dev_names()
    macs = macs_now()
    out = []
    for r in rows:
        try:
            a = json.loads(r[13] or "[]")
        except Exception:
            a = []
        out.append({"t": r[1], "d": r[2], "q": r[3], "ip": r[4],
                    "mac": macs.get(r[4]) or r[5] or "",
                    "s": r[6], "b": r[7], "c": r[8], "e": r[9], "u": r[10],
                    "r": r[11], "why": r[12], "a": a, "n": len(a),
                    "name": dev.get(r[4], "")})
    return out, total


def read_client_top(limit=30):
    """按客户端统计查询量（从 alogs 现算，不依赖 AGH 的统计接口）"""
    conn = db()
    with _db_lock:
        rows = conn.execute(
            "SELECT client, COUNT(*) c, SUM(blocked) b FROM alogs "
            "GROUP BY client ORDER BY c DESC LIMIT ?", (int(limit),)).fetchall()
    dev = dev_names()
    macs = macs_now()
    hosts = _ik.get("host") or {}
    return [{"ip": r[0], "mac": macs.get(r[0], ""), "c": int(r[1] or 0),
             "b": int(r[2] or 0), "name": dev.get(r[0], ""),
             "host": hosts.get(r[0], "")} for r in rows]


def _norm_log_ts(ts):
    """logs.ts 是 ISO8601('2026-10-03T10:47:02+08:00')，alogs.ts 是
    'YYYY-MM-DD HH:MM:SS'。统一到后者的前 19 位才能互相比较。"""
    return (ts or "")[:19].replace("T", " ")


def _map_real_clients(conn, rows):
    """把 MosDNS 日志里的客户端反查成真实设备 IP。

    背景：路由器上 dnsmasq 配的是 server=127.0.0.1#5335，查询由 dnsmasq 转发进
    MosDNS，源地址被改写为本机 —— 所以 logs 表的 client 恒为 ::ffff:127.0.0.1。
    AdGuard Home 的 alogs 表记录的是真实客户端（含 IP/MAC），且同一条查询
    在两表里**时间戳精确到同一秒、域名和查询类型一致**，可以用
    (时间秒, 域名, qtype) 做关联反查。实测 200 条样本命中率 99.5%。

    返回 {(ts19, domain, qtype): [候选客户端, ...]}，只保留能匹配上的键。
    同一秒同一域名有多台设备查询时会有多个候选（实测约 24.5%），
    这种情况无法判断到底是哪台，如实把候选都返回，由前端标注「等 N 台」。
    """
    if not rows:
        return {}
    ts_list = [_norm_log_ts(r[1]) for r in rows]
    lo, hi = min(ts_list), max(ts_list)
    amap = {}
    try:
        cur = conn.execute(
            "SELECT substr(ts,1,19), domain, qtype, client FROM alogs "
            "WHERE substr(ts,1,19) BETWEEN ? AND ?", (lo, hi))
        for ts19, dom, qt, cli in cur.fetchall():
            if not cli:
                continue
            amap.setdefault((ts19, dom, qt), []).append(cli)
    except Exception as e:
        sys.stderr.write("[clientmap] alogs 关联失败: %s\n" % e)
        return {}
    out = {}
    for r in rows:
        key = (_norm_log_ts(r[1]), r[2], r[3])
        cands = amap.get(key)
        if not cands:
            continue
        uniq = []
        for x in cands:                       # 去重但保持顺序
            if x not in uniq:
                uniq.append(x)
        out[key] = uniq
    return out


def read_logs(n=LOG_DEFAULT):
    c = db()
    with _db_lock:
        cur = c.execute("SELECT id,ts,domain,qtype,status,blocked,cached,elapsed,"
                        "upstream,rule,client,answers,total "
                        "FROM logs ORDER BY ts DESC, id DESC LIMIT ?", (int(n),))
        rows = cur.fetchall()
        total = c.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
        cmap = _map_real_clients(c, rows)
    out = []
    for r in rows:
        try:
            a = json.loads(r[11] or "[]")
        except Exception:
            a = []
        # ip2 = 反查到的真实客户端（列表），ip2n = 候选数（>1 表示同秒有多台设备）
        real = cmap.get((_norm_log_ts(r[1]), r[2], r[3])) or []
        out.append({"t": r[1], "d": r[2], "q": r[3], "s": r[4], "b": r[5], "c": r[6],
                    "e": r[7], "u": r[8], "r": r[9], "ip": r[10], "a": a, "n": r[12],
                    "ip2": real, "ip2n": len(real)})
    return out, total


# ---------- 时序采样（分层存储） ----------
# 存的是「增量」而不是累计值，两个理由：
#   1) 查询时直接 SUM，不用做首尾差分，区间统计简单且准确
#   2) MosDNS 重置累计值时（重启 / 手动清零 / stats.dump 回滚）增量不会出现负数
# 采样用的是 collect() 的**原始**结果，不走面板基线 ——
# 面板「清除重计」只是改显示，历史库始终记录真实数据。

def _meta_get(k, default=None):
    conn = db()
    with _db_lock:
        r = conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    if not r:
        return default
    try:
        return json.loads(r[0])
    except Exception:
        return default


def _meta_set(k, v):
    conn = db()
    with _db_lock:
        conn.execute("INSERT INTO meta(k,v) VALUES(?,?) "
                     "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                     (k, json.dumps(v, ensure_ascii=False)))
        conn.commit()


def sample_min():
    """每分钟记一条增量。返回 (是否写入, 增量字典)"""
    data = collect()
    s = data.get("stats") or {}
    cur_total = int(s.get("total_queries") or 0)
    if not cur_total:
        return False, None
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    cur = {"total": cur_total,
           "cached": int(s.get("cached_queries") or 0),
           "blocked": int(s.get("blocked_queries") or 0)}
    prev = _meta_get("last_stats")
    if not prev:
        d = {k: 0 for k in cur}                     # 首次采样没有前值，不能当增量
    elif any(cur[k] < int(prev.get(k) or 0) for k in cur):
        d = {k: 0 for k in cur}                     # 累计值倒退 => MosDNS 重置过
    else:
        d = {k: cur[k] - int(prev.get(k) or 0) for k in cur}
    _meta_set("last_stats", cur)
    conn = db()
    with _db_lock:
        conn.execute("INSERT INTO stats_min(ts,d_total,d_cached,d_blocked,avg_ms,total) "
                     "VALUES(?,?,?,?,?,?) ON CONFLICT(ts) DO UPDATE SET "
                     "d_total=d_total+excluded.d_total, d_cached=d_cached+excluded.d_cached, "
                     "d_blocked=d_blocked+excluded.d_blocked, avg_ms=excluded.avg_ms, "
                     "total=excluded.total",
                     (ts, d["total"], d["cached"], d["blocked"],
                      float(s.get("avg_latency_ms") or 0), cur["total"]))
        conn.commit()
    return True, d


def rollup_hour():
    """把已经走完的小时的分钟数据汇总进 stats_hour（保留 400 天）"""
    cur_h = datetime.now().strftime("%Y-%m-%d %H:00")
    conn = db()
    with _db_lock:
        rows = conn.execute(
            "SELECT substr(ts,1,13)||':00' AS h, SUM(d_total), SUM(d_cached), "
            "SUM(d_blocked), AVG(avg_ms), MAX(total), COUNT(*) "
            "FROM stats_min WHERE substr(ts,1,13)||':00' < ? GROUP BY h", (cur_h,)).fetchall()
        for h, t, ca, b, am, tt, n in rows:
            conn.execute("INSERT INTO stats_hour(ts,d_total,d_cached,d_blocked,avg_ms,total,n) "
                         "VALUES(?,?,?,?,?,?,?) ON CONFLICT(ts) DO UPDATE SET "
                         "d_total=excluded.d_total, d_cached=excluded.d_cached, "
                         "d_blocked=excluded.d_blocked, avg_ms=excluded.avg_ms, "
                         "total=excluded.total, n=excluded.n",
                         (h, t or 0, ca or 0, b or 0, am or 0, tt or 0, n))
        conn.commit()
    return len(rows)


def sample_hour():
    """每小时记一次域名榜增量 + 累加域名永久累计"""
    data = collect()
    ts = datetime.now().strftime("%Y-%m-%d %H:00")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = db()
    written = 0
    for kind, key in (("domain", "top_domains"), ("blocked", "top_blocked")):
        cur = {x["d"]: int(x["c"] or 0) for x in (data.get(key) or [])}
        prev = _meta_get("last_top_" + kind)
        if prev is None:
            # 首次采样没有前值，只建基准。若把累计值当增量写进去，
            # 第一个小时的次数会虚高成 MosDNS 的历史累计。
            _meta_set("last_top_" + kind, cur)
            continue
        rows = []
        for d, v in cur.items():
            delta = v - int(prev.get(d) or 0)
            if delta < 0:
                delta = v                       # 重置过，按当前值重新计数
            if delta > 0:
                rows.append((d, delta))
        with _db_lock:
            conn.executemany("INSERT INTO top_hour(ts,kind,d,c) VALUES(?,?,?,?) "
                             "ON CONFLICT(ts,kind,d) DO UPDATE SET c=c+excluded.c",
                             [(ts, kind, d, c) for d, c in rows])
            for d, delta in rows:
                conn.execute(
                    "INSERT INTO domain_total(d,c,blocked,first_seen,last_seen) "
                    "VALUES(?,?,?,?,?) ON CONFLICT(d) DO UPDATE SET "
                    "c=c+excluded.c, blocked=blocked+excluded.blocked, "
                    "last_seen=excluded.last_seen",
                    (d, delta if kind == "domain" else 0,
                     delta if kind == "blocked" else 0, now, now))
            conn.commit()
        _meta_set("last_top_" + kind, cur)
        written += len(rows)
    return written


def prune():
    """按分层保留策略清理过期数据；返回各表删除行数"""
    now = datetime.now()
    cut_min = (now - timedelta(days=KEEP_MIN_DAYS)).strftime("%Y-%m-%d %H:%M")
    cut_hour = (now - timedelta(days=KEEP_HOUR_DAYS)).strftime("%Y-%m-%d %H:00")
    conn = db()
    with _db_lock:
        a = conn.execute("DELETE FROM stats_min WHERE ts < ?", (cut_min,)).rowcount
        b = conn.execute("DELETE FROM stats_hour WHERE ts < ?", (cut_hour,)).rowcount
        c = conn.execute("DELETE FROM top_hour WHERE ts < ?", (cut_hour,)).rowcount
        conn.commit()
    return {"stats_min": a, "stats_hour": b, "top_hour": c}


def _range_start(rng, unit):
    """返回区间起始时间字符串，unit: min|hour"""
    now = datetime.now()
    days = {"today": 1, "7d": 7, "30d": 30}.get(rng, 1)
    if rng == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        start = (now - timedelta(days=days)).replace(minute=0, second=0, microsecond=0)
    return start.strftime("%Y-%m-%d %H:%M" if unit == "min" else "%Y-%m-%d %H:00")


def _hist_avg_ms(rng):
    """区间内的平均延迟（按查询量加权，比简单算术平均更接近真实体感）"""
    conn = db()
    try:
        with _db_lock:
            if rng == "today":
                st = _range_start("today", "min")
                row = conn.execute(
                    "SELECT SUM(avg_ms*d_total), SUM(d_total) FROM stats_min WHERE ts >= ?",
                    (st,)).fetchone()
            else:
                if rng == "all":
                    row = conn.execute(
                        "SELECT SUM(avg_ms*d_total), SUM(d_total) FROM stats_hour").fetchone()
                else:
                    st = _range_start(rng, "hour")
                    row = conn.execute(
                        "SELECT SUM(avg_ms*d_total), SUM(d_total) FROM stats_hour WHERE ts >= ?",
                        (st,)).fetchone()
            num, den = (row[0] or 0), (row[1] or 0)
        return round(float(num) / float(den), 2) if den else 0.0
    except Exception:
        return 0.0


def _range_seconds(rng, first_ts):
    """区间跨度的秒数，用来算区间平均 QPS"""
    now = datetime.now()
    if rng == "today":
        return max(60.0, (now - now.replace(hour=0, minute=0, second=0,
                                            microsecond=0)).total_seconds())
    if rng in ("7d", "30d"):
        return float({"7d": 7, "30d": 30}[rng] * 86400)
    # all：从库里第一条数据算起
    if first_ts:
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:00", "%Y-%m-%d"):
            try:
                return max(60.0, (now - datetime.strptime(first_ts[:16], fmt)).total_seconds())
            except Exception:
                continue
    return 86400.0


def read_history(rng="today"):
    """返回 [{time,total,cached,blocked}]，时间粒度随区间自动放大"""

    conn = db()
    args = ()
    if rng == "today":
        start = _range_start(rng, "min")
        sql = ("SELECT substr(ts,1,15)||'0' AS g, SUM(d_total), SUM(d_cached), SUM(d_blocked) "
               "FROM stats_min WHERE ts >= ? GROUP BY g ORDER BY g")
        src, args = "stats_min", (start,)
    elif rng == "7d":
        # stats_hour 只覆盖「已走完的整点」；当前这个小时还没 rollup，
        # 从 stats_min 补上，否则刚部署完的几小时里 7 天视图是空的。
        start = _range_start(rng, "hour")
        sql = ("SELECT g, SUM(t), SUM(c), SUM(b) FROM ("
               "  SELECT ts AS g, d_total t, d_cached c, d_blocked b FROM stats_hour WHERE ts >= ?"
               "  UNION ALL"
               "  SELECT substr(ts,1,13)||':00', d_total, d_cached, d_blocked FROM stats_min"
               "    WHERE ts >= ? AND substr(ts,1,13)||':00' >"
               "          (SELECT IFNULL(MAX(ts),'0000') FROM stats_hour)"
               ") GROUP BY g ORDER BY g")
        src, args = "stats_hour+stats_min", (start, start)
    elif rng == "all":
        start = None
        sql = ("SELECT g, SUM(t), SUM(c), SUM(b) FROM ("
               "  SELECT substr(ts,1,10) AS g, d_total t, d_cached c, d_blocked b FROM stats_hour"
               "  UNION ALL"
               "  SELECT substr(ts,1,10), d_total, d_cached, d_blocked FROM stats_min"
               "    WHERE substr(ts,1,13)||':00' >"
               "          (SELECT IFNULL(MAX(ts),'0000') FROM stats_hour)"
               ") GROUP BY g ORDER BY g")
        src = "stats_hour+stats_min"
    else:                                   # 30d
        start = _range_start(rng, "hour")
        sql = ("SELECT g, SUM(t), SUM(c), SUM(b) FROM ("
               "  SELECT substr(ts,1,10) AS g, d_total t, d_cached c, d_blocked b"
               "    FROM stats_hour WHERE ts >= ?"
               "  UNION ALL"
               "  SELECT substr(ts,1,10), d_total, d_cached, d_blocked FROM stats_min"
               "    WHERE ts >= ? AND substr(ts,1,13)||':00' >"
               "          (SELECT IFNULL(MAX(ts),'0000') FROM stats_hour)"
               ") GROUP BY g ORDER BY g")
        src, args = "stats_hour+stats_min", (start, start)
    with _db_lock:
        rows = conn.execute(sql, args).fetchall()
        have_min = conn.execute("SELECT COUNT(*) FROM stats_min").fetchone()[0]
        have_hour = conn.execute("SELECT COUNT(*) FROM stats_hour").fetchone()[0]
        first = conn.execute("SELECT MIN(ts) FROM stats_min").fetchone()[0]
        first_h = conn.execute("SELECT MIN(ts) FROM stats_hour").fetchone()[0]
    pts = [{"time": r[0], "total": int(r[1] or 0),
            "cached": int(r[2] or 0), "blocked": int(r[3] or 0)} for r in rows]

    # ---- 区间汇总：给 KPI 卡片用 ----
    # 键名刻意与 MosDNS stats 保持一致，前端就能"换数据源但不换渲染逻辑"。
    # hosts_queries / cache_size 是 MosDNS 的瞬时或累计状态，历史库里没存，置 None。
    t_sum = sum(p["total"] for p in pts)
    c_sum = sum(p["cached"] for p in pts)
    b_sum = sum(p["blocked"] for p in pts)
    secs = _range_seconds(rng, first or first_h)
    total_pts = {
        "total_queries": t_sum,
        "cached_queries": c_sum,
        "blocked_queries": b_sum,
        "cached_percentage": round(c_sum / t_sum * 100, 2) if t_sum else 0,
        "blocked_percentage": round(b_sum / t_sum * 100, 2) if t_sum else 0,
        "avg_latency_ms": _hist_avg_ms(rng),
        "qps": round(t_sum / secs, 2) if secs else 0,
        "hosts_queries": None,      # 历史库没有这一项
        "cache_size": None,         # 这是"此刻"的状态，不属于区间统计
    }
    return {"points": pts, "src": src, "rows_min": have_min, "rows_hour": have_hour,
            "first_ts": first or first_h, "sum": total_pts, "seconds": int(secs)}


def read_top_range(rng="today", kind="domain", limit=50):
    """区间内的域名榜（增量求和）"""
    start = _range_start(rng, "hour")
    conn = db()
    with _db_lock:
        rows = conn.execute("SELECT d, SUM(c) AS s FROM top_hour "
                            "WHERE kind=? AND ts >= ? GROUP BY d ORDER BY s DESC LIMIT ?",
                            (kind, start, int(limit))).fetchall()
    return [{"d": r[0], "c": int(r[1] or 0)} for r in rows]


def read_domain_total(limit=50, blocked=False):
    """永久累计榜"""
    conn = db()
    col = "blocked" if blocked else "c"
    with _db_lock:
        rows = conn.execute("SELECT d, %s AS v, first_seen, last_seen FROM domain_total "
                            "WHERE %s > 0 ORDER BY v DESC LIMIT ?" % (col, col),
                            (int(limit),)).fetchall()
    return [{"d": r[0], "c": int(r[1] or 0), "first": r[2], "last": r[3]} for r in rows]


# ---------- AdGuard Home：真实客户端 IP ----------
# 为什么需要它：路由器上 dnsmasq 的配置是 server=127.0.0.1#5335，
# 查询由 dnsmasq 转发进 MosDNS，源地址被改写成回环，
# 所以 MosDNS 层看到的客户端恒为 ::ffff:127.0.0.1，拿不到真实设备。
# AGH 监听 5553、数据面真实可见客户端，链路是 设备 → AGH:5553 → MosDNS:127.0.0.1:5335。
# 注意：AGH 强制 HTTPS + 登录态，所以走 3443 + cookie；凭据放 agh.conf（600）。
_agh = {"cookie": "", "ts": 0.0, "err": "", "ok": False}


def agh_conf():
    cfg = {}
    try:
        with open(AGH_CONF, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                cfg[k.strip()] = v.strip()
    except Exception as e:
        sys.stderr.write("[agh] 读配置失败: %s\n" % e)
    return cfg


def load_devices():
    """devices.conf: 192.168.1.200=我的台式机（可选，用来给纯 IP 补个名字）"""
    dev = {}
    try:
        with open(DEV_CONF, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    ip, name = line.split("=", 1)
                elif line:
                    ip, name = line, ""
                else:
                    continue
                dev[ip.strip()] = name.strip()
    except Exception:
        pass
    return dev


# ---------- 爱快（iKuai）----------
# 爱快的 Web 管理界面本身就是一套 JSON API：
#   POST /Action/login  {"username","passwd"(md5),"pass"(base64)} -> sess_key
#   POST /Action/call   {"func_name":"dhcp_lease","action":"show","param":{...}}
# 不需要在爱快上额外开任何服务，只要 Web 管理界面能访问 + 有管理员账号。
IK_CONF = os.path.join(HERE, "ikuai.conf")
IK_TTL = 1800          # sess_key 缓存时长（秒）
IK_NAME_TTL = 300      # 设备名缓存时长（秒）
_ik = {"ts": 0.0, "sess": "", "user": "", "err": "", "ok": False,
       "names": {}, "host": {}, "names_ts": 0.0}


def ik_conf():
    cfg = {"IKUAI_URL": "https://192.168.1.1", "IKUAI_USER": "admin",
           "IKUAI_PASS": ""}
    try:
        with open(IK_CONF, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                cfg[k.strip()] = v.strip()
    except Exception:
        pass
    return cfg


def _ik_ctx():
    """爱快用的是自签证书，跟 AGH 一样不校验"""
    import ssl
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _ik_post(path, body, cookie=None):
    import urllib.request
    cfg = ik_conf()
    url = cfg["IKUAI_URL"].rstrip("/") + path
    headers = {"Content-Type": "application/json;charset=UTF-8"}
    if cookie:
        headers["Cookie"] = cookie
    req = urllib.request.Request(url, data=body.encode("utf-8"),
                                 headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=10, context=_ik_ctx()) as r:
        raw = r.read().decode("utf-8", "replace")
        hdrs = dict(r.headers.items())
        # 可能出现多条 Set-Cookie，dict() 会把它们合并丢掉，单独取出来拼起来
        scs = r.headers.get_all("Set-Cookie") or []
        if len(scs) > 1:
            hdrs["Set-Cookie"] = "\n".join(scs)
        return raw, hdrs


def ik_login(force=False):
    """登录爱快拿 sess_key。V3 用 Result，V4 用 code/Data.sess_key，两边都兼容"""
    if not force and _ik["sess"] and time.time() - _ik["ts"] < IK_TTL:
        return True
    cfg = ik_conf()
    if not cfg.get("IKUAI_PASS"):
        _ik["ok"] = False
        _ik["err"] = "未配置 ikuai.conf（IKUAI_PASS 为空）"
        return False
    try:
        import hashlib
        import base64
        pwd = cfg["IKUAI_PASS"]
        body = json.dumps({
            "username": cfg["IKUAI_USER"],
            "passwd": hashlib.md5(pwd.encode("utf-8")).hexdigest(),
            "pass": base64.b64encode(
                ("salt_113" + pwd).encode("utf-8")).decode("ascii"),
        }, ensure_ascii=False)
        raw, hdrs = _ik_post("/Action/login", body)
        j = {}
        try:
            j = json.loads(raw)
        except Exception:
            j = {}
        # 成功时 V4: {"code":0,...}  V3: {"Result":10000,...}
        code = j.get("code", j.get("Result"))
        if code not in (0, 10000, "0", "10000"):
            _ik["ok"] = False
            _ik["err"] = j.get("message") or j.get("ErrMsg") or raw[:80]
            return False
        # sess_key 优先从 Set-Cookie 取（爱快 V4 只放这里），
        # body 里的 Result=10000 是状态码不是 sess_key，千万别当成 key。
        sess = ""
        for part in re.split(r"[;\n]", hdrs.get("Set-Cookie") or ""):
            part = part.strip()
            if part.lower().startswith("sess_key="):
                sess = part.split("=", 1)[1].strip()
                break
        if not sess:
            data = j.get("Data") or j.get("data") or {}
            if isinstance(data, dict):
                sess = (data.get("sess_key") or data.get("sess") or "").strip()
        if not sess:
            v = j.get("sess_key")
            if isinstance(v, str) and v.strip():
                sess = v.strip()
        if not sess or sess in ("10000", "0"):
            _ik["ok"] = False
            _ik["err"] = "拿到 code=0 但没解析出 sess_key：%s / cookie=%s" % (
                raw[:100], (hdrs.get("Set-Cookie") or "")[:60])
            return False
        _ik["sess"] = sess
        _ik["user"] = cfg["IKUAI_USER"]
        _ik["ts"] = time.time()
        _ik["ok"] = True
        _ik["err"] = ""
        return True
    except Exception as e:
        _ik["ok"] = False
        _ik["err"] = "登录异常：%s" % e
        return False


def ik_call(func, param, retry=True):
    if not ik_login():
        return None
    body = json.dumps({"func_name": func, "action": "show", "param": param},
                      ensure_ascii=False)
    try:
        raw, _ = _ik_post("/Action/call", body,
                          "username=%s; sess_key=%s" % (_ik["user"], _ik["sess"]))
        j = json.loads(raw)
    except Exception as e:
        _ik["err"] = "调用 %s 失败：%s" % (func, e)
        return None
    code = j.get("code", j.get("Result"))
    if code not in (0, 10000, "0", "10000"):
        if retry:                       # session 过期就重登一次再试
            if ik_login(force=True):
                return ik_call(func, param, retry=False)
        _ik["err"] = "%s：%s" % (func, j.get("message") or j.get("ErrMsg") or "")
        return None
    return j


def ik_names(force=False):
    """从爱快 DHCP 租约表拿 IP -> hostname。缓存 5 分钟，避免频繁打扰路由器"""
    if not force and _ik["names"] and time.time() - _ik["names_ts"] < IK_NAME_TTL:
        return _ik["names"]
    j = ik_call("dhcp_lease", {"TYPE": "total,data", "limit": "0,500"})
    if not j:
        return _ik["names"]
    rows = []
    for key in ("result", "results", "Data", "data"):
        v = j.get(key)
        if isinstance(v, list):
            rows = v
            break
        if isinstance(v, dict) and isinstance(v.get("data"), list):
            rows = v["data"]
            break
    out, host = {}, {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        # 爱快 V4 的租约表里 IP 字段叫 ip_addr，不是 ip
        ip = (r.get("ip_addr") or r.get("ip") or "").strip()
        # termname = 在爱快里给终端打的备注（如「360门铃」），比 hostname 直观，
        # 优先用它；没有备注再退回 DHCP 上报的 hostname。
        # 这两个字段爱快会做 URL 编码（空格 -> %20、单引号 -> %27），要解回来。
        tm = unquote(r.get("termname") or "").strip()
        hn = unquote(r.get("hostname") or r.get("comment") or "").strip()
        if not ip:
            continue
        if hn and hn not in ("*", "-", "?"):
            host[ip] = hn
        name = ""
        for cand in (tm, hn):
            if cand and cand not in ("*", "-", "?"):
                name = cand
                break
        if name:
            out[ip] = name
    if out:
        _ik["names"] = out
        _ik["host"] = host
        _ik["names_ts"] = time.time()
    return _ik["names"]


def dev_names():
    """设备名优先级：手动 devices.conf > 爱快 DHCP hostname"""
    names = {}
    try:
        names.update(ik_names())
    except Exception:
        pass
    names.update(load_devices())
    return names


def agh_login(force=False):
    """拿/续 cookie，30 分钟有效，提前 2 分钟换新的"""
    if not force and _agh["cookie"] and time.time() - _agh["ts"] < 1740:
        return True
    cfg = agh_conf()
    if not (cfg.get("AGH_URL") and cfg.get("AGH_USER")):
        _agh["err"] = "未配置 agh.conf"
        return False
    url = cfg["AGH_URL"].rstrip("/") + "/control/login"
    body = json.dumps({"name": cfg.get("AGH_USER", ""),
                       "password": cfg.get("AGH_PASS", "")})
    cmd = ('curl -sk --noproxy "*" -m 8 -X POST "%s" '
           '-H "Content-Type: application/json" '
           "-D /tmp/agh-h.txt -o /tmp/agh-b.txt -d '%s'; "
           "echo '@@@'; sed -n 's/^[Ss]et-[Cc]ookie: \\([^;]*\\).*/\\1/p' /tmp/agh-h.txt; "
           "rm -f /tmp/agh-h.txt /tmp/agh-b.txt" % (url, body))
    out, err = ssh(cmd, 20)
    if err:
        _agh["err"] = "ssh: %s" % err
        return False
    parts = (out or "").split("@@@")
    cookie = (parts[1].strip() if len(parts) > 1 else "")
    if not cookie:
        _agh["err"] = "登录失败（返回中无 cookie，检查 agh.conf 里的密码）"
        _agh["ok"] = False
        return False
    _agh["cookie"] = cookie
    _agh["ts"] = time.time()
    _agh["ok"] = True
    _agh["err"] = ""
    return True


def agh_get(path, timeout=20, retry=True):
    """带登录态请求 AGH API，401/403 时自动重登一次"""
    if not agh_login():
        return None
    cfg = agh_conf()
    url = cfg["AGH_URL"].rstrip("/") + path
    cmd = ('curl -sk --noproxy "*" -m 15 -w "\\n@@@HTTP%%{http_code}" -H "%s" "%s"'
           % ("Cookie: " + _agh["cookie"], url))
    out, err = ssh(cmd, timeout)
    if err:
        _agh["err"] = "ssh: %s" % err
        return None
    body, _, code = (out or "").rpartition("@@@HTTP")
    code = code.strip()
    if code in ("401", "403") and retry:
        _agh["cookie"] = ""
        if agh_login(force=True):
            return agh_get(path, timeout, retry=False)
        return None
    if code != "200":
        _agh["err"] = "HTTP %s" % code
        return None
    try:
        return json.loads(body)
    except Exception as e:
        _agh["err"] = "JSON 解析失败: %s" % e
        return None


def agh_querylog(limit=500, search=None, older_than=None):
    q = "?limit=%d" % int(limit)
    if search:
        q += "&search=" + quote(str(search))
    if older_than:
        q += "&older_than=" + quote(str(older_than))
    return agh_get("/control/querylog" + q)


def agh_stats():
    return agh_get("/control/stats")


def agh_clients():
    """AGH 自己按客户端汇总的统计（含每客户端查询数），用来做设备榜"""
    return agh_get("/control/stats_top?limit=100")


def read_macs():
    """从路由器的邻居表取 IP→MAC（只读命令，不动任何配置）
    注意 BusyBox 的 ip neigh 输出格式是：
      192.168.1.40 lladdr 8c:d0:b2:b0:36:c4 STALE
    中间夹着 "lladdr"，所以 MAC 在**第 3 列**，不是第 5 列。
    在 Python 侧解析，不用 awk（省一层 shell 转义麻烦）。"""
    out, _ = ssh("ip neigh show dev br-lan 2>/dev/null", 15)
    macs = {}
    for line in (out or "").splitlines():
        p = line.split()
        if len(p) >= 3 and p[1] == "lladdr" and ":" in p[2]:
            macs[p[0]] = p[2].lower()
    return macs


def _sig(d):
    """快照指纹：这几个值没变就不推，避免无意义地让页面重绘"""
    s = d.get("stats") or {}
    td = d.get("top_domains") or []
    return (
        s.get("total_queries"),
        s.get("cached_queries"),
        s.get("blocked_queries"),
        round(float(s.get("cached_percentage") or 0), 2),
        (td[0].get("d"), td[0].get("c")) if td else None,
        len(d.get("history") or []),
        d.get("log_total"),
        d.get("cache_size"),
    )


def sse_payload(force=True):
    """给 SSE 用的快照：剔除 logs 明细（体积大且另有 /api/logs 负责）。

    🔴 **必须和 /api/snapshot 一样套上 apply_baseline**，否则「清除重计」会被打回原形：
       前端 clearStats() 先 pullLive() 拿到已归零的数据、界面刚更新完，
       push_loop 紧接着推来这一帧**未减基线的原始累计值**，
       而前端 applyPush() 是 `DATA = obj` 整体替换 + renderAll() 全量重绘，
       界面立刻跳回清除前的数字 —— 用户看到的就是「点了清除重计没反应」。
       （2026-10-03 用户报的正是这个 bug。）

    这个差错的迷惑性在于：push_loop 里**算指纹用的是套过 baseline 的 d**，
    推送内容却走这个函数用了原始值，两个口径不一致，光看代码不容易发现。
    实锤方式：对比 /api/snapshot 与 /api/stream 首帧的 top_domains[0].c，
    修复前一个是 501（增量）、一个是 1207（原始值），且 since 一个有值一个为 None。"""
    d = apply_baseline(dict(get_snapshot(force=force)), load_baseline())
    d.pop("logs", None)
    d["_push"] = True
    return json.dumps(d, ensure_ascii=False, separators=(",", ":"))


def push_loop():
    """SSE 推送线程：有页面在看就每 PUSH_INTERVAL 秒采一次路由器，数据有变化才广播。

    没人看时降到 PUSH_IDLE_INTERVAL，尽量少去 SSH 路由器。"""
    last_beat = 0.0
    while True:
        try:
            with _sse_lock:
                n_cli = len(_sse_clients)
            if n_cli:
                d = apply_baseline(dict(get_snapshot(force=True)), load_baseline())
                sig = _sig(d)
                if sig != _last_sig["v"]:
                    _last_sig["v"] = sig
                    payload = sse_payload(force=False)
                    with _sse_lock:
                        targets = list(_sse_clients)
                    for q in targets:
                        try:
                            q.put_nowait(payload)
                        except queue.Full:
                            pass                      # 这个页面跟不上，丢掉这一帧
                # 心跳：即使没变化也定期发一行注释，保活 + 让前端知道链路键在
                if time.time() - last_beat >= SSE_HEARTBEAT:
                    last_beat = time.time()
                    beat = {"_hb": True,
                            "synced_at": d.get("synced_at"),
                            "total": (d.get("stats") or {}).get("total_queries")}
                    with _sse_lock:
                        targets = list(_sse_clients)
                    for q in targets:
                        try:
                            q.put_nowait(json.dumps(beat, ensure_ascii=False))
                        except queue.Full:
                            pass
                time.sleep(PUSH_INTERVAL)
            else:
                time.sleep(PUSH_IDLE_INTERVAL)
        except Exception as e:
            sys.stderr.write("[push] 异常: %s\n" % e)
            time.sleep(2)


def collector_loop():
    # 先跑一次 AGH 登录，失败也继续（MosDNS 那条链路不依赖它）
    try:
        if agh_login(force=True):
            sys.stderr.write("[agh] 登录成功 %s\n" % agh_conf().get("AGH_URL", ""))
        else:
            sys.stderr.write("[agh] 登录失败: %s\n" % _agh.get("err"))
    except Exception as e:
        sys.stderr.write("[agh] 登录异常: %s\n" % e)
    # 先各跑一次建立基准（首次采样没有前值，只写基准不算增量）
    for fn, name in ((sample_min, "sample_min"), (sample_hour, "sample_hour")):
        try:
            fn()
        except Exception as e:
            sys.stderr.write("[%s] 首次采样异常: %s\n" % (name, e))
    next_min = time.time() + SAMPLE_MIN
    next_hour = time.time() + SAMPLE_HOUR
    next_prune = time.time() + PRUNE_INTERVAL
    while True:
        try:
            n, err = ingest()
            if err:
                sys.stderr.write("[ingest] 拉取失败: %s\n" % err)
        except Exception as e:
            sys.stderr.write("[ingest] 异常: %s\n" % e)
        # AGH 日志（真实客户端 IP）——失败只记一行，不影响 MosDNS 那条链路
        try:
            n, err = ingest_agh()
            if err:
                sys.stderr.write("[ingest_agh] %s\n" % err)
        except Exception as e:
            sys.stderr.write("[ingest_agh] 异常: %s\n" % e)
        now = time.time()
        if now >= next_min:
            try:
                sample_min()
                rollup_hour()
            except Exception as e:
                sys.stderr.write("[sample_min] 异常: %s\n" % e)
            next_min = now + SAMPLE_MIN
        if now >= next_hour:
            try:
                sample_hour()
            except Exception as e:
                sys.stderr.write("[sample_hour] 异常: %s\n" % e)
            next_hour = now + SAMPLE_HOUR
        if now >= next_prune:
            try:
                sys.stderr.write("[prune] %s\n" % prune())
            except Exception as e:
                sys.stderr.write("[prune] 异常: %s\n" % e)
            next_prune = now + PRUNE_INTERVAL
        time.sleep(INGEST_INTERVAL)


# ---------- 面板基线（只影响本面板显示，绝不动路由器上的 MosDNS） ----------
# 原理：点「清除」时把此刻的累计值记为基线，之后展示 当前值 - 基线值，
# 相当于从那一刻重新开始计数。MosDNS 里的真实统计完全不被修改。
BASELINE_FILE = os.path.join(HERE, "baseline.json")
_baseline = None
_b_lock = threading.Lock()

CUMULATIVE_KEYS = ("total_queries", "cached_queries", "blocked_queries",
                   "arbitrary_queries", "hosts_queries")
# 🔴 三个可独立清除的模块，各自带自己的基线时间戳。基线结构：
#    {"enabled": true, "mods": {
#        "top_domains": {"ts": "...", "data": {域名: 次数}},
#        "top_blocked": {"ts": "...", "data": {...}},
#        "stats":       {"ts": "...", "data": {...}}}}
#    某个模块没有基线 = 这个模块没被清除过，就显示完整累计值。
MOD_KEYS = ("top_domains", "top_blocked", "stats")


def _upgrade_baseline(b):
    """旧格式（扁平 {enabled, ts, stats, top_domains, top_blocked}）→ 新格式。
       旧格式是一次性全量基线，所以三个模块共用同一个 ts —— 升级后行为与以前一致。"""
    if not b or not b.get("enabled"):
        return {"enabled": False, "mods": {}}
    if "mods" in b:
        return b
    ts = b.get("ts")
    mods = {}
    for k in MOD_KEYS:
        v = b.get(k)
        if v:
            mods[k] = {"ts": ts, "data": v}
    return {"enabled": True, "mods": mods} if mods else {"enabled": False, "mods": {}}


def load_baseline():
    global _baseline
    with _b_lock:
        if _baseline is None:
            b = None
            if os.path.exists(BASELINE_FILE):
                try:
                    with open(BASELINE_FILE, "r", encoding="utf-8") as f:
                        b = json.load(f)
                except Exception:
                    b = None
            _baseline = _upgrade_baseline(b)
        return _baseline


def save_baseline(b):
    """保存基线。

    🔴 **这里是合并，不是整体覆盖**：make_baseline() 只带本次 scope 涉及的模块，
       其余模块的基线必须原样保留 —— 否则清一个榜会把另一个榜的起点也一起抹掉，
       就退回成「点哪都是全清」了（2026-10-03 用户明确指出各模块应相互独立）。"""
    global _baseline
    with _b_lock:
        mods = dict((_baseline or {}).get("mods") or {})
        if not b or not b.get("enabled"):
            merged = {"enabled": False, "mods": {}}
        else:
            for k in MOD_KEYS:
                if k in b:
                    mods[k] = {"ts": b["ts"], "data": b[k]}
            merged = {"enabled": True, "mods": mods}
        _baseline = merged
        try:
            with open(BASELINE_FILE, "w", encoding="utf-8") as f:
                json.dump(merged, f, ensure_ascii=False, indent=1)
        except Exception as e:
            sys.stderr.write("保存基线失败: %s\n" % e)


def make_baseline(data, scope="all"):
    """按 scope 取一份新基线。scope: all / top_domains / top_blocked / stats

    🔴 只带本次 scope 涉及的模块，save_baseline 那边会把它合并进已有基线，
       所以清「热门域名 TOP 50」不会动 top_blocked 和 stats（趋势图）的起点。"""
    ts = data.get("synced_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out = {"enabled": True, "ts": ts}
    if scope in ("all", "top_domains"):
        out["top_domains"] = {x["d"]: x["c"] for x in (data.get("top_domains") or [])}
    if scope in ("all", "top_blocked"):
        out["top_blocked"] = {x["d"]: x["c"] for x in (data.get("top_blocked") or [])}
    if scope in ("all", "stats"):
        out["stats"] = dict(data.get("stats") or {})
    return out


def diff_top(items, base, limit):
    out = []
    for x in items:
        c = (x.get("c") or 0) - (base.get(x.get("d")) or 0)
        if c > 0:
            out.append({"d": x["d"], "c": c})
    out.sort(key=lambda x: -x["c"])
    return out[:limit]


def apply_baseline(data, b):
    """把原始快照换算成「基线之后」的增量视图。

    🔴 **每个模块用自己的基线时间戳，互不干扰**：
       清「热门域名 TOP 50」只减 top_domains，趋势图和概览统计保持完整 ——
       旧实现是三模块共用一个 ts，还会按那个 ts 把 history 整片裁掉，
       结果点一下 TOP 榜的清除，查询趋势图就空了（2026-10-03 用户报的）。"""
    if not b or not b.get("enabled"):
        data["since"] = None
        return data
    mods = b.get("mods") or {}

    # --- 概览统计（total/cached/blocked...）---
    s = dict(data.get("stats") or {})
    mst = mods.get("stats")
    if mst:
        bs = mst.get("data") or {}
        for k in CUMULATIVE_KEYS:
            s[k] = max(0, (s.get(k) or 0) - (bs.get(k) or 0))
    tot = s.get("total_queries") or 0
    s["cached_percentage"] = round((s.get("cached_queries") or 0) / tot * 100, 2) if tot else 0
    s["blocked_percentage"] = round((s.get("blocked_queries") or 0) / tot * 100, 2) if tot else 0
    data["stats"] = s

    # --- 两个 TOP 榜：只有被清除过的才做差分 ---
    if mods.get("top_domains"):
        data["top_domains"] = diff_top(data.get("top_domains") or [],
                                       mods["top_domains"].get("data") or {}, TOP_SHOW)
    if mods.get("top_blocked"):
        data["top_blocked"] = diff_top(data.get("top_blocked") or [],
                                       mods["top_blocked"].get("data") or {}, TOP_SHOW)

    # --- 趋势图 history：只跟着 stats 的起点裁剪，不再被 TOP 榜的清除波及 ---
    cut = ((mst or {}).get("ts") or "")[:13]          # "YYYY-MM-DD HH"
    if cut:
        # 用 >= 保留基线所在的那个小时，否则当天趋势图会整片变空
        data["history"] = [p for p in (data.get("history") or [])
                           if ((p.get("time") or "")[:13].replace("T", " ")) >= cut]

    # since 分三份给前端：顶部「统计起点」用 stats 的，TOP 榜下面用各自的
    data["since"] = (mst or {}).get("ts") or None
    data["since_top"] = (mods.get("top_domains") or {}).get("ts") or None
    data["since_blk"] = (mods.get("top_blocked") or {}).get("ts") or None
    return data


def get_snapshot(force=False):
    now = time.time()
    with _lock:
        if not force and _cache["data"] and (now - _cache["ts"]) < CACHE_TTL:
            d = dict(_cache["data"])
            d["_cached"] = True
            d["_age_ms"] = int((now - _cache["ts"]) * 1000)
            return d
    data = collect()
    with _lock:
        _cache["data"] = data
        _cache["ts"] = time.time()
    return data


# ---------- 实时查询 ----------
def query(name):
    t0 = time.time()
    nm = re.sub(r"[^A-Za-z0-9._-]", "", (name or "").strip())[:路由器]
    if not nm:
        return {"ok": False, "error": "名称为空或不合法"}

    dig = ('dig @127.0.0.1 -p %d +noall +answer +time=3 +tries=1 %s A; '
           'dig @127.0.0.1 -p %d +noall +answer +time=3 +tries=1 %s AAAA'
           % (MOSDNS_PORT, nm, MOSDNS_PORT, nm))
    out, err = ssh(dig, 25)

    answers, seen = [], set()
    for line in (out or "").splitlines():
        line = line.strip()
        if not line or line.startswith(";"):
            continue
        p = line.split()
        if len(p) >= 5 and p[3] in ("A", "AAAA", "CNAME"):
            v = p[4].rstrip(".")
            k = (p[3], v)
            if k not in seen:
                seen.add(k)
                answers.append({"type": p[3], "value": v})

    # 从 MosDNS 日志里找这次查询的处理链路（A / AAAA 分别取最新一条）
    handling = {}
    logs = _jload_pull_logs()
    tgt = nm.lower().rstrip(".")
    for it in logs:
        d = (it.get("domain") or "").lower().rstrip(".")
        if d == tgt or d.endswith("." + tgt):
            q = it.get("qtype")
            if q and q not in handling:
                handling[q] = {
                    "status": it.get("status"),
                    "rule": it.get("rule"),
                    "upstream": it.get("upstream"),
                    "blocked": bool(it.get("is_blocked")),
                    "cached": bool(it.get("is_cached")),
                    "elapsed_ms": it.get("elapsed_ms", 0),
                    "qtype": q,
                }

    return {
        "ok": True,
        "name": nm,
        "answers": answers,
        "mosdns": handling.get("A") or handling.get("AAAA"),
        "handling": handling,
        "via": "%s:%d" % (MOSDNS_HOST, MOSDNS_PORT),
        "took_ms": int((time.time() - t0) * 1000),
        "error": err,
    }


def _jload_pull_logs():
    out, _ = ssh('curl -s --noproxy "*" -m 10 "%s/logs"' % STATS_API, 20)
    j = _jload(out)
    return (j or {}).get("items") or []


# ---------- 登录认证 ----------
# 说明：面板经 frp 暴露到了公网，所以默认要求登录。
# 凭据不落明文：auth.conf 里存 PBKDF2-SHA256 哈希 + 随机 salt，会话用 HMAC 签名的 cookie。
_auth_cache = {"mtime": 0, "cfg": None}
_fails = {}                 # 来源 IP -> [连续失败次数, 最近失败时间]
_fail_lock = threading.Lock()


def auth_cfg():
    """读 auth.conf；不存在 / AUTH_ENABLE != 1 / 字段不全 都视为「不启用认证」"""
    try:
        mt = os.path.getmtime(AUTH_CONF)
    except Exception:
        return None
    c = _auth_cache
    if c["mtime"] == mt and c["cfg"] is not None:
        return c["cfg"] or None
    cfg = {}
    try:
        with open(AUTH_CONF, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#") and "=" in s:
                    k, v = s.split("=", 1)
                    cfg[k.strip()] = v.strip()
    except Exception as e:
        sys.stderr.write("[auth] 读取 auth.conf 失败: %s\n" % e)
        cfg = {}
    ok = (cfg.get("AUTH_ENABLE") == "1" and cfg.get("AUTH_USER")
          and cfg.get("AUTH_HASH") and cfg.get("AUTH_SECRET"))
    _auth_cache["mtime"] = mt
    _auth_cache["cfg"] = cfg if ok else None
    return _auth_cache["cfg"]


def pw_hash(pw, rounds=PBKDF2_ROUNDS):
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"), salt, rounds)
    return "pbkdf2_sha256$%d$%s$%s" % (
        rounds, base64.b64encode(salt).decode(), base64.b64encode(dk).decode())


def pw_verify(pw, stored):
    try:
        algo, rounds, salt, want = str(stored).split("$")
        if algo != "pbkdf2_sha256":
            return False
        dk = hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"),
                                 base64.b64decode(salt), int(rounds))
        return hmac.compare_digest(base64.b64encode(dk).decode(), want)
    except Exception:
        return False


def _sess_sign(secret, user, exp):
    return hmac.new(secret.encode("utf-8"),
                    ("%s|%d" % (user, exp)).encode("utf-8"),
                    hashlib.sha256).hexdigest()[:32]


def sess_make(cfg, user):
    exp = int(time.time()) + SESS_DAYS * 86400
    return "%s|%d|%s" % (user, exp, _sess_sign(cfg["AUTH_SECRET"], user, exp))


def sess_check(cfg, val):
    try:
        user, exp, sig = str(val).split("|")
        exp = int(exp)
    except Exception:
        return None
    if exp < time.time():
        return None
    if not hmac.compare_digest(sig, _sess_sign(cfg["AUTH_SECRET"], user, exp)):
        return None
    return user


def is_lan_ip(ip):
    """私有 / 回环 / 链路本地地址"""
    s = (ip or "").strip().lower()
    if not s:
        return False
    if s.startswith("::ffff:"):
        s = s[7:]
    if s in ("::1", "127.0.0.1", "localhost", "0.0.0.0"):
        return True
    if s.startswith("192.168.") or s.startswith("10.") or s.startswith("169.254."):
        return True
    m = re.match(r"^172\.(\d+)\.", s)
    if m and 16 <= int(m.group(1)) <= 31:
        return True
    # IPv6 ULA / 链路本地
    return s.startswith("fc") or s.startswith("fd") or s.startswith("fe80")


def fail_wait(ip):
    """返回还需等待的秒数；0 表示可以继续尝试"""
    with _fail_lock:
        n, t = _fails.get(ip, (0, 0.0))
        if n >= FAIL_MAX and time.time() - t < FAIL_LOCK:
            return int(FAIL_LOCK - (time.time() - t))
        return 0


def fail_hit(ip):
    with _fail_lock:
        n, t = _fails.get(ip, (0, 0.0))
        if time.time() - t > FAIL_LOCK:
            n = 0
        _fails[ip] = (n + 1, time.time())


def fail_clear(ip):
    with _fail_lock:
        _fails.pop(ip, None)


def esc_html(s):
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


# ---------- HTTP ----------
class Handler(BaseHTTPRequestHandler):
    server_version = "MosDNSDashboard/1.0"

    # 🔴 HEAD 请求标记：True 时 `_send()` 和静态文件分支照常算 Content-Length、
    #    照常发响应头，但**不发 body**（RFC 要求 HEAD 的头与 GET 一致、只是没 body）。
    #    ⚠️ 实现 HEAD **不能**用"把 self.wfile 换成黑洞"那套 —— Python 3 的
    #    `end_headers()` 是把 `_headers_buffer` 写进 self.wfile 的，换掉 wfile
    #    连响应头一起吞了，客户端拿到的是 EOF（实测：curl 报 SSL unexpected eof、
    #    香港 nginx 直接回 502）。所以只能靠标记位。
    _head_only = False

    # ---- 认证相关的小工具 ----
    def _peer(self):
        """判断来源用的 IP：优先 X-Forwarded-For 最右边一项。

        外网链路是 浏览器 → 香港 nginx(8443) → frps → frpc → 本机 8088。
        nginx 配了 proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for，
        真实客户端 IP 会被**追加到最右边**，而客户端自己伪造的 XFF 只会堆在左边，
        所以取最右一项才是可信的。没有 XFF 时用 socket 远端地址（内网直连场景）。
        """
        xff = (self.headers.get("X-Forwarded-For") or "").strip()
        if xff:
            return xff.split(",")[-1].strip()
        return self.address_string() or "-"

    def _is_https(self):
        try:
            import ssl as _ssl
            return isinstance(self.connection, _ssl.SSLSocket)
        except Exception:
            return False

    # ---- 明文 http:// 打开 8088：直接 301 跳到 https:// ----
    def handle(self):
        """🔴 8088 只跑 HTTPS。用 http:// 打开时浏览器发的是明文，
        TLS 握手会直接失败 —— 用户看到的不是"跳一下"，而是连接错误。
        所以 MuxHTTPServer 在 accept 之后 peek 首字节分流：明文的连接会被打上
        _plain_http 标记，这里直接回 301，根本不进正常的请求处理流程。"""
        if getattr(self.connection, "plain_http", False):
            self._redirect_plain_http()
            return
        BaseHTTPRequestHandler.handle(self)

    def _redirect_plain_http(self):
        try:
            # 读请求头只为拿到 Host 和路径，好把用户跳回他本来的地址
            self.connection.settimeout(3.0)
            raw = b""
            while b"\r\n\r\n" not in raw and len(raw) < 8192:
                chunk = self.connection.recv(1024)
                if not chunk:
                    break
                raw += chunk
            lines = raw.decode("latin-1", "replace").split("\r\n")
            parts = (lines[0] if lines else "").split(" ")
            path = parts[1] if len(parts) > 1 else "/"
            host = ""
            for ln in lines[1:]:
                if ln.lower().startswith("host:"):
                    host = ln.split(":", 1)[1].strip()
                    break
            # Host 里没带端口就补上 8088，否则会被跳到 https 默认的 443
            if host and ":" not in host:
                host = "%s:%d" % (host, LISTEN_PORT)
            if not host:
                host = "%s:%d" % (LISTEN_HOST if LISTEN_HOST not in ("0.0.0.0", "", "::")
                                  else "127.0.0.1", LISTEN_PORT)
            self.connection.sendall((
                "HTTP/1.1 301 Moved Permanently\r\n"
                "Location: https://%s%s\r\n"
                "Content-Length: 0\r\n"
                "Cache-Control: no-store\r\n"
                "Connection: close\r\n\r\n" % (host, path)
            ).encode("latin-1"))
        except Exception:
            pass
        finally:
            try:
                self.connection.close()
            except Exception:
                pass

    def _sess_user(self):
        cfg = auth_cfg()
        if not cfg:
            return None
        for part in (self.headers.get("Cookie") or "").split(";"):
            k, _, v = part.partition("=")
            if k.strip() == SESS_COOKIE:
                return sess_check(cfg, v.strip())
        return None

    def _set_cookie(self, val):
        # HttpOnly 挡 JS 读取；SameSite=Lax 挡跨站带 cookie；HTTPS 下加 Secure
        c = "%s=%s; Path=/; HttpOnly; SameSite=Lax" % (SESS_COOKIE, val)
        if self._is_https():
            c += "; Secure"
        self.send_header("Set-Cookie", c)

    def _del_cookie(self):
        c = "%s=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0" % SESS_COOKIE
        if self._is_https():
            c += "; Secure"
        self.send_header("Set-Cookie", c)

    def _redirect(self, loc):
        self.send_response(302)
        self.send_header("Location", loc)
        self.send_header("Content-Length", "0")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()

    def _login_page(self, err="", code=200, user=""):
        try:
            with open(LOGIN_PAGE, "r", encoding="utf-8") as f:
                html = f.read()
        except Exception:
            html = ("<!doctype html><meta charset=utf-8><title>登入</title>"
                    "<form method=post action=/api/login>"
                    "<input name=username placeholder=用户名>"
                    "<input name=password type=password placeholder=密码>"
                    "<button>登入</button></form>")
        html = html.replace("<!--__ERR__-->",
                            '<div class="alert">%s</div>' % esc_html(err) if err else "")
        html = html.replace('value="__USER__"',
                            'value="%s"' % esc_html(user))   # 失败后回填用户名
        self._send(code, html, "text/html; charset=utf-8")

    def _guard(self):
        """统一入口检查。返回 True 表示已经发出响应，调用方直接 return"""
        cfg = auth_cfg()
        if not cfg:
            return False
        p = (urlparse(self.path).path.rstrip("/")) or "/"
        if p in ("/login.html", "/api/login") or p.startswith("/static/login"):
            return False
        if self._sess_user():
            return False
        # 内网免登录：只有明确判定为私有地址才放行
        if cfg.get("AUTH_LAN_FREE") == "1" and is_lan_ip(self._peer()):
            return False
        if p.startswith("/api/"):
            self._send(401, json.dumps({"ok": False, "error": "unauthorized",
                                        "login": "/login.html"}, ensure_ascii=False))
        else:
            self._redirect("/login.html")
        return True

    def _handle_login(self):
        cfg = auth_cfg()
        if not cfg:
            return self._redirect("/")
        ip = self._peer()
        w = fail_wait(ip)
        if w:
            return self._login_page("尝试过于频繁，请 %d 秒后再试" % w, 429)
        try:
            ln = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(ln).decode("utf-8", "replace") if ln else ""
        except Exception:
            raw = ""
        f = {}
        if "json" in (self.headers.get("Content-Type") or "").lower():
            try:
                f = json.loads(raw or "{}")
            except Exception:
                f = {}
        else:
            f = {k: v[0] for k, v in parse_qs(raw).items()}
        user = (f.get("username") or "").strip()
        pw = f.get("password") or ""
        ok = (hmac.compare_digest(user, cfg["AUTH_USER"])
              and pw_verify(pw, cfg["AUTH_HASH"]))
        if not ok:
            fail_hit(ip)
            sys.stderr.write("[auth] 登录失败 %s user=%r\n" % (ip, user))
            return self._login_page("用户名或密码错误", 401, user)
        fail_clear(ip)
        self.send_response(302)
        self._set_cookie(sess_make(cfg, user))
        self.send_header("Location", "/")
        self.send_header("Content-Length", "0")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        sys.stderr.write("[auth] 登录成功 %s user=%s\n" % (ip, user))

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        if isinstance(body, str):
            body = body.encode("utf-8")
        ae = (self.headers.get("Accept-Encoding") or "").lower()
        gz = False
        if "gzip" in ae and len(body) > 2048:
            try:
                body = gzip.compress(body, 6)
                gz = True
            except Exception:
                gz = False
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        if gz:
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self._head_only:
            return                       # HEAD：响应头已发完，body 不写
        try:
            self.wfile.write(body)
        except Exception:
            pass

    def _handle_clear(self, q):
        """重置面板统计起点：只改面板主机本地基线 + 本地日志库，路由器的 MosDNS 完全不动"""
        mode = q.get("mode", ["set"])[0]
        scope = (q.get("scope", ["all"])[0] or "all").strip()
        if scope not in ("all", "top_domains", "top_blocked", "stats"):
            scope = "all"
        try:
            if mode == "reset":                      # 恢复看全部累计
                save_baseline({"enabled": False})
                return self._send(200, json.dumps(
                    {"ok": True, "mode": "reset", "scope": "all", "since": None},
                    ensure_ascii=False))
            raw = get_snapshot(force=True)           # 原始累计值 -> 作为新基线
            b = make_baseline(raw, scope)
            save_baseline(b)                         # ← 内部按模块合并，不会抹掉别的模块
            # 日志明细是"查询"维度的东西，只跟着 stats 一起清；
            # 清 TOP 榜时不动它，否则上游服务器的统计（也是从 logs 聚合的）会跟着归零。
            if scope in ("all", "stats"):
                c = db()
                with _db_lock:
                    c.execute("DELETE FROM logs")
                    c.commit()
            with _lock:
                _cache["ts"] = 0.0                   # 让快照缓存立即失效
                _cache["data"] = None
            sys.stderr.write("[clear] scope=%s 起点重置为 %s\n" % (scope, b["ts"]))
            return self._send(200, json.dumps(
                {"ok": True, "mode": "set", "scope": scope, "since": b["ts"]},
                ensure_ascii=False))
        except Exception as e:
            return self._send(500, json.dumps({"ok": False, "error": str(e)},
                                              ensure_ascii=False))

    def _stream(self):
        """SSE：把这条连接挂成长连接，由 push_loop 往里灌数据。

        用 HTTP/1.1 + chunked 风格（这里不设 Content-Length，直接一直写），
        客户端断开时 write 抛异常，我们就退出循环并注销自己。"""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Accel-Buffering", "no")     # 让香港 nginx 别缓冲
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        q = queue.Queue(maxsize=8)
        with _sse_lock:
            _sse_clients.add(q)
        peer = self._peer()
        sys.stderr.write("[sse] +1 %s（当前 %d 个订阅）\n"
                         % (peer, len(_sse_clients)))
        try:
            # 先立刻推一帧，页面不用等
            self.wfile.write(("data: %s\n\n" % sse_payload(force=False)).encode("utf-8"))
            self.wfile.flush()
            while True:
                try:
                    msg = q.get(timeout=SSE_HEARTBEAT)
                    self.wfile.write(("data: %s\n\n" % msg).encode("utf-8"))
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")     # 心跳注释行
                self.wfile.flush()
        except Exception:
            pass                                        # 客户端关了页面
        finally:
            with _sse_lock:
                _sse_clients.discard(q)
                left = len(_sse_clients)
            sys.stderr.write("[sse] -1 %s（剩余 %d 个订阅）\n" % (peer, left))

    def do_POST(self):
        u = urlparse(self.path)
        p = u.path.rstrip("/") or "/"
        if p == "/api/login":
            return self._handle_login()
        if self._guard():
            return
        if p == "/api/clear":
            return self._handle_clear(parse_qs(u.query))
        if p == "/api/device_name":
            return self._handle_device_name()
        return self._send(404, json.dumps({"ok": False, "error": "not found"},
                                          ensure_ascii=False))

    def _handle_device_name(self):
        """给某个 IP 起个名字（写 devices.conf，只落在这台面板主机上）"""
        try:
            ln = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(ln).decode("utf-8") or "{}")
        except Exception:
            return self._send(400, json.dumps(
                {"ok": False, "error": "请求体不是合法 JSON"}, ensure_ascii=False))
        ip = str(body.get("ip") or "").strip()
        name = str(body.get("name") or "").strip()
        if not ip:
            return self._send(400, json.dumps(
                {"ok": False, "error": "缺少 ip"}, ensure_ascii=False))
        with _lock:
            try:
                lines = []
                if os.path.exists(DEV_CONF):
                    with open(DEV_CONF, "r", encoding="utf-8") as f:
                        lines = f.read().splitlines()
                out, hit = [], False
                for line in lines:
                    s = line.strip()
                    if s and not s.startswith("#") and "=" in s \
                            and s.split("=", 1)[0].strip() == ip:
                        if name:
                            out.append("%s=%s" % (ip, name))
                        hit = True
                    else:
                        out.append(line)
                if not hit and name:
                    out.append("%s=%s" % (ip, name))
                tmp = DEV_CONF + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    f.write("\n".join(out) + "\n")
                os.replace(tmp, DEV_CONF)
            except Exception as e:
                return self._send(500, json.dumps(
                    {"ok": False, "error": str(e)}, ensure_ascii=False))
        return self._send(200, json.dumps(
            {"ok": True, "ip": ip, "name": name,
             "devices": load_devices()}, ensure_ascii=False))

    def do_GET(self):
        u = urlparse(self.path)
        p = u.path.rstrip("/") or "/"

        # 已登录时访问登录页直接回主页
        if p == "/login.html" and auth_cfg() and self._sess_user():
            return self._redirect("/")
        if p == "/api/logout":
            self.send_response(302)
            self._del_cookie()
            self.send_header("Location", "/login.html")
            self.send_header("Content-Length", "0")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        if self._guard():
            return

        if p == "/login.html":
            return self._login_page()

        if p in ("/", "/index.html"):
            try:
                with open(INDEX, "r", encoding="utf-8") as f:
                    html = f.read()
            except Exception as e:
                return self._send(500, "无法读取 index.html: %s" % e,
                                  "text/plain; charset=utf-8")
            # 服务端直接把快照内联进 HTML：首屏必定有数据，
            # 即使浏览器的 fetch 因代理/预览面板等原因打不通也能正常显示。
            try:
                snap = apply_baseline(dict(get_snapshot()), load_baseline())
                html = html.replace(
                    "/*__MOSDNS_DATA__*/null",
                    json.dumps(snap, ensure_ascii=False, separators=(",", ":")))
            except Exception as e:
                sys.stderr.write("内联快照失败: %s\n" % e)
            return self._send(200, html, "text/html; charset=utf-8")

        if p.startswith("/static/"):
            # 只服务 static/ 下的图片，防止路径穿越
            rel = p[len("/static/"):]
            full = os.path.normpath(os.path.join(STATIC_DIR, rel))
            if (not full.startswith(STATIC_DIR + os.sep)
                    or not os.path.isfile(full)):
                return self._send(404, json.dumps({"ok": False, "error": "not found"},
                                                  ensure_ascii=False))
            ctype = STATIC_EXT.get(os.path.splitext(full)[1].lower())
            if not ctype:
                return self._send(403, json.dumps({"ok": False, "error": "forbidden"},
                                                  ensure_ascii=False))
            try:
                with open(full, "rb") as f:
                    body = f.read()
            except Exception as e:
                return self._send(500, str(e), "text/plain; charset=utf-8")
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "public, max-age=86400")
            self.end_headers()
            if not self._head_only:
                self.wfile.write(body)
            return

        if p == "/api/snapshot":
            q = parse_qs(u.query)
            force = q.get("force", [""])[0] == "1"
            try:
                n = int(q.get("logs", [str(LOG_DEFAULT)])[0])
                n = max(1, min(LOG_KEEP, n))
            except Exception:
                n = LOG_DEFAULT
            try:
                d = apply_baseline(dict(get_snapshot(force)), load_baseline())
                if n != LOG_DEFAULT:
                    d["logs"], d["log_total"] = read_logs(n)
                return self._send(200, json.dumps(d, ensure_ascii=False))
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)},
                                                  ensure_ascii=False))

        if p == "/api/clear":
            return self._handle_clear(parse_qs(u.query))

        if p == "/api/logs":
            try:
                n = int(parse_qs(u.query).get("n", [str(LOG_KEEP)])[0])
                n = max(1, min(LOG_KEEP, n))
            except Exception:
                n = LOG_KEEP
            try:
                logs, total = read_logs(n)
                return self._send(200, json.dumps(
                    {"ok": True, "logs": logs, "total": total, "returned": len(logs)},
                    ensure_ascii=False))
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)},
                                                  ensure_ascii=False))

        if p == "/api/stream":
            return self._stream()

        if p == "/api/alog":
            """真实客户端查询日志（来自 AGH，含客户端 IP / MAC）"""
            q = parse_qs(u.query)
            try:
                n = int(q.get("n", [str(LOG_DEFAULT)])[0])
                n = max(1, min(LOG_KEEP, n))
            except Exception:
                n = LOG_DEFAULT
            try:
                logs, total = read_alogs(n)
                return self._send(200, json.dumps(
                    {"ok": True, "logs": logs, "total": total,
                     "clients": read_client_top(30),
                     "agh_ok": bool(_agh.get("ok")), "agh_err": _agh.get("err") or ""},
                    ensure_ascii=False))
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)},
                                                  ensure_ascii=False))

        if p == "/api/ikuai":
            """爱快对接自检：强制重新登录并拉一次 DHCP 租约表"""
            try:
                names = ik_names(force=True)
                return self._send(200, json.dumps(
                    {"ok": bool(_ik.get("ok")), "err": _ik.get("err") or "",
                     "url": ik_conf().get("IKUAI_URL", ""),
                     "user": ik_conf().get("IKUAI_USER", ""),
                     "names": names}, ensure_ascii=False))
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)},
                                                  ensure_ascii=False))

        if p == "/api/clients":
            """按设备汇总的查询量（IP / MAC / 设备名）"""
            try:
                return self._send(200, json.dumps(
                    {"ok": True, "clients": read_client_top(60),
                     "macs": read_macs()}, ensure_ascii=False))
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)},
                                                  ensure_ascii=False))


        if p == "/api/query":
            name = parse_qs(u.query).get("name", [""])[0]
            if not name.strip():
                return self._send(400, json.dumps({"ok": False, "error": "缺少 name 参数"},
                                                  ensure_ascii=False))
            try:
                return self._send(200, json.dumps(query(name), ensure_ascii=False))
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)},
                                                  ensure_ascii=False))

        if p == "/api/history":
            q = parse_qs(u.query)
            rng = q.get("range", ["today"])[0]
            if rng not in ("today", "7d", "30d", "all"):
                rng = "today"
            try:
                h = read_history(rng)
                h["range"] = rng
                if rng == "all":
                    # 自建库以来的永久累计
                    h["top_domains"] = read_domain_total(TOP_SHOW, blocked=False)
                    h["top_blocked"] = read_domain_total(TOP_SHOW, blocked=True)
                else:
                    h["top_domains"] = read_top_range(rng, "domain", TOP_SHOW)
                    h["top_blocked"] = read_top_range(rng, "blocked", TOP_SHOW)
                return self._send(200, json.dumps(h, ensure_ascii=False))
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)},
                                                  ensure_ascii=False))

        if p == "/api/domains":
            """永久累计榜：自建库以来每个域名被查询 / 被拦截的总次数"""
            q = parse_qs(u.query)
            kind = q.get("kind", ["domain"])[0]
            try:
                n = int(q.get("n", [str(TOP_SHOW)])[0])
                n = max(1, min(200, n))
            except Exception:
                n = TOP_SHOW
            try:
                return self._send(200, json.dumps({
                    "ok": True, "kind": kind,
                    "items": read_domain_total(n, blocked=(kind == "blocked")),
                }, ensure_ascii=False))
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)},
                                                  ensure_ascii=False))

        if p == "/health":
            conn = db()
            try:
                with _db_lock:
                    n_logs = conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
                    n_alogs = conn.execute("SELECT COUNT(*) FROM alogs").fetchone()[0]
                    n_min = conn.execute("SELECT COUNT(*) FROM stats_min").fetchone()[0]
                    n_hour = conn.execute("SELECT COUNT(*) FROM stats_hour").fetchone()[0]
                    n_top = conn.execute("SELECT COUNT(*) FROM top_hour").fetchone()[0]
                    n_dom = conn.execute("SELECT COUNT(*) FROM domain_total").fetchone()[0]
            except Exception:
                n_logs = n_alogs = n_min = n_hour = n_top = n_dom = -1
            return self._send(200, json.dumps({
                "ok": True,
                "uptime_s": int(time.time() - START_TS),
                "source": MOSDNS_HOST,
                "agh": {"ok": bool(_agh.get("ok")), "err": _agh.get("err") or "",
                        "url": agh_conf().get("AGH_URL", "")},
                "ikuai": {"ok": bool(_ik.get("ok")), "err": _ik.get("err") or "",
                          "url": ik_conf().get("IKUAI_URL", ""),
                          "names": len(_ik.get("names") or {})},
                "db": {"logs": n_logs, "alogs": n_alogs, "stats_min": n_min,
                       "stats_hour": n_hour, "top_hour": n_top,
                       "domain_total": n_dom},
                "keep": {"logs": LOG_KEEP, "min_days": KEEP_MIN_DAYS,
                         "hour_days": KEEP_HOUR_DAYS},
            }, ensure_ascii=False))

        return self._send(404, json.dumps({"ok": False, "error": "not found"},
                                          ensure_ascii=False))

    # HEAD 按 RFC 必须是「安全且无副作用」的。这三个端点要么会挂住线程、
    # 要么会真的改数据，所以 HEAD 直接拒掉，不去跑 do_GET。
    HEAD_BLOCK = {
        "/api/stream",   # SSE 长连接，一路写下去要等客户端断开才结束 → 线程被占死
        "/api/clear",    # 会真的重置统计基线并 DELETE FROM logs
        "/api/logout",   # 会真的把 cookie 注销掉
    }

    def do_HEAD(self):
        """HEAD：只回响应头、不带 body。

        之前没实现这个方法 → 服务端回 501，被一些默认用 HEAD 探活的工具判成"站点挂了"。
        实现 = 置 `_head_only` 标记 + 借 do_GET 跑一遍完整路由（这样 Content-Type /
        Content-Length 和 GET 完全一致，也不用维护第二套路由判断），
        再额外挡掉 HEAD_BLOCK 里的三个端点。
        """
        p = urlparse(self.path).path.rstrip("/") or "/"
        if p in self.HEAD_BLOCK:
            self.send_response(405)
            self.send_header("Allow", "GET")
            self.send_header("Content-Length", "0")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        self._head_only = True
        try:
            self.do_GET()
        finally:
            self._head_only = False

    def log_message(self, fmt, *args):
        # 记录来源判定：socket 远端 + XFF 原文，方便核对「内网免登录」有没有被外网绕过
        extra = ""
        if auth_cfg():
            xff = (self.headers.get("X-Forwarded-For") or "").strip()
            extra = " [peer=%s%s]" % (self._peer(),
                                      (" xff=%s" % xff) if xff else "")
        sys.stderr.write("[%s] %s%s %s\n" % (
            datetime.now().strftime("%H:%M:%S"), self.address_string(),
            extra, fmt % args))


# ---------- 8088 同端口同时收 https / http ----------
# 思路和香港 nginx 的 ssl_preread 一样：accept 之后**只 peek 一个字节**，
# TLS 握手的第一个记录一定是 0x16(Handshake)，是就 wrap 成 TLS，
# 不是就当成明文 HTTP、打标记交给 Handler 回 301。
# ⚠️ 不能像以前那样把整个监听 socket 包成 TLS（srv.socket = ctx.wrap_socket），
#    那样明文请求一进来就是握手失败，用户看到的是连接错误而不是跳转。
class _PlainSocket(socket.socket):
    """🔴 socket.socket 不允许挂自定义属性（底层是 __slots__，没有 __dict__，
       `sock._plain_http = True` 会直接 AttributeError 把进程搞崩）。
       明文连接需要带个标记给 Handler 认，所以包一层自己的子类 —— Python
       子类不声明 __slots__ 就有 __dict__，可以随便挂属性。"""
    plain_http = False


class MuxHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    ssl_ctx = None          # 由 main() 在证书存在时填上

    def get_request(self):
        sock, addr = self.socket.accept()
        first = b""
        try:
            # 只 peek 不读走；加超时是防"连上但不发数据"的客户端把 accept 循环卡死
            sock.settimeout(2.0)
            first = sock.recv(1, socket.MSG_PEEK)
        except Exception:
            self._close(sock)
            raise
        finally:
            try:
                sock.settimeout(None)
            except Exception:
                pass
        if first and first[0] == 0x16 and self.ssl_ctx is not None:
            try:
                return self.ssl_ctx.wrap_socket(sock, server_side=True), addr
            except Exception:
                self._close(sock)
                raise
        # 明文：换成能挂标记的子类（detach 把 fd 交给它，原对象不再拥有这个 fd）
        try:
            # 先取再 detach —— detach 之后原对象的 family/type/proto 不保证还能读
            fam, typ, pro = sock.family, sock.type, sock.proto
            plain = _PlainSocket(fam, typ, pro, fileno=sock.detach())
            plain.plain_http = True
            return plain, addr
        except Exception:
            return sock, addr

    @staticmethod
    def _close(sock):
        try:
            sock.close()
        except Exception:
            pass


def main():
    # 先攒一批日志，再启动 HTTP；后台线程持续累积
    try:
        n, err = ingest()
        sys.stderr.write("首次日志采集 %d 条%s\n" % (n, ("（%s）" % err) if err else ""))
    except Exception as e:
        sys.stderr.write("首次日志采集失败: %s\n" % e)
    threading.Thread(target=collector_loop, daemon=True).start()
    threading.Thread(target=push_loop, daemon=True).start()

    srv = MuxHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    if os.path.exists(TLS_CERT) and os.path.exists(TLS_KEY):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(TLS_CERT, TLS_KEY)
        # 🔴 不是包整个监听 socket，而是给 MuxHTTPServer 让它按连接判协议
        MuxHTTPServer.ssl_ctx = ctx
        sys.stderr.write("已启用 HTTPS(TLS>=1.2)，证书 %s；明文 http 请求将 301 跳到 https\n"
                         % TLS_CERT)
    else:
        sys.stderr.write("未找到 TLS 证书，回退为明文 HTTP\n")
    sys.stderr.write("MosDNS Dashboard 监听 %s:%d  数据源 %s  日志保留 %d 条\n"
                     % (LISTEN_HOST, LISTEN_PORT, MOSDNS_HOST, LOG_KEEP))
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
