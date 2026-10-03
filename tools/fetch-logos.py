#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MosDNS 仪表盘 —— 网站图标本地化采集器
--------------------------------------
把仪表盘采集到的域名的网站图标抓到
/opt/mosdns-dashboard/static/logo/<域名>.<ext>，
页面即可用本地 /static/logo/<域名>.png 直接调用，不必每次去第三方拉。

设计要点
  * 独立脚本，不改 server.py；采集器没覆盖到的域名，页面自动回退远程多源，不会坏。
  * 降域去重：榜单里大量是 CDN 子域（i0.hdslb.com / api.live.bilibili.com），
    本身没有 favicon。一律降到主域名（bilibili.com）抓取，再分发给它的所有子域。
  * 抓取顺序（命中即停）：
      1. https://<主域名>/favicon.ico  和 www 版
      2. 首页 HTML 里的 <link rel=...icon href=...>（很多站点没有 /favicon.ico）
      3. Google s2 / DuckDuckGo —— 这两个对未知域名会 404，安全
    ⚠ 不用 icon.horse：实测它对**不存在的域名**也返回 200，且是"按域名生成的
      字母占位图"（每个域名图都不一样，无法用 hash 过滤），会把假图标当真。
  * 国内直连优先，国外走 v2raya 代理（默认 http://127.0.0.1:20171）。
  * 幂等：已存在的域名默认跳过。

关键字兜底（第二阶段）
  像 67.ucp-ntfy.kaspersky-labs.com 这种，主域名 kaspersky-labs.com 是纯遥测域、
  根本没有网站。按顺序找"捐赠者"：
      1. ALIAS 手工映射（文本上无关联的 CDN：hdslb -> bilibili.com 等）
      2. 品牌关键字子串匹配：kaspersky-labs ⊇ kaspersky，复用已有图标
      3. 按关键字猜官网：kaspersky-labs -> kaspersky.com（只猜 .com，
         避免 .cn/.net 抢注站给错图标）

用法
  fetch-dashboard-logos.py                # 采集 TOP150
  fetch-dashboard-logos.py -n 300         # 采集 TOP300
  fetch-dashboard-logos.py --force        # 强制重抓
  fetch-dashboard-logos.py --no-proxy     # 只用直连
  fetch-dashboard-logos.py --no-brand     # 跳过第二阶段关键字兜底
"""
import argparse
import hashlib
import os
import re
import shutil
import socket
import sqlite3
import ssl
import sys
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

HERE = "/opt/mosdns-dashboard"
DB = os.path.join(HERE, "logs.db")
OUT = os.path.join(HERE, "static", "logo")
DEFAULT_PROXY = "http://127.0.0.1:20171"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
TIMEOUT = 4
MAX_BYTES = 512 * 1024
MIN_BYTES = 100
ALLOWED = (".png", ".ico", ".jpg", ".svg")   # server.py 静态路由支持的扩展名
socket.setdefaulttimeout(TIMEOUT)

# 需要取三段的复合后缀
MULTI = {"com.cn", "net.cn", "org.cn", "gov.cn", "edu.cn", "ac.cn",
         "com.hk", "net.hk", "org.hk", "co.jp", "ne.jp", "or.jp",
         "com.tw", "net.tw", "org.tw", "com.br", "co.uk", "org.uk",
         "com.au", "co.nz", "com.sg", "co.kr", "com.mo"}

# ---------------------------------------------------------------------------
# 手工映射：主域名与其品牌在文字上毫无关联，机器猜不出来，只能列出来。
# 左侧可以是完整主域名或品牌关键字，右侧是"捐赠者"官网。可自行增删。
# ---------------------------------------------------------------------------
ALIAS = {
    "hdslb": "bilibili.com",          # bilibili 视频 CDN
    "bilivideo": "bilibili.com",      # bilibili 视频 CDN
    "gtimg": "qq.com",                # 腾讯图片 CDN
    "myqcloud": "tencent.com",        # 腾讯云
    "tencent-cloud": "tencent.com",
    "alicdn": "alibaba.com",          # 阿里 CDN
    "ksyuncdn": "ksyun.com",          # 金山云 CDN
    "windowsupdate": "microsoft.com",
    "gstatic": "google.com",
    "googleapis": "google.com",
    "kaspersky-labs": "kaspersky.com",
    "netease": "163.com",             # 网易官网是 163.com
    "dns.google": "google.com",
}

# 通用图标源对"不存在的域名"返回的占位图指纹（运行时自校准，见 calibrate()）
PLACEHOLDER = set()
FAKE_DOMAIN = "zzqq-nonexist-9x7.com"
GENERIC_SRCS = (
    "https://www.google.com/s2/favicons?domain=%s&sz=128",
    "https://icons.duckduckgo.com/ip3/%s.ico",
)

LINK_RE = re.compile(rb'<link[^>]+rel=["\']?[^"\'>]*icon[^"\'>]*["\']?[^>]*>', re.I)
HREF_RE = re.compile(rb'href=["\']([^"\']+)["\']', re.I)


# --------------------------------------------------------------------------
# 域名处理
# --------------------------------------------------------------------------
def reg_domain(d):
    """取主域名（eTLD+1 近似）：api.live.bilibili.com -> bilibili.com"""
    d = (d or "").lower().strip().rstrip(".")
    parts = [p for p in d.split(".") if p]
    if len(parts) <= 2:
        return ".".join(parts)
    if ".".join(parts[-2:]) in MULTI and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def is_domain(d):
    """过滤脏数据（表里混进过 '5.5' 这种）"""
    if not d or len(d) > 253:
        return False
    parts = [p for p in d.lower().strip().rstrip(".").split(".") if p]
    if len(parts) < 2 or any(len(p) > 63 for p in parts):
        return False
    if not re.fullmatch(r"[a-z]{2,24}", parts[-1]):
        return False
    if parts[-1] in ("arpa", "local", "invalid", "test", "internal", "home"):
        return False
    return bool(re.fullmatch(r"[a-z0-9.\-]+", ".".join(parts)))


def safe_name(d):
    return re.sub(r"[^a-z0-9.\-]", "_", d.lower().strip().rstrip("."))


def brand_of(domain):
    """主域名去掉 TLD：kaspersky-labs.com -> kaspersky-labs ; vivo.com.cn -> vivo"""
    root = reg_domain(domain)
    parts = root.split(".")
    if len(parts) < 2:
        return ""
    if ".".join(parts[-2:]) in MULTI and len(parts) >= 3:
        return ".".join(parts[:-2])
    return ".".join(parts[:-1])


def brand_candidates(root):
    """品牌关键字候选（从最具体到最宽）：
       kaspersky-labs.com -> [kaspersky-labs, kaspersky]
       dns.google         -> [dns, google]"""
    parts = reg_domain(root).split(".")
    if len(parts) < 2:
        return []
    core = parts[:-2] if (".".join(parts[-2:]) in MULTI and len(parts) >= 3) else parts[:-1]
    cands = [".".join(core)] + list(core)
    if len(parts) == 2:            # dns.google：两段都可能是品牌
        cands.append(parts[-1])
    for c in list(cands):          # kaspersky-labs -> kaspersky
        first = c.split("-")[0].split("_")[0]
        if first != c:
            cands.append(first)
    seen, out = set(), []
    for c in cands:
        if c and len(c) >= 3 and c not in seen:
            seen.add(c)
            out.append(c)
    return out


def guess_domains(brand):
    """按关键字猜官网。只猜 .com —— .cn/.net 极易撞上抢注站给错图标"""
    if not brand:
        return []
    key = brand.split("-")[0].split("_")[0]
    if len(key) < 4 or key.isdigit():
        return []
    return ["%s.com" % key]


# --------------------------------------------------------------------------
# 文件读写
# --------------------------------------------------------------------------
def existing(safe):
    for e in ALLOWED:
        p = os.path.join(OUT, safe + e)
        if os.path.isfile(p) and os.path.getsize(p) >= MIN_BYTES:
            return p
    return None


def save_icon(name, ext, data):
    """写入图标，并清掉同名的其它扩展名旧文件（防止旧的错误图标被优先命中）"""
    for e in ALLOWED:
        p = os.path.join(OUT, name + e)
        if e != ext and os.path.exists(p):
            os.remove(p)
    with open(os.path.join(OUT, name + ext), "wb") as f:
        f.write(data)


def ext_of(data):
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if data[:3] == b"\xff\xd8\xff":
        return ".jpg"
    if data[:4] == b"\x00\x00\x01\x00":
        return ".ico"
    head = data[:200].lstrip()
    if head[:5] == b"<?xml" or head[:4] == b"<svg":
        return ".svg"
    return None


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------
def make_opener(proxy=None):
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    handlers = [urllib.request.HTTPSHandler(context=ctx)]
    if proxy:
        handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
    else:
        handlers.append(urllib.request.ProxyHandler({}))   # 直连
    return urllib.request.build_opener(*handlers)


def grab(opener, url):
    req = urllib.request.Request(
        url, headers={"User-Agent": UA, "Accept": "image/*,*/*;q=0.8"})
    with opener.open(req, timeout=TIMEOUT) as r:
        data = r.read(MAX_BYTES)
    if len(data) < MIN_BYTES:
        return None
    if hashlib.md5(data).hexdigest() in PLACEHOLDER:
        return None
    e = ext_of(data)
    return (e, data) if e in ALLOWED else None


def calibrate(direct, proxied):
    """自校准：看通用源对"不存在的域名"返回什么，记进 PLACEHOLDER 黑名单
       （Google s2 / DuckDuckGo 目前是 404，这里只是留个保险）"""
    for op in (direct, proxied):
        if op is None:
            continue
        for tpl in GENERIC_SRCS:
            try:
                got = grab(op, tpl % FAKE_DOMAIN)
            except Exception:
                continue
            if got:
                PLACEHOLDER.add(hashlib.md5(got[1]).hexdigest())


def icon_from_homepage(op, root):
    """解析首页 <link rel="icon|apple-touch-icon" href=...> —— 很多站点没有 /favicon.ico"""
    for page in ("https://%s/" % root, "https://www.%s/" % root):
        try:
            req = urllib.request.Request(
                page, headers={"User-Agent": UA, "Accept": "text/html,*/*"})
            with op.open(req, timeout=TIMEOUT) as r:
                ctype = r.headers.get("Content-Type", "")
                html = r.read(300000)
        except Exception:
            continue
        if b"<" not in html[:400] and "html" not in ctype.lower():
            continue
        for tag in LINK_RE.findall(html)[:6]:
            m = HREF_RE.search(tag)
            if not m:
                continue
            href = m.group(1).decode("utf8", "ignore").strip()
            if href.lower().startswith("data:"):
                continue
            url = urljoin(page, href)
            if not url.lower().startswith(("http://", "https://")):
                continue
            try:
                got = grab(op, url)
            except Exception:
                continue
            if got:
                return got, url.split("/")[2]
    return None, None


def fetch_root(root, direct, proxied):
    """抓主域名图标，返回 ((ext, data), 来源) 或 (None, None)"""
    for op in (direct, proxied):
        if op is None:
            continue
        # 1) 站点自己的 /favicon.ico
        for url in ("https://%s/favicon.ico" % root,
                    "https://www.%s/favicon.ico" % root):
            try:
                got = grab(op, url)
            except Exception:
                continue
            if got:
                return got, url.split("/")[2]
        # 2) 首页 link rel=icon
        got, host = icon_from_homepage(op, root)
        if got:
            return got, host + " <link>"
        # 3) 通用源（未知域名会 404，安全）
        for tpl in GENERIC_SRCS:
            try:
                got = grab(op, tpl % root)
            except Exception:
                continue
            if got:
                return got, tpl.split("/")[2]
    return None, None


# --------------------------------------------------------------------------
# 品牌库
# --------------------------------------------------------------------------
def build_brand_index():
    """扫描已有 logo -> {品牌关键字: 文件路径}，只认主域名文件，避免子域噪音"""
    idx = {}
    if not os.path.isdir(OUT):
        return idx
    for fn in os.listdir(OUT):
        p = os.path.join(OUT, fn)
        if not os.path.isfile(p) or os.path.getsize(p) < MIN_BYTES:
            continue
        base, ext = os.path.splitext(fn)
        if ext.lower() not in ALLOWED:
            continue
        if reg_domain(base) != base:          # 子域文件，跳过
            continue
        b = brand_of(base)
        if b and len(b) >= 4:
            idx[b] = p
    return idx


def match_brand(brand, idx):
    """已有品牌里找 brand 的子串，取最长者：kaspersky-labs -> kaspersky"""
    if not brand:
        return None
    best = None
    for b in idx:
        if b == brand or b not in brand:
            continue
        if best is None or len(b) > len(best):
            best = b
    return best


def donor_file(gd, direct, proxied):
    """拿到捐赠域名的图标文件：已有就用，没有就现抓。返回 (路径, 说明)"""
    p = existing(safe_name(gd))
    if p:
        return p, "已有 %s" % gd
    got, host = fetch_root(gd, direct, proxied)
    if not got:
        return None, ""
    ext, data = got
    p = os.path.join(OUT, safe_name(gd) + ext)
    save_icon(safe_name(gd), ext, data)
    return p, "现抓 %s <- %s" % (gd, host)


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--limit", type=int, default=150)
    ap.add_argument("-j", "--jobs", type=int, default=10)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--no-proxy", action="store_true")
    ap.add_argument("--no-brand", action="store_true",
                    help="跳过第二阶段关键字兜底")
    ap.add_argument("--proxy", default=DEFAULT_PROXY)
    a = ap.parse_args()

    os.makedirs(OUT, exist_ok=True)

    con = sqlite3.connect("file:%s?mode=ro" % DB, uri=True)
    rows = con.execute(
        "SELECT d FROM domain_total ORDER BY c DESC LIMIT ?", (a.limit,)).fetchall()
    con.close()
    domains = [r[0] for r in rows if r[0]]
    junk = [d for d in domains if not is_domain(d)]
    domains = [d for d in domains if is_domain(d)]
    if not domains:
        print("没取到域名")
        return 1

    groups = defaultdict(list)
    for d in domains:
        groups[reg_domain(d)].append(d)
    print("域名 %d 个 -> 主域名 %d 个（降域去重）%s\n"
          % (len(domains), len(groups),
             "；过滤脏数据 %d 个：%s" % (len(junk), ",".join(junk[:5])) if junk else ""))

    direct = make_opener(None)
    proxied = None if a.no_proxy else make_opener(a.proxy)
    calibrate(direct, proxied)
    if PLACEHOLDER:
        print("占位图黑名单：%d 个（通用源对不存在域名的返回）\n" % len(PLACEHOLDER))

    def work(item):
        root, subs = item
        rs = safe_name(root)
        if not a.force and all(existing(safe_name(s)) for s in subs):
            # 子域都有图标，但主域名文件可能缺失（早期版本只存了子域），顺手补齐
            if not existing(rs):
                src = existing(safe_name(subs[0]))
                if src:
                    shutil.copy(src, os.path.join(OUT, rs + os.path.splitext(src)[1]))
            return (root, "skip", len(subs), "")
        got, host = fetch_root(root, direct, proxied)
        if not got:
            return (root, "fail", len(subs), "")
        ext, data = got
        save_icon(rs, ext, data)                       # 主域名自己存一份
        for s in subs:
            if s != root:
                save_icon(safe_name(s), ext, data)     # 分发给所有子域
        return (root, "ok", len(subs), "%s %dB <- %s" % (ext, len(data), host))

    n_ok = n_skip = 0
    failed = []
    with ThreadPoolExecutor(max_workers=a.jobs) as ex:
        for root, st, cnt, msg in ex.map(work, list(groups.items())):
            if st == "ok":
                n_ok += 1
                print("  [OK]   %-30s 覆盖 %-3d 个子域  %s" % (root, cnt, msg))
            elif st == "skip":
                n_skip += 1
            else:
                failed.append(root)

    # ------------------------------------------------------------------
    # 第二阶段：关键字兜底
    # kaspersky-labs.com 这种纯遥测域没有网站，但同品牌 kaspersky.com 有图标。
    # 顺序：手工映射 -> 品牌关键字子串 -> 按关键字猜官网（只猜 .com）
    # ------------------------------------------------------------------
    rescued = []
    if failed and not a.no_brand:
        idx = build_brand_index()
        print("\n第二阶段：关键字兜底（待救 %d 个，品牌库 %d 个）\n"
              % (len(failed), len(idx)))
        for root in list(failed):
            cands = []                                  # [(kind, value)]
            for key in (root, brand_of(root)):          # 1) 手工映射
                if key in ALIAS:
                    cands.append(("domain", ALIAS[key]))
            for b in brand_candidates(root):            # 2) 关键字子串
                hit = match_brand(b, idx)
                if hit:
                    cands.append(("file", idx[hit]))
            for b in brand_candidates(root):            # 3) 猜官网
                for gd in guess_domains(b):
                    cands.append(("domain", gd))

            src, how = None, ""
            for kind, val in cands:
                if kind == "file":
                    src, how = val, "关键字复用 %s" % os.path.basename(val)
                else:
                    p, note = donor_file(val, direct, proxied)
                    if p:
                        src, how = p, "同源 %s（%s）" % (val, note)
                if src:
                    break
            if not src:
                continue

            ext = os.path.splitext(src)[1]
            with open(src, "rb") as f:
                data = f.read()
            # 主域名自己可能不在榜单里（榜单只有它的 CDN 子域），也要存一份
            subs = sorted(set((groups.get(root) or []) + [root]))
            for s in subs:
                save_icon(safe_name(s), ext, data)
            rescued.append((root, len(subs), how))
            failed.remove(root)

        for root, cnt, how in rescued:
            print("  [KEY]  %-30s 覆盖 %-3d 个子域  %s" % (root, cnt, how))
        print("\n关键字兜底救回 %d 个，仍失败 %d 个" % (len(rescued), len(failed)))

    print("\n主域名：新增 %d / 已有 %d / 失败 %d" % (n_ok, n_skip, len(failed)))
    print("logo 目录文件总数：%d" % len(os.listdir(OUT)))
    if failed:
        print("失败（页面自动回退远程源或显示 🌐）：" + ", ".join(failed[:30]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
