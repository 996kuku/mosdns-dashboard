#!/bin/bash
# 幂等地给 /opt/mosdns-dashboard/index.html 的 favSrcs 加上"本地 logo 优先"
# 由 systemd 的 ExecStartPre=- 调用（"-" = 失败也不阻止服务启动）
set -u
H=/opt/mosdns-dashboard/index.html
DIR=/opt/mosdns-dashboard/static/logo

[ -f "$H" ] || { echo "[logo] index.html 不存在，跳过"; exit 0; }
grep -q "/static/logo/" "$H" && { echo "[logo] 已启用本地 logo 优先，跳过"; exit 0; }
[ -d "$DIR" ] || { echo "[logo] $DIR 不存在，跳过（先跑 fetch-dashboard-logos.py）"; exit 0; }

cp -a "$H" "$H.bak.logo-$(date +%Y%m%d-%H%M%S)"

python3 - "$H" <<'PYEOF'
import sys
p = sys.argv[1]
s = open(p, encoding="utf-8").read()
anchor = "'https://' + d + '/favicon.ico',"
if anchor not in s:
    sys.stderr.write("[logo] 找不到 favSrcs 锚点，未修改\n")
    sys.exit(0)
ins = "\n".join([
    "    '/static/logo/' + d + '.png',",
    "    '/static/logo/' + d + '.ico',",
    "    '/static/logo/' + d + '.jpg',",
    "    '/static/logo/' + d + '.svg',",
])
s = s.replace(anchor, ins + "\n" + anchor, 1)
# 顺手更新注释
s = s.replace(
    "/* 网站图标：多源回退（站点自身 -> Google s2 -> DuckDuckGo -> favicon.im -> 地球占位） */",
    "/* 网站图标：本地 static/logo 优先 -> 站点自身 -> Google s2 -> DuckDuckGo -> favicon.im -> 地球占位 */",
    1)
open(p, "w", encoding="utf-8").write(s)
sys.stderr.write("[logo] favSrcs 已加入本地 logo 优先（4 个扩展名）\n")
PYEOF
exit 0
