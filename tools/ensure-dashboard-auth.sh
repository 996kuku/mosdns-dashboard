#!/bin/bash
# 登录认证自检：只报警，不改写代码
# 为什么需要：server.py 会被反复重新部署，一旦新版本没带认证模块，
#             面板就会在公网上裸奔。认证必须写在 server.py 里才能拦请求，
#             这里改不了代码，只能把风险喊进系统日志。
set -u
P=/opt/mosdns-dashboard/server.py
A=/opt/mosdns-dashboard/auth.conf
TAG=mosdns-dashboard

[ -f "$P" ] || { echo "[auth] server.py 不存在，跳过"; exit 0; }

if [ ! -f "$A" ]; then
  echo "[auth] 没有 auth.conf，面板对外公开（需要登录就跑 dashboard-setpass）"
  logger -t "$TAG" "[auth] 没有 auth.conf，面板对外公开"
  exit 0
fi

if ! grep -q '^AUTH_ENABLE=1' "$A"; then
  echo "[auth] auth.conf 未启用认证，面板对外公开"
  exit 0
fi

if ! grep -q 'AUTH_CONF' "$P"; then
  MSG="[auth] 严重：auth.conf 要求登录，但 server.py 没有认证代码，面板当前无保护"
elif ! grep -q '_guard()' "$P"; then
  MSG="[auth] 严重：server.py 缺少 _guard 拦截，面板当前无保护"
else
  echo "[auth] 认证代码在位"
  exit 0
fi

echo "$MSG" >&2
logger -t "$TAG" "$MSG"
# 顺带写一条到服务日志，方便 journalctl 直接看到
echo "$MSG" >&2
exit 0   # 始终成功，绝不阻止服务启动
