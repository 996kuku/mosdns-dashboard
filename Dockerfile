# MosDNS 仪表盘
#
# 纯 Python 标准库实现，所以镜像可以很小：
#   - 基础镜像 python:3.12-slim（约 120MB）
#   - 只装 openssh-client：SSH 模式下要连路由器。
#     local 模式用不到，但装上也不占多少体积，省得切换模式时才发现缺。
FROM python:3.12-slim

# tini 作为 PID 1：面板起了后台线程（采集 + SSE 推送），
# 没有 init 进程时容器可能收不到 SIGTERM 而残留。
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openssh-client \
      tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 只复制运行必需的文件（server.py 是单文件服务，前端内联在里面对应位置）
COPY server.py index.html login.html ./
COPY static ./static
COPY tools ./tools
COPY *.conf.example ./

# 非 root 运行；数据目录单独挂 volume
RUN useradd --system --home /app --uid 10001 --shell /usr/sbin/nologin mdash \
 && mkdir -p /app/data \
 && chown -R mdash:mdash /app

# 注：下面 DASH_TLS_KEY / SSH_KEY 是**文件路径**不是密钥内容，
#     build-check 会误报 secret，可忽略（要用内联 secret 得用 --mount=type=secret）。
ENV MOSDNS_TRANSPORT=local \
    DASH_HOST=0.0.0.0 \
    DASH_PORT=8088 \
    DATA_DIR=/app/data \
    DASH_TLS_CERT=/app/ssl/server.pem \
    DASH_TLS_KEY=/app/ssl/server.key \
    SSH_KEY=/app/data/id_ed25519_mosdns \
    DEVICE_PROVIDERS=auto

VOLUME ["/app/data"]

EXPOSE 8088

USER mdash

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-u", "server.py"]
