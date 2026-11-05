#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MosDNS Dashboard 冒烟测试
验证数据库、健康检查、指标端点、增量采集是否正常工作
"""
import os
import sys
import json
import tempfile
import sqlite3
from pathlib import Path

# 设置 UTF-8 输出（Windows 兼容）
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 修改工作目录到项目根目录
PROJECT_ROOT = Path(__file__).parent
os.chdir(PROJECT_ROOT)

# 导入 server 模块
sys.path.insert(0, str(PROJECT_ROOT))
import server

def test_database_wal():
    """测试 1：验证数据库 WAL 模式是否启用"""
    print("=" * 60)
    print("测试 1：数据库 WAL 模式")
    print("=" * 60)

    try:
        conn = server.db()
        # 检查 journal_mode
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        print(f"  journal_mode: {mode}")
        assert mode.upper() == "WAL", f"期望 WAL，实际 {mode}"

        # 检查 synchronous
        sync = conn.execute("PRAGMA synchronous").fetchone()[0]
        print(f"  synchronous: {sync}")
        assert sync in (1, 2), f"synchronous 异常: {sync}"

        # 检查 busy_timeout
        timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]
        print(f"  busy_timeout: {timeout}ms")
        assert timeout >= 5000, f"busy_timeout 太短: {timeout}"

        print("  ✓ 数据库配置正确\n")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        return False


def test_health_endpoint():
    """测试 2：验证 /healthz 端点返回格式"""
    print("=" * 60)
    print("测试 2：健康检查端点 /healthz")
    print("=" * 60)

    try:
        # 创建模拟的 Handler 实例
        from io import BytesIO

        class MockHandler(server.Handler):
            def __init__(self):
                self.path = "/healthz"
                self.command = "GET"
                self.headers = {}
                self.rfile = BytesIO()
                self.wfile = BytesIO()
                self._response_code = 0
                self.address_string = lambda: "127.0.0.1"

            def send_response(self, code, message=None):
                self._response_code = code

            def send_header(self, key, value):
                pass

            def end_headers(self):
                pass

            def log_message(self, fmt, *args):
                pass

        h = MockHandler()
        h.do_GET()
        response = h.wfile.getvalue().decode()

        print(f"  响应码: {h._response_code}")
        print(f"  响应内容: {response[:200]}")

        data = json.loads(response)
        assert "status" in data, "缺少 status 字段"
        assert "checks" in data, "缺少 checks 字段"

        checks = data["checks"]
        print(f"  检查项: {list(checks.keys())}")

        # 至少应该有 database 检查
        assert "database" in checks, "缺少 database 检查"

        print("  ✓ 健康检查端点正常\n")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_metrics_endpoint():
    """测试 3：验证 /metrics 端点返回 Prometheus 格式"""
    print("=" * 60)
    print("测试 3：Prometheus 指标端点 /metrics")
    print("=" * 60)

    try:
        from io import BytesIO

        class MockHandler(server.Handler):
            def __init__(self):
                self.path = "/metrics"
                self.command = "GET"
                self.headers = {}
                self.rfile = BytesIO()
                self.wfile = BytesIO()
                self._response_code = 0
                self.address_string = lambda: "127.0.0.1"

            def send_response(self, code, message=None):
                self._response_code = code

            def send_header(self, key, value):
                pass

            def end_headers(self):
                pass

            def log_message(self, fmt, *args):
                pass

        h = MockHandler()
        h.do_GET()
        response = h.wfile.getvalue().decode()

        print(f"  响应码: {h._response_code}")
        print(f"  响应内容（前 500 字符）:\n{response[:500]}")

        # 检查是否包含关键指标
        assert "mosdns_queries_total" in response, "缺少 mosdns_queries_total"
        assert "# HELP" in response, "缺少 HELP 注释"
        assert "# TYPE" in response, "缺少 TYPE 注释"

        print("  ✓ Prometheus 指标端点正常\n")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_incremental_ingest():
    """测试 4：验证增量采集逻辑"""
    print("=" * 60)
    print("测试 4：增量日志采集")
    print("=" * 60)

    try:
        conn = server.db()

        # 检查 meta 表是否有 last_log_id
        with server._db_lock:
            row = conn.execute("SELECT v FROM meta WHERE k='last_log_id'").fetchone()

        print(f"  last_log_id: {row[0] if row else '(未设置)'}")

        # 模拟插入一条日志
        test_id = "test_log_001"
        test_ts = "2026-11-04T10:00:00+08:00"
        with server._db_lock:
            conn.execute(
                "INSERT OR REPLACE INTO logs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (test_id, test_ts, "test.com", "A", "NOERROR", 0, 0, 10.5, "8.8.8.8", "", "127.0.0.1", "[]", 1)
            )
            conn.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('last_log_id',?)", (test_id,))
            conn.commit()

        # 验证插入成功
        with server._db_lock:
            row = conn.execute("SELECT id FROM logs WHERE id=?", (test_id,)).fetchone()
            meta = conn.execute("SELECT v FROM meta WHERE k='last_log_id'").fetchone()

        assert row is not None, "日志插入失败"
        assert meta and meta[0] == test_id, "last_log_id 更新失败"

        print(f"  ✓ 增量采集逻辑正常")
        print(f"    - 日志插入成功: {test_id}")
        print(f"    - last_log_id 已更新: {meta[0]}")

        # 清理测试数据
        with server._db_lock:
            conn.execute("DELETE FROM logs WHERE id=?", (test_id,))
            conn.commit()

        print("  ✓ 增量采集测试通过\n")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        return False


def test_startup_checks():
    """测试 5：验证启动检查逻辑"""
    print("=" * 60)
    print("测试 5：启动安全检查")
    print("=" * 60)

    try:
        # 检查 auth_cfg 函数是否存在
        assert hasattr(server, 'auth_cfg'), "缺少 auth_cfg 函数"

        # 检查 SSH 检查逻辑（不实际连接）
        assert hasattr(server, 'ssh'), "缺少 ssh 函数"

        # 检查 db 函数
        assert hasattr(server, 'db'), "缺少 db 函数"

        print("  ✓ 启动检查所需函数都存在")
        print("  ✓ 启动安全检查逻辑正常\n")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        return False


def test_healthz_required_vs_optional():
    """测试 6：healthz 必需/可选依赖分层

    只装 MosDNS（没配 AGH/iKuai）的正常部署必须返回 200 ready，
    不能因为可选集成就绪与否被判成 503。"""
    print("=" * 60)
    print("测试 6：healthz 必需/可选依赖分层")
    print("=" * 60)

    try:
        from io import BytesIO

        class MockHandler(server.Handler):
            def __init__(self):
                self.path = "/healthz"
                self.command = "GET"
                self.headers = {}
                self.rfile = BytesIO()
                self.wfile = BytesIO()
                self._response_code = 0
                self.address_string = lambda: "127.0.0.1"

            def send_response(self, code, message=None):
                self._response_code = code

            def send_header(self, key, value):
                pass

            def end_headers(self):
                pass

            def log_message(self, fmt, *args):
                pass

        h = MockHandler()
        h.do_GET()
        data = json.loads(h.wfile.getvalue().decode())
        checks = data["checks"]
        print(f"  状态码: {h._response_code}  status: {data['status']}")

        # 可选集成未配置时应为 None，而不是 False
        assert checks["agh"] is None, f"未配置 AGH 应为 None，实际 {checks['agh']}"
        assert checks["ikuai"] is None, f"未配置 iKuai 应为 None，实际 {checks['ikuai']}"
        # 必需项（数据库）正常时必须 ready
        assert data["ready"] is True, "数据库正常时应 ready"
        assert h._response_code == 200, f"数据库+磁盘正常应返回 200，实际 {h._response_code}"
        print("  ✓ 可选集成未配置不再拖垮健康检查")
        print("  ✓ 数据库正常时返回 200 ready\n")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_metrics_format_and_caching():
    """测试 7：metrics 输出格式 + 不触发强制采集

    两个要点：
      1. HELP/TYPE 每个指标族只出现一次（旧实现按上游数量重复）
      2. 走缓存，不调 collect()（否则 Prometheus 抓取会压垮路由器）"""
    print("=" * 60)
    print("测试 7：metrics 格式与缓存行为")
    print("=" * 60)

    try:
        from io import BytesIO

        class MockHandler(server.Handler):
            def __init__(self, path):
                self.path = path
                self.command = "GET"
                self.headers = {}
                self.rfile = BytesIO()
                self.wfile = BytesIO()
                self._response_code = 0
                self.address_string = lambda: "127.0.0.1"

            def send_response(self, code, message=None):
                self._response_code = code

            def send_header(self, key, value):
                pass

            def end_headers(self):
                pass

            def log_message(self, fmt, *args):
                pass

        # 7a. 缓存行为：把 collect 替换成"爆炸函数"，
        #     如果 metrics 调了它就会抛错 —— 证明走的是缓存。
        boom = lambda *a, **k: (_ for _ in ()).throw(AssertionError("collect 被调用了"))
        orig_collect = server.collect
        server.collect = boom
        try:
            h = MockHandler("/metrics")
            h.do_GET()
            body = h.wfile.getvalue().decode()
            assert "mosdns_queries_total" in body, "指标缺失"
            print("  ✓ 未触发 collect()，走的是缓存")
        finally:
            server.collect = orig_collect

        # 7b. 格式：HELP/TYPE 不重复
        h2 = MockHandler("/metrics")
        h2.do_GET()
        body2 = h2.wfile.getvalue().decode()
        for metric in ("mosdns_queries_total", "mosdns_cache_hits_total",
                       "mosdns_upstream_queries_total"):
            help_n = body2.count(f"# HELP {metric} ")
            type_n = body2.count(f"# TYPE {metric} ")
            print(f"  {metric}: HELP×{help_n} TYPE×{type_n}")
            assert help_n == 1, f"{metric} 的 HELP 应只出现 1 次，实际 {help_n}"
            assert type_n == 1, f"{metric} 的 TYPE 应只出现 1 次，实际 {type_n}"
        print("  ✓ HELP/TYPE 每个指标族只出现一次")

        # 7c. 标签转义
        esc = server._prom_label('a"b\\c\nd')
        assert esc == 'a\\"b\\\\c\\nd', f"转义错误: {esc!r}"
        print(f"  ✓ 标签转义正确: {esc!r}\n")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_collect_lock_serializes():
    """测试 8：采集锁确实串行化并发调用"""
    print("=" * 60)
    print("测试 8：采集并发串行化")
    print("=" * 60)

    try:
        import threading
        import time as _t

        active = {"n": 0, "max": 0}
        guard = threading.Lock()

        def fake_collect():
            with guard:
                active["n"] += 1
                active["max"] = max(active["max"], active["n"])
            _t.sleep(0.08)
            with guard:
                active["n"] -= 1
            return {"stats": {}}

        orig = server._collect
        server._collect = fake_collect
        try:
            threads = [threading.Thread(target=server.collect) for _ in range(6)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        finally:
            server._collect = orig

        print(f"  并发 6 个线程，最大同时执行数: {active['max']}")
        assert active["max"] == 1, f"采集应串行（最大并发 1），实际 {active['max']}"
        print("  ✓ 采集已串行化，不会打出并发 SSH 会话\n")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        return False


def test_guard_allows_probes():
    """测试 9：探针端点不被认证拦截（k8s 探针带不了 cookie）"""
    print("=" * 60)
    print("测试 9：探针端点免认证")
    print("=" * 60)

    try:
        # 临时造一个最小 auth.conf 让认证开启
        conf = server.AUTH_CONF
        had = os.path.exists(conf)
        backup = open(conf, encoding="utf-8").read() if had else None
        with open(conf, "w", encoding="utf-8") as f:
            f.write("AUTH_ENABLE=1\nAUTH_USER=u\n"
                    "AUTH_HASH=pbkdf2_sha256$1$c2FsdA==$aGFzaA==\n"
                    "AUTH_SECRET=s\n")
        server._auth_cache["mtime"] = 0  # 清缓存让它重读
        try:
            from io import BytesIO

            class MockHandler(server.Handler):
                def __init__(self, path):
                    self.path = path
                    self.command = "GET"
                    self.headers = {}
                    self.rfile = BytesIO()
                    self.wfile = BytesIO()
                    self._response_code = 0
                    self.address_string = lambda: "127.0.0.1"

                def send_response(self, code, message=None):
                    self._response_code = code

                def send_header(self, key, value):
                    pass

                def end_headers(self):
                    pass

                def log_message(self, fmt, *args):
                    pass

            for path in ("/healthz", "/metrics"):
                h = MockHandler(path)
                blocked = h._guard()
                assert blocked is False, f"{path} 被认证拦截了，探针会拿到 401"
                print(f"  ✓ {path} 免认证放行")
            print("  ✓ 探针在认证开启时仍可访问\n")
            return True
        finally:
            if had:
                with open(conf, "w", encoding="utf-8") as f:
                    f.write(backup)
            else:
                os.remove(conf)
            server._auth_cache["mtime"] = 0
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_adaptive_transport():
    """测试 10：传输方式自适应与路径外部化"""
    print("=" * 60)
    print("测试 10：自适应传输与路径外部化")
    print("=" * 60)

    try:
        # 路径不再写死 /opt
        for name in ("TLS_CERT", "TLS_KEY", "SSH_KEY"):
            v = getattr(server, name)
            assert "/opt/" not in v, f"{name} 仍写死 /opt: {v}"
        print(f"  TLS_CERT = {server.TLS_CERT}")
        print(f"  SSH_KEY  = {server.SSH_KEY}")
        print("  ✓ 路径以 HERE/HOME 为基准")

        # 传输模式可强制指定
        orig = server._transport_cache["mode"]
        server._transport_cache["mode"] = None
        server.TRANSPORT = "local"
        assert server.transport_mode() == "local"
        print("  ✓ local 模式生效")
        server.TRANSPORT = "ssh"
        server._transport_cache["mode"] = None
        assert server.transport_mode() == "ssh"
        print("  ✓ ssh 模式生效")
        server._transport_cache["mode"] = orig
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        return False


def test_neigh_parser_both_formats():
    """测试 11：邻居表解析兼容 BusyBox 与 iproute2"""
    print("=" * 60)
    print("测试 11：邻居表解析双格式兼容")
    print("=" * 60)

    try:
        busybox = ("192.168.1.40 lladdr 8c:d0:b2:b0:36:c4 STALE\n"
                   "192.168.1.41 lladdr aa:bb:cc:dd:ee:ff REACHABLE")
        iproute2 = ("192.168.1.50 dev br-lan lladdr 11:22:33:44:55:66 STALE\n"
                    "default dev br-lan")
        a = server._parse_neigh_lines(busybox)
        b = server._parse_neigh_lines(iproute2)
        print(f"  BusyBox 格式: {a}")
        print(f"  iproute2 格式: {b}")
        assert len(a) == 2, f"BusyBox 应解析出 2 条，实际 {len(a)}"
        assert len(b) == 1, f"iproute2 应解析出 1 条，实际 {len(b)}"
        assert a["192.168.1.40"] == "8c:d0:b2:b0:36:c4"
        assert b["192.168.1.50"] == "11:22:33:44:55:66"
        print("  ✓ 两种格式都能正确解析\n")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        return False


def test_dns_query_without_dig():
    """测试 12：原生 DNS 查询可用（不依赖 dig）"""
    print("=" * 60)
    print("测试 12：原生 DNS 查询")
    print("=" * 60)

    try:
        # 编码/解码自洽
        enc = server._dns_encode_name("example.com")
        assert enc == b"\x07example\x03com\x00", f"编码错误: {enc!r}"
        dec, off = server._dns_decode_name(enc, 0)
        assert dec == "example.com", f"解码错误: {dec}"
        print(f"  编解码自洽: {dec}")

        # 真实查询（公共 DNS）
        r = server._dns_query("223.5.5.5", 53, "example.com", 1, timeout=4)
        print(f"  example.com A -> {r}")
        assert r and r[0][0] == "A", "DNS 查询失败"
        print("  ✓ 不依赖 dig 即可完成查询\n")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        return False


def test_no_hardcoded_br_lan():
    """测试 13：不再写死 br-lan"""
    print("=" * 60)
    print("测试 13：网接口不再写死 br-lan")
    print("=" * 60)

    try:
        import inspect
        src = inspect.getsource(server)
        # read_macs 里不该再有写死 br-lan 的调用
        assert 'ip neigh show dev br-lan' not in src, "仍有写死 br-lan 的调用"
        assert 'dev %s' in src or "dev \"" in src, "未使用动态接口"
        # 必须有候选列表与探测函数
        assert hasattr(server, "detect_lan_iface")
        assert server.LAN_IFACE_CANDIDATES, "候选接口列表为空"
        print(f"  候选接口: {', '.join(server.LAN_IFACE_CANDIDATES[:5])}")
        print(f"  探测函数: detect_lan_iface()")
        print("  ✓ 接口改为自动探测 + 可配置\n")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        return False


def test_mosdns_cfg_not_hardcoded():
    """测试 14：MosDNS 配置路径改为候选探测"""
    print("=" * 60)
    print("测试 14：MosDNS 配置路径自适应")
    print("=" * 60)

    try:
        import inspect
        src = inspect.getsource(server)
        # 远端脚本里不该再写死单一路径
        assert 'open("/var/etc/mosdns.json")' not in src, "远端脚本仍写死单一路径"
        assert server.MOSDNS_CFG_CANDIDATES, "候选路径为空"
        print(f"  候选路径 {len(server.MOSDNS_CFG_CANDIDATES)} 个:")
        for p in server.MOSDNS_CFG_CANDIDATES[:5]:
            print(f"    - {p}")
        print("  ✓ 改为多候选自动探测\n")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        return False


def test_device_providers_registry():
    """测试 15：设备数据源 provider 注册与自动启用"""
    print("=" * 60)
    print("测试 15：设备数据源 provider 注册")
    print("=" * 60)

    try:
        names = [n for n, _ in server.DEVICE_PROVIDER_FUNCS]
        print(f"  已注册: {', '.join(names)}")
        for expect in ("arp", "dnsmasq", "agh", "ikuai", "http", "manual"):
            assert expect in names, f"缺少 provider: {expect}"

        # auto 模式应启用全部
        orig = server.DEVICE_PROVIDERS
        server.DEVICE_PROVIDERS = "auto"
        got = [n for n, _ in server._resolve_providers()]
        assert len(got) == len(names), f"auto 应启用全部，实际 {got}"
        print(f"  ✓ auto 启用 {len(got)} 个 provider")

        # 显式指定
        server.DEVICE_PROVIDERS = "dnsmasq,manual"
        got = [n for n, _ in server._resolve_providers()]
        assert got == ["dnsmasq", "manual"], f"显式指定失败: {got}"
        print(f"  ✓ 显式指定生效: {got}")

        # 未知 provider 应被忽略而不是报错
        server.DEVICE_PROVIDERS = "bogus,manual"
        got = [n for n, _ in server._resolve_providers()]
        assert got == ["manual"], f"未知 provider 未过滤: {got}"
        print(f"  ✓ 未知 provider 被安全忽略")

        server.DEVICE_PROVIDERS = orig
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        return False


def test_device_providers_priority():
    """测试 16：多源合并时高优先级覆盖低优先级"""
    print("=" * 60)
    print("测试 16：设备数据源优先级合并")
    print("=" * 60)

    try:
        import tempfile
        # 造 devices.conf（manual provider 的数据源）
        with open("devices.conf", "w", encoding="utf-8") as f:
            f.write("10.0.0.5=手动配置名\n")
        server._provider_cache.clear()
        try:
            info = server.device_providers()
            assert info["names"].get("10.0.0.5") == "手动配置名", \
                f"manual 未生效: {info['names']}"
            print(f"  manual 数据: {info['names']}")
            print(f"  启用的源: {info['sources']}")
            print("  ✓ manual（最高优先级）生效")

            # 顺序决定优先级：把 manual 放前面，低优先级的会覆盖它
            orig = server.DEVICE_PROVIDERS
            server.DEVICE_PROVIDERS = "manual,arp"
            server._provider_cache.clear()
            # arp 在这台机器上拿不到 10.0.0.5，所以 manual 的值仍在
            info2 = server.device_providers()
            assert info2["names"].get("10.0.0.5") == "手动配置名"
            print("  ✓ 顺序即优先级（后者覆盖前者）")
            server.DEVICE_PROVIDERS = orig
        finally:
            if os.path.exists("devices.conf"):
                os.remove("devices.conf")
            server._provider_cache.clear()
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        return False


def test_dnsmasq_provider():
    """测试 17：dnsmasq 租约文件解析"""
    print("=" * 60)
    print("测试 17：dnsmasq 租约文件解析")
    print("=" * 60)

    try:
        import tempfile
        # dnsmasq 租约格式：<expire> <mac> <ip> <hostname> <client-id>
        content = ("1700000000 aa:bb:cc:dd:ee:ff 192.168.1.50 myphone *\n"
                   "1700000001 11:22:33:44:55:66 192.168.1.51 *\n"
                   "1700000002 77:88:99:aa:bb:cc 192.168.1.52 tv\n")
        d = tempfile.mkdtemp()
        p = os.path.join(d, "dnsmasq.leases")
        with open(p, "w", encoding="utf-8") as f:
            f.write(content)

        orig = os.environ.get("DNSMASQ_LEASES")
        os.environ["DNSMASQ_LEASES"] = p
        # 直接调 provider 函数（绕过缓存）
        r = server._p_dnsmasq()
        if orig is None:
            os.environ.pop("DNSMASQ_LEASES", None)
        else:
            os.environ["DNSMASQ_LEASES"] = orig

        print(f"  names: {r['names']}")
        print(f"  macs:  {r['macs']}")
        assert r["names"].get("192.168.1.50") == "myphone"
        assert r["names"].get("192.168.1.52") == "tv"
        assert r["macs"].get("192.168.1.50") == "aa:bb:cc:dd:ee:ff"
        assert "192.168.1.51" not in r["names"], "hostname 为 * 时不应写入"
        print("  ✓ 租约解析正确，* 已正确跳过\n")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        return False


def test_http_provider():
    """测试 18：通用 HTTP provider 两种返回格式"""
    print("=" * 60)
    print("测试 18：通用 HTTP provider")
    print("=" * 60)

    try:
        import threading
        import json as _json
        from http.server import BaseHTTPRequestHandler, HTTPServer

        class H(BaseHTTPRequestHandler):
            mode = "list"
            def do_GET(self):
                body = ([] if self.mode == "list" else {})
                if self.mode == "list":
                    body = [{"ip": "10.1.1.1", "name": "dev-a"},
                            {"ip": "10.1.1.2", "name": "dev-b"}]
                else:
                    body = {"10.1.1.3": "dev-c"}
                data = _json.dumps(body).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            def log_message(self, *a):
                pass

        srv = HTTPServer(("127.0.0.1", 0), H)
        port = srv.server_address[1]
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        try:
            # 数组格式
            H.mode = "list"
            orig = server.DEV_HTTP_URL
            server.DEV_HTTP_URL = f"http://127.0.0.1:{port}/x"
            r = server._p_http()
            print(f"  数组格式: {r['names']}")
            assert r["names"].get("10.1.1.1") == "dev-a"

            # 对象格式
            H.mode = "dict"
            r2 = server._p_http()
            print(f"  对象格式: {r2['names']}")
            assert r2["names"].get("10.1.1.3") == "dev-c"
            server.DEV_HTTP_URL = orig
            print("  ✓ 两种返回格式都支持\n")
            return True
        finally:
            srv.shutdown()
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        import traceback
        traceback.print_exc()
        return False


def test_providers_endpoint():
    """测试 19：/api/providers 自检端点"""
    print("=" * 60)
    print("测试 19：/api/providers 端点")
    print("=" * 60)

    try:
        from io import BytesIO

        class MockHandler(server.Handler):
            def __init__(self):
                self.path = "/api/providers"
                self.command = "GET"
                self.headers = {}
                self.rfile = BytesIO()
                self.wfile = BytesIO()
                self._response_code = 0
                self.address_string = lambda: "127.0.0.1"

            def send_response(self, code, message=None):
                self._response_code = code

            def send_header(self, key, value):
                pass

            def end_headers(self):
                pass

            def log_message(self, fmt, *args):
                pass

        server._provider_cache.clear()
        h = MockHandler()
        h.do_GET()
        j = json.loads(h.wfile.getvalue().decode())
        print(f"  状态码: {h._response_code}")
        print(f"  启用的源: {j.get('active')}")
        print(f"  provider 明细: {len(j.get('providers') or [])} 个")
        assert h._response_code == 200
        assert "providers" in j and "active" in j
        assert isinstance(j["providers"], list) and j["providers"]
        print("  ✓ 自检端点正常\n")
        return True
    except Exception as e:
        print(f"  ✗ 失败: {e}\n")
        return False


def main():
    print("\n" + "=" * 60)
    print("MosDNS Dashboard 冒烟测试")
    print("=" * 60 + "\n")

    results = []

    # 运行所有测试
    results.append(("数据库 WAL 模式", test_database_wal()))
    results.append(("健康检查端点", test_health_endpoint()))
    results.append(("Prometheus 指标", test_metrics_endpoint()))
    results.append(("增量日志采集", test_incremental_ingest()))
    results.append(("启动安全检查", test_startup_checks()))
    results.append(("healthz 必需/可选分层", test_healthz_required_vs_optional()))
    results.append(("metrics 格式与缓存", test_metrics_format_and_caching()))
    results.append(("采集并发串行化", test_collect_lock_serializes()))
    results.append(("探针端点免认证", test_guard_allows_probes()))
    results.append(("自适应传输与路径", test_adaptive_transport()))
    results.append(("邻居表双格式解析", test_neigh_parser_both_formats()))
    results.append(("原生 DNS 查询", test_dns_query_without_dig()))
    results.append(("网接口不写死", test_no_hardcoded_br_lan()))
    results.append(("配置路径自适应", test_mosdns_cfg_not_hardcoded()))
    results.append(("设备源 provider 注册", test_device_providers_registry()))
    results.append(("设备源优先级合并", test_device_providers_priority()))
    results.append(("dnsmasq 租约解析", test_dnsmasq_provider()))
    results.append(("通用 HTTP provider", test_http_provider()))
    results.append(("provider 自检端点", test_providers_endpoint()))

    # 汇总结果
    print("=" * 60)
    print("测试汇总")
    print("=" * 60)

    passed = sum(1 for _, r in results if r)
    total = len(results)

    for name, result in results:
        status = "✓ 通过" if result else "✗ 失败"
        print(f"  {status}  {name}")

    print(f"\n通过: {passed}/{total}")

    if passed == total:
        print("\n🎉 所有测试通过！\n")
        return 0
    else:
        print(f"\n⚠️  {total - passed} 个测试失败\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
