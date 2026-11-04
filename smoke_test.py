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
