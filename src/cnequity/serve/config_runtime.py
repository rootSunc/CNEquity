"""配置文件与常驻服务的同步；切换目录时重新建立缓存和存储维护对象。"""

from __future__ import annotations

import threading

from cnequity.config import load_config
from cnequity.serve.ops.catalog import OpsError


class ConfigRuntime:
    def __init__(self, app):
        self.app = app
        self.lock = threading.RLock()
        self.active_requests = 0
        self.signature = self._signature()
        self.notice = None

    def _signature(self):
        path = self.app.state.config_path
        if path is None:
            return None
        try:
            stat = path.stat()
        except FileNotFoundError:
            return None
        return stat.st_mtime_ns, stat.st_size

    def remember(self):
        self.signature = self._signature()
        self.notice = None

    def reload(self):
        from cnequity.serve.app import activate_lake

        path = self.app.state.config_path
        if path is None:
            raise OpsError("没有可重新加载的配置文件。")
        try:
            before = self._signature()
            config = load_config(path)
            if before != self._signature():
                raise OpsError("配置文件正在被修改，请保存完成后刷新页面。")
        except (OSError, ValueError) as exc:
            raise OpsError("配置文件无法读取或格式无效，请修正配置文件后刷新页面。") from exc
        old = self.app.state.config
        switching = old is not None and old.data_root != config.data_root
        maintenance = self.app.state.storage_maintenance
        if switching:
            from cnequity.file_lock import is_locked
            from cnequity.serve.ops.preflight import engine_blockers, scheduler_blockers
            from cnequity.serve.ops.records import slot_path

            # 正在运行的操作继续使用原湖；切换前必须等已有请求、采集和维护结束。
            blocked = (
                self.active_requests > 0
                or (maintenance is not None and maintenance.busy)
                or is_locked(slot_path(old))
                or engine_blockers(old, "lake-write")
                or scheduler_blockers(old, "lake-write")
            )
            if blocked:
                raise OpsError(
                    "数据目录已修改，但仍有请求、取数或存储维护正在执行；结束后刷新页面即可切换。"
                )
        if maintenance is not None:
            with maintenance.gate.condition:
                if switching and (maintenance.gate.closed or maintenance.gate.readers):
                    raise OpsError("数据目录已修改，正在等待原数据湖的读取或维护结束，请稍后刷新。")
                if switching:
                    maintenance.gate.close()
        try:
            if switching or old is None:
                activate_lake(self.app, config)
            else:
                # 同一个湖仍保留维护任务和审批，避免普通开关变更丢失维护进度。
                self.app.state.config = config
                self.app.state.view.config = config
                self.app.state.view.invalidate()
                maintenance.config = config
                self.app.state.ops.bind(config)
            self.app.state.schedule_service = None
            self.app.state.settings_service = None
            self.signature = before
            self.notice = None
            return config
        except Exception:
            if switching and maintenance is not None:
                maintenance.gate.reopen()
            raise

    def synchronize(self):
        if self.app.state.setup or self.app.state.config_path is None:
            return
        with self.lock:
            try:
                if self._signature() == self.signature:
                    return
                self.reload()
            except OSError as exc:
                self.notice = "配置文件状态无法读取，请检查访问权限后刷新页面。"
                raise OpsError(self.notice) from exc
            except OpsError as exc:
                self.notice = str(exc)
                raise
