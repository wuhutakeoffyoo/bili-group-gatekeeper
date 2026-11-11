"""Explicit application lifecycle, transport registry and event routing."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import shutil
from pathlib import Path

from app.config import ConfigManager
from app.database.async_storage import run_storage
from app.database.legacy import initialize_database
from app.notifications.dedup import NotificationDeduplicator
from app.onebot.client import OneBotClient
from app.onebot.events import OneBotEvent
from app.onebot.inspector import OneBotRequestInspector
from app.onebot.recovery import MissedRequestRecovery
from app.settings import get_settings

logger = logging.getLogger(__name__)


class Runtime:
    def __init__(self, *, settings=None, config=ConfigManager, service=None, clients=None,
                 ownership=None, patrol=None, credentials=None, background_enabled=True):
        from app.commands import CommandService
        from app.service import GatekeeperService

        self.settings = settings or get_settings()
        self.config = config
        self.clients: dict[str, OneBotClient] = dict(clients or {})
        self.service = service or GatekeeperService(config, settings=self.settings, client_provider=lambda: self.client)
        self.commands = CommandService(self.service, settings=self.settings)
        self.commands.task_factory = self.spawn
        self.ownership = ownership
        self.patrol = patrol
        self.credentials = credentials
        self.request_inspector = OneBotRequestInspector(self.get_client)
        self.reconciler = None
        self.request_recovery = MissedRequestRecovery(self)
        self.background_enabled = background_enabled
        self.started = False
        self._tasks: set[asyncio.Task] = set()
        self._disk_notifications = NotificationDeduplicator()
        self._cookie_notice_issue = None

    @property
    def client(self):
        return next((client for client in self.clients.values() if client.connected), None)

    def get_client(self, self_id=None):
        if self_id is None:
            return self.client
        return next((client for client in self.clients.values() if client.self_id == str(self_id) and client.connected), None)

    def spawn(self, coroutine, *, name=None):
        task = asyncio.create_task(coroutine, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._task_finished)
        return task

    def _task_finished(self, task):
        self._tasks.discard(task)
        if not task.cancelled():
            error = task.exception()
            if error:
                logger.error("Background task %s failed", task.get_name(), exc_info=(type(error), error, error.__traceback__))

    def new_client(self, *, url=None, self_id=""):
        return OneBotClient(
            url, self_id=self_id,
            access_token=str(getattr(self.settings, "onebot_access_token", "")),
            event_handler=self.handle_event,
            action_timeout=float(getattr(self.settings, "onebot_action_timeout", 30)),
            event_workers=int(getattr(self.settings, "onebot_event_workers", 4)),
            event_queue_size=int(getattr(self.settings, "onebot_event_queue_size", 256)))

    async def start(self):
        if self.started:
            return
        try:
            await run_storage(self.config.initialize, settings=self.settings)
            from app.bilibili.pacing import configure_pacing

            configure_pacing(self.settings)
            await run_storage(initialize_database)
            await self.service.decisions.restore_pending()
            from app.gatekeeper.reconciler import DecisionReconciler

            self.reconciler = DecisionReconciler(self.service.decisions, inspector=self.inspect_pending_request,
                                                replay=self.replay_pending_action, notify=self.notify,
                                                interval=float(getattr(self.settings, "decision_reconcile_interval", 30)))
            if self.credentials is None:
                from app.bilibili.credentials import CredentialService

                self.credentials = CredentialService(config=self.config, notify=self.notify)
            self.service.credentials = self.credentials
            if self.ownership is None:
                from app.ownership import OwnershipService

                self.ownership = OwnershipService(config=self.config)
            if self.patrol is None:
                from app.patrol import PatrolService

                self.patrol = PatrolService(self, config=self.config)
            self.service.ownership = self.ownership
            self.ownership.resume = self.resume_ownership
            self.service.patrol = self.patrol
            urls = getattr(self.settings, "onebot_ws_urls", [])
            if isinstance(urls, str):
                urls = [url.strip() for url in urls.split(",") if url.strip()]
            for url in urls:
                key = f"forward:{url}"
                if key not in self.clients:
                    self.clients[key] = self.new_client(url=url)
            for client in self.clients.values():
                client.event_handler = self.handle_event
                await client.start()
            self.started = True
            if self.background_enabled:
                from app.bilibili import monitor

                monitor.reset_dedup_state()
                self.reconciler.start()
                self.spawn(self.request_recovery.loop(), name="missed-group-requests")
                self.spawn(self._disk_monitor(), name="disk-monitor")
                self.spawn(self._cookie_monitor(), name="bilibili-cookie-monitor")
                self.spawn(self.credentials.loop(), name="bilibili-credentials")
                self.spawn(self._cookie_startup(), name="bilibili-startup")
                for name in ("ownership", "patrol"):
                    extension = getattr(self, name)
                    if extension is not None and hasattr(extension, "loop"):
                        self.spawn(extension.loop(), name=name)
        except BaseException:
            await self.close()
            raise

    async def notify(self, message):
        return await self.service.notify_admins(self.client, message)

    async def check_cookie_once(self):
        from app.bilibili import monitor

        detail = await monitor.run_cookie_check_once()
        if detail.result == monitor.CookieCheckResult.VALID:
            self._cookie_notice_issue = None
            return detail
        issue = detail.result.value
        if self._cookie_notice_issue != issue:
            if detail.result == monitor.CookieCheckResult.EXPIRED:
                message = "B站Cookie已明确失效，请使用 /登录二维码、/设置cookie 或 /获取cookie 更新凭证。"
            else:
                message = "B站Cookie检查异常，当前无法可靠判断是否过期；请检查网络或接口状态。"
            if await self.notify(message):
                self._cookie_notice_issue = issue
        return detail

    async def _cookie_monitor(self):
        from app.bilibili import monitor

        await asyncio.sleep(monitor.STARTUP_CHECK_DELAY_SECONDS)
        while True:
            try:
                await self.check_cookie_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Cookie validity monitor failed; will retry next cycle")
            await asyncio.sleep(monitor.get_check_interval_seconds())

    async def inspect_pending_request(self, metadata):
        from app.gatekeeper.decisions import RequestEvidence

        if self.request_inspector is not None:
            return await self.request_inspector(metadata)
        # Standard OneBot V11 doesn't provide a definitive request-outcome query.
        # Never infer an approval from membership or absence from a request list.
        return RequestEvidence(state="unknown", **{key: str(metadata.get(key, "")) for key in
            ("bot_id", "group_id", "qq", "sub_type", "flag", "expected_result")},
            detail="当前OneBot未提供可核对该申请的确定证据，请管理员确认实际审批结果")

    async def replay_pending_action(self, metadata):
        from app.onebot.client import OneBotDisconnected

        client = self.get_client(metadata["bot_id"])
        if client is None:
            raise OneBotDisconnected("the original bot is not connected")
        await client.call_action(metadata["action"], **metadata["params"])

    @staticmethod
    def _ownership_request_audit(request_key):
        from app.database.legacy import database_connection

        with database_connection() as connection:
            row = connection.execute(
                "SELECT id,result,action_status FROM join_request_audits WHERE request_key=? LIMIT 1",
                (request_key,)).fetchone()
        if row is None:
            return None
        value = dict(row)
        if value["action_status"] == "pending":
            return {"state": "pending", "audit_id": value["id"],
                    "detail": "账号验证已完成，但QQ审批结果尚未确认；请等待对账或联系管理员，勿重复操作"}
        if value["action_status"] == "applied" and value["result"] == "approved":
            return {"state": "approved", "audit_id": value["id"], "detail": "账号验证及本群条件复核通过，原入群申请已获得QQ接口成功回执"}
        if value["action_status"] == "applied" and value["result"] == "rejected":
            return {"state": "rejected", "audit_id": value["id"], "detail": "账号验证成功，但重新检查未通过本群当前规则，请查看QQ拒绝原因后再申请"}
        return {"state": "manual", "audit_id": value["id"], "detail": "该申请已有审批记录，请联系管理员核对实际结果"}

    async def resume_ownership(self, state):
        """Resume only an original, exactly unhandled request after QR proof.

Every qualification and binding condition runs again. The final response comes
from the durable exact-request audit, never membership or browser assertions.
"""
        from src.gatekeeper.decisions import DecisionRequest

        request = state.get("request") if isinstance(state, dict) else None
        required = ("bot_id", "group_id", "qq", "sub_type", "flag")
        if (not isinstance(request, dict) or any(not str(request.get(key, "")).strip() for key in required)
                or request.get("sub_type") != "add" or not str(request.get("qq", "")).isdigit()
                or not str(request.get("group_id", "")).isdigit()):
            return {"state": "verified", "detail": "账号验证完成，但原申请信息不完整；请用同一QQ和UID重新申请"}
        metadata = {key: str(request[key]) for key in required}
        metadata["expected_result"] = "approved"
        exact = DecisionRequest(metadata["bot_id"], metadata["group_id"], metadata["sub_type"], metadata["flag"])
        recorded = await run_storage(self._ownership_request_audit, exact.request_key)
        if recorded is not None:
            return recorded
        client = self.get_client(metadata["bot_id"])
        if client is None:
            return {"state": "verified", "detail": "账号验证完成，但原机器人当前未连接；未执行审批，请稍后重新申请或联系管理员"}
        evidence = await self.inspect_pending_request(metadata)
        if getattr(evidence, "state", "") != "unhandled" or not evidence.matches(metadata):
            return {"state": "verified", "detail": "账号验证完成，但无法确定原申请仍未处理；未自动审批，请用同一QQ和UID重新申请或联系管理员"}
        await self.service.handle_group_request(client, {
            "post_type": "request", "request_type": "group", "sub_type": "add",
            "self_id": int(metadata["bot_id"]), "group_id": int(metadata["group_id"]),
            "user_id": int(metadata["qq"]), "flag": metadata["flag"],
            "comment": str(request.get("comment") or ("UID:" + str(state.get("uid", "")))),
        })
        recorded = await run_storage(self._ownership_request_audit, exact.request_key)
        return recorded or {"state": "waiting", "detail": "账号验证完成，但当前规则尚未允许执行审批；原申请保持未处理，请联系管理员复核"}

    async def handle_event(self, payload, client=None):
        event = OneBotEvent(payload) if isinstance(payload, dict) else payload
        client = client or self.get_client(getattr(event, "self_id", None))
        if client is None:
            logger.warning("Unable to route event: no OneBot client")
            return
        for extension in (self.ownership, self.patrol):
            if extension is not None and hasattr(extension, "handle_event"):
                if await extension.handle_event(event, client):
                    return
        post_type = getattr(event, "post_type", "")
        if post_type == "request" and getattr(event, "request_type", "") == "group":
            await self.service.handle_group_request(client, event)
        elif post_type == "notice" and getattr(event, "notice_type", "") == "group_decrease":
            await self.service.handle_group_decrease(client, event)
            if self.patrol is not None:
                await self.patrol.on_group_decrease(str(event.group_id), str(event.user_id))
        elif post_type == "message":
            reply = await self.commands.dispatch(str(event.user_id), event)
            if reply is not None:
                await client.send_private_msg(user_id=int(event.user_id), message=reply)

    async def attach_reverse(self, websocket, self_id):
        key = f"reverse:{self_id}"
        previous = self.clients.get(key)
        if previous is not None:
            await previous.close()
        client = self.new_client(self_id=self_id)
        self.clients[key] = client
        try:
            await client.serve(websocket)
        finally:
            await client.close()
            if self.clients.get(key) is client:
                self.clients.pop(key, None)

    async def _cookie_startup(self):
        from app.bilibili.credential import CookieManager

        cookie = self.config.get_bili_cookie().strip()
        if not cookie:
            current = self.config.get_config()
            values = [str(getattr(current, "cookiecloud_" + name, "")).strip()
                      for name in ("host", "uuid", "key")]
            if all(values):
                cookie = await CookieManager.fetch_from_cookiecloud(*values)
                if cookie:
                    await run_storage(self.config.set_bili_cookie, cookie)
        if cookie:
            await self.credentials.ensure_fingerprints()
            cookie = self.config.get_bili_cookie().strip()
            await self.service.ensure_login_account_follows_targets(cookie)
        else:
            logger.warning("No Bilibili credentials configured; use administrator QR login or CookieCloud")

    async def _disk_monitor(self):
        while True:
            try:
                await self.check_disk_space()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Disk space monitor failed")
            await asyncio.sleep(6 * 3600)

    async def check_disk_space(self):
        from app.notifications import email as email_notifier

        path = Path(str(getattr(self.settings, "local_log_directory", "logs") or "logs")).expanduser()
        if not path.is_absolute():
            path = Path(getattr(self.settings, "app_root", Path.cwd())) / path
        path = path.resolve()
        path.mkdir(parents=True, exist_ok=True)
        free = None
        try:
            usage = await asyncio.to_thread(shutil.disk_usage, path)
            free = usage.free / (1024 ** 3)
            level = "严重" if free < .2 else "警告" if free < 1 else None
            if level is None:
                self._disk_notifications.reset()
                return
            message = f"[磁盘告警-{level}] 日志目录所在磁盘剩余 {free:.2f} GB，请检查磁盘空间。"
        except OSError:
            level, message = "检查失败", "[磁盘告警-检查失败] 无法读取日志目录所在磁盘空间。"
        logger.warning(message)
        await self.notify(message)
        if self._disk_notifications.should_attempt(level):
            sent = await email_notifier.send_notification(
                f"【Bilibili QQ Group Gatekeeper 磁盘告警】{level}",
                f"{message}\n监控路径：{path}\n仅通知，不自动清理日志或运行数据。")
            if sent:
                self._disk_notifications.mark_sent(level)

    async def close(self):
        if self.reconciler is not None:
            await self.reconciler.stop()
            self.reconciler = None
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        for client in list(self.clients.values()):
            await client.close()
        self.clients.clear()
        from app.bilibili.pacing import OBSERVATIONS

        await OBSERVATIONS.close()
        seen = set()
        for component in (self.ownership, self.patrol, self.credentials, self.service):
            if component is not None and id(component) not in seen and hasattr(component, "close"):
                seen.add(id(component))
                with contextlib.suppress(Exception):
                    await component.close()
        self.started = False
