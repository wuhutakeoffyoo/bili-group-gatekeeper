"""Private administrator commands with an explicit reply boundary."""
from __future__ import annotations

import asyncio
import json
import logging
import shlex

from app.bilibili.credential import CookieManager
from app.config import parse_bool, parse_reject_reasons, parse_uid_list
from app.database.async_storage import run_storage
from app.database.leaves import LeaveRecordManager
from app.database.records import JoinRequestRecordManager
from app.onebot.events import OneBotEvent
from app.reports import format_user_check_reply, needs_profile_name
from src.gatekeeper.config import GroupConfig
from src.gatekeeper.decision_runtime import identity_decision_keys
from src.gatekeeper.formatters import format_group_config

logger = logging.getLogger(__name__)


class CommandReply(Exception):
    """A user-facing reply, caught once by CommandService.dispatch."""


def split_arguments(raw):
    try:
        return shlex.split(raw)
    except ValueError:
        return raw.split()


class CommandService:
    SUPER_COMMANDS = {"设置群", "设置cookie", "获取cookie", "登录二维码", "移除退群记录", "待确认审批", "确认审批"}

    def __init__(self, service, *, settings=None):
        self.service = service
        self.config = service.config
        self.settings = settings or service.settings
        self.task_factory = None
        self.handlers = {
            "help": self.help, "帮助": self.help, "设置群": self.set_group,
            "设置cookie": self.set_cookie, "获取cookie": self.fetch_cookie,
            "登录二维码": self.qrcode, "查看配置": self.show_config,
            "移除退群记录": self.remove_leaves, "解除绑定": self.clear_binding,
            "检查qq": self.check_qq, "检查bili": self.check_bili,
            "待确认审批": self.pending, "确认审批": self.resolve_pending,
        }

    async def dispatch(self, actor, event=None, text=None):
        if isinstance(actor, (dict, OneBotEvent)) and event is None:
            event = actor
            actor = event.get("user_id", "") if isinstance(event, dict) else event.user_id
        event = OneBotEvent(event) if isinstance(event, dict) else event
        if event is None or getattr(event, "message_type", "") != "private":
            return None
        actor = str(actor)
        if actor not in self.service.admins():
            return None
        raw = str(text if text is not None else event.plain_text).strip()
        starts = getattr(self.settings, "command_start", ["/", ""])
        starts = [starts] if isinstance(starts, str) else starts
        matched = None
        for prefix in sorted(starts, key=len, reverse=True):
            if raw.startswith(prefix):
                candidate = raw[len(prefix):]
                parts = candidate.split(maxsplit=1)
                name = parts[0] if parts else ""
                args = parts[1] if len(parts) > 1 else ""
                if name.casefold() in self.handlers:
                    matched = (name.casefold(), args.strip())
                    break
        if matched is None:
            return None
        name, args = matched
        if name in self.SUPER_COMMANDS and actor not in self.service.superadmins():
            return None
        try:
            return await self.handlers[name](actor, args)
        except CommandReply as reply:
            return str(reply)
        except (ValueError, TypeError) as exc:
            logger.info("Invalid command argument for %s (%s)", name, type(exc).__name__)
            return "参数无效，请使用 /help 查看用法。"
        except Exception:
            logger.exception("Administrator command %s failed", name)
            return "操作失败，已保留原有数据，请检查日志后重试。"

    async def help(self, actor, raw):
        return "\n".join([
            "━━━━ 可用指令 ━━━━", "", "▎管理员私聊",
            "/help 或 /帮助", "/查看配置 [<群号>]", "/解除绑定 <QQ号>",
            "/检查qq <QQ号>", "/检查bili <群号> <B站UID或昵称>", "",
            "▎超级管理员私聊", "/设置群 <群号> [<参数> <值> ...]",
            "/移除退群记录 <QQ号>", "/设置cookie <B站Cookie>", "/获取cookie", "/登录二维码",
            "/待确认审批", "/确认审批 <审计ID> <applied|failed>", "",
            "群聊及非管理员消息不回复。命令英文字母不区分大小写。",
            "开关支持 on/off、true/false、1/0。",
            "设置群参数：enable、required_qq_level、required_bili_level、target_uids、",
            "target_medal_uids、required_medal_level、reject_reason_with_leave_time、reject_reasons、custom_reject_uids。",
            "审核流程通过管理控制台保存至 SQLite，支持 when 表达式或兼容 conditions + mode。",
            "/检查bili 不写申请、绑定、退群记录；未提供 QQ 的条件按无法判断参与模拟。",
        ])

    async def set_group(self, actor, raw):
        args = split_arguments(raw)
        if not args:
            raise CommandReply("用法：/设置群 <群号> [<参数名> <值> ...]\n示例：/设置群 123456789 enable on required_qq_level 32\n参数：enable、required_qq_level、required_bili_level、target_uids、target_medal_uids、required_medal_level、reject_reason_with_leave_time、reject_reasons、custom_reject_uids。")
        group_id = args[0]
        if not group_id.isdigit():
            raise CommandReply("群号需要填写纯数字")
        if len(args) % 2 == 0:
            raise CommandReply(f"参数 {args[-1]} 缺少对应的值")
        existing = self.config.get_group_config(group_id)
        group = existing.model_copy(deep=True) if existing else GroupConfig(group_id=group_id)
        updated = []
        for index in range(1, len(args), 2):
            key, value = args[index].lower(), args[index + 1]
            if key == "enable":
                group.enabled = parse_bool(value)
            elif key in {"required_qq_level", "required_bili_level", "required_medal_level"}:
                try:
                    value = int(value)
                except (ValueError, TypeError):
                    raise CommandReply(f"{key} 需要输入合法的数字等级") from None
                if value < 0:
                    raise CommandReply(f"{key} 不能小于 0")
                setattr(group, key, value)
            elif key in {"target_uids", "target_medal_uids"}:
                try:
                    uids = parse_uid_list(value)
                    if any(not 0 < uid <= 2**63 - 1 for uid in uids):
                        raise ValueError("UID out of range")
                except (ValueError, TypeError):
                    raise CommandReply(f"{key} 需要输入有效的正整数 UID，多个用逗号分隔") from None
                setattr(group, key, uids)
            elif key == "reject_reason_with_leave_time":
                group.reject_reason_with_leave_time = parse_bool(value)
            elif key == "reject_reasons":
                try:
                    group.reject_reasons = parse_reject_reasons(value)
                except (ValueError, TypeError):
                    raise CommandReply("reject_reasons 需要输入合法的 JSON 对象") from None
            elif key == "custom_reject_uids":
                try:
                    raw_restrictions = json.loads(value)
                    if not isinstance(raw_restrictions, dict):
                        raise ValueError("Expected a UID mapping")
                    restrictions = {}
                    for uid, reason in raw_restrictions.items():
                        uid = uid.strip()
                        if not uid.isascii() or not uid.isdigit() or not 0 < int(uid) <= 2**63 - 1:
                            raise ValueError("Invalid UID")
                        if isinstance(reason, dict):
                            reason = reason.get("reason")
                        if not isinstance(reason, str) or not reason.strip():
                            raise ValueError("Expected a nonempty reason")
                        restrictions[str(int(uid))] = reason.strip()
                    group.custom_reject_uids = restrictions
                except (ValueError, TypeError):
                    raise CommandReply('custom_reject_uids 需要输入 UID 到拒绝原因的 JSON 对象，例如 {"12345":"拒绝原因"}') from None
            else:
                raise CommandReply(f"不支持的参数：{key}")
            updated.append(key)
        # Persistence commits before the new snapshot is made visible.
        await run_storage(self.config.set_group_config, group, updated)
        summary = ""
        cookie = self.config.get_bili_cookie().strip()
        if "target_uids" in updated and cookie:
            summary = "\n" + await self.service.ensure_login_account_follows_targets(cookie)
        return f"群 {group_id} 配置已更新" + (f"：{'、'.join(updated)}" if updated else "") + summary

    def cookiecloud(self):
        current = self.config.get_config()
        return tuple(str(getattr(current, "cookiecloud_" + name, "")).strip()
                     for name in ("host", "uuid", "key"))

    async def set_cookie(self, actor, raw):
        if not raw.strip():
            raise CommandReply("用法：/设置cookie <B站Cookie>")
        await run_storage(self.config.set_bili_cookie, raw.strip())
        return "B站 Cookie 已保存\n" + await self.service.ensure_login_account_follows_targets(raw.strip())

    async def fetch_cookie(self, actor, raw):
        host, uuid, key = self.cookiecloud()
        if not all((host, uuid, key)):
            raise CommandReply("请先配置 COOKIECLOUD_HOST、COOKIECLOUD_UUID、COOKIECLOUD_KEY")
        cookie = await CookieManager.fetch_from_cookiecloud(host, uuid, key)
        if not cookie:
            raise CommandReply("从 CookieCloud 获取 Cookie 失败")
        await run_storage(self.config.set_bili_cookie, cookie)
        return "已从 CookieCloud 获取并保存 B站 Cookie\n" + await self.service.ensure_login_account_follows_targets(cookie)

    async def qrcode(self, actor, raw):
        if self.service.credentials is not None:
            session = await self.service.credentials.start_qr_login()
            import base64
            import io
            import qrcode
            from qrcode.image.pure import PyPNGImage

            output = io.BytesIO()
            qrcode.make(session['url'], image_factory=PyPNGImage).save(output)
            encoded = base64.b64encode(output.getvalue()).decode('ascii')
            if self.task_factory:
                self.task_factory(self._poll_qrcode(actor, session["session_id"]), name="administrator-qr-login")
            return f"请使用B站客户端扫描下方二维码并确认登录。\n[CQ:image,file=base64://{encoded}]\n二维码有效期约3分钟，登录成功后凭证会自动保存。"
        url, path = await run_storage(CookieManager.generate_qrcode)
        return f"二维码已生成：{path}\n登录链接：{url}"

    async def _poll_qrcode(self, actor, identifier):
        while True:
            await asyncio.sleep(2)
            result = await self.service.credentials.poll_qr_login(identifier)
            state = result.get("state")
            if state not in {"success", "expired"}:
                continue
            client = self.service.client_provider()
            if client:
                text = "B站扫码登录成功，凭证已保存。" if state == "success" else "B站登录二维码已过期，请重新使用 /登录二维码。"
                await client.send_private_msg(user_id=int(actor), message=text)
            if state == "success":
                await self.service.ensure_login_account_follows_targets(self.config.get_bili_cookie())
            return

    async def show_config(self, actor, raw):
        if raw.strip():
            group = self.config.get_group_config(raw.strip())
            if not group:
                raise CommandReply(f"群 {raw.strip()} 未配置")
            return format_group_config(group)
        config = self.config.get_config()
        status = "已设置" if self.config.get_bili_cookie() else "已设置 CookieCloud" if all(self.cookiecloud()) else "未设置"
        lines = ["【全局配置】", f"B站 Cookie：{status}", f"命令私聊管理员：{'、'.join(sorted(self.service.admins())) or '未设置'}",
                 f"异常通知模式：{self.config.get_notify_mode()}", f"群配置数量：{len(config.groups)}"]
        for group_id in sorted(config.groups):
            lines += ["", format_group_config(config.groups[group_id])]
        return "\n".join(lines)

    @staticmethod
    def require_qq(raw, usage):
        qq = raw.strip()
        if not qq:
            raise CommandReply(usage)
        if not qq.isdigit():
            raise CommandReply("QQ号需要填写纯数字")
        return qq

    async def remove_leaves(self, actor, raw):
        qq = self.require_qq(raw, "用法：/移除退群记录 <QQ号>")
        groups = await run_storage(LeaveRecordManager.remove_leave_record_from_all_groups, qq)
        if not groups:
            return f"未找到 QQ {qq} 的退群记录"
        return f"已移除 QQ {qq} 的退群记录，共影响 {len(groups)} 个群：" + "、".join(groups)

    async def clear_binding(self, actor, raw):
        qq = self.require_qq(raw, "用法：/解除绑定 <QQ号>")
        async with self.service.decisions.locks.hold(*identity_decision_keys(qq, None)):
            had = await run_storage(JoinRequestRecordManager.get_user_binding, qq)
            removed = await run_storage(JoinRequestRecordManager.clear_identity_binding, qq)
        if not removed and had is None:
            return f"未找到 QQ {qq} 的绑定记录"
        return f"已清除 QQ {qq} 的绑定信息，解除的 B站 UID：{'、'.join(map(str, removed)) if removed else '无当前归属 UID'}"

    async def check_qq(self, actor, raw):
        qq = self.require_qq(raw, "用法：/检查qq <QQ号>")
        binding, summary, audits = await run_storage(JoinRequestRecordManager.get_user_report, qq, 5)
        profiles = ([(binding.latest_bili_name, binding.latest_bili_uid)] if binding else [])
        profiles += [(audit.bili_name, audit.bili_uid) for audit in audits]
        uids = list(dict.fromkeys(uid for name, uid in profiles if needs_profile_name(name, uid)))
        resolved_names = {}
        cookie = self.config.get_bili_cookie()

        async def resolve_names():
            for uid in uids:
                profile = await self.service.bili_requests.run(cookie,
                    lambda api, uid=uid: api.resolve_user_profile(f"UID:{uid}"), cache_scope="report:profile")
                if profile.state == "passed" and profile.matched_target_uid == uid and profile.matched_name:
                    resolved_names[uid] = profile.matched_name

        if cookie and uids:
            try:
                await asyncio.wait_for(resolve_names(), timeout=8)
            except Exception:
                logger.warning("Unable to resolve historical UID display names; keeping the stored report")
        return format_user_check_reply(qq, binding, summary, audits, resolved_names=resolved_names)

    async def check_bili(self, actor, raw):
        args = split_arguments(raw)
        if len(args) < 2:
            raise CommandReply("用法：/检查bili <群号> <B站UID或昵称>")
        try:
            result = await self.service.simulate(args[0], " ".join(args[1:]))
        except ValueError as exc:
            raise CommandReply(str(exc)) from None
        return result["text"]

    async def pending(self, actor, raw):
        audits = await run_storage(JoinRequestRecordManager.get_pending_decisions, 20)
        if not audits:
            return "当前没有待确认审批"
        lines = ["【待确认审批】"]
        for audit in audits:
            lines.append(f"审计ID：{audit.id} | 时间：{audit.created_at} | 群：{audit.group_id} | QQ：{audit.qq} | 原计划：{'同意' if audit.result == 'approved' else '拒绝'}")
        return "\n".join([*lines, "", "核对 QQ 实际结果后使用 /确认审批 <审计ID> <applied|failed>。", "只暂停与未确认审批相关的身份和请求。"])

    async def resolve_pending(self, actor, raw):
        parts = raw.strip().split()
        if len(parts) != 2 or not parts[0].isdigit():
            raise CommandReply("用法：/确认审批 <审计ID> <applied|failed>")
        state = parts[1].lower()
        if state not in {"applied", "failed"}:
            raise CommandReply("确认结果只能是 applied 或 failed")
        result = await self.service.decisions.resolve_pending(int(parts[0]), applied=state == "applied")
        if not result.found:
            return f"审计 {parts[0]} 不存在或已经确认"
        if not result.finalized:
            return f"审计 {parts[0]} 确认失败，请检查 SQLite"
        return f"审计 {parts[0]} 已标记为 {state}；剩余待确认：{result.remaining}"
