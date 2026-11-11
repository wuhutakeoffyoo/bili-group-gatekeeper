"""Application services for requests, simulation, audit and administrator notices.

The service owns workflows; OneBot is an injected action port. No framework
matchers, framework startup hooks or module-global application instances exist.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from app.bilibili.client import ACCOUNT_INPUT_GUIDANCE, ACCOUNT_REQUIRED_MESSAGE, BiliApi, CheckResult
from app.bilibili.runtime import BiliRequestCoordinator
from app.bilibili.parser import mistaken_qq_input
from app.config import ConfigManager
from app.database.async_storage import run_storage
from app.database.leaves import LeaveRecordManager
from app.database.records import JoinRequestRecordManager
from app.gatekeeper.decisions import ScopedDecisionService
from app.onebot.events import OneBotEvent
from app.onebot.snowluma import add_request_identity, request_identity_keys
from app.settings import get_settings
from src.gatekeeper.conditions import (
    LEGACY_NICKNAME_NOT_FOUND_MESSAGE, build_condition_snapshot,
    build_rule_condition_statuses, requires_bili_identity,
)
from src.gatekeeper.config import GroupConfig, parse_bool
from src.gatekeeper.decision_runtime import DecisionLocks, identity_decision_keys
from src.gatekeeper.decisions import DecisionRequest, DefiniteActionFailure
from src.gatekeeper.formatters import (
    build_reject_reason, build_stage_reject_reasons, format_action_text,
    format_condition_details, format_flow_action_text, format_stage_results,
    format_uid_restriction_reason,
)
from src.gatekeeper.models import RuleConditionStatus, StageDispatchResult, StageExecutionResult, StageRuntime
from src.gatekeeper.parsing import extract_bili_name, parse_qq_level
from src.gatekeeper.pipeline import (
    collect_pipeline_conditions, get_review_stages, get_stage_flow_action,
    get_stage_outcome_actions, get_stage_outcome_target, iter_pipeline,
)

logger = logging.getLogger(__name__)


class SqliteDecisionStorage:
    async def add_audit(self, **kwargs):
        return await run_storage(JoinRequestRecordManager.add_audit, **kwargs)

    async def begin_pending_decision(self, **kwargs):
        return await run_storage(JoinRequestRecordManager.begin_pending_decision, **kwargs)

    async def finalize_pending_decision(self, audit_id, **kwargs):
        return await run_storage(JoinRequestRecordManager.finalize_pending_decision, audit_id, **kwargs)

    async def get_pending_decision_count(self):
        return await run_storage(JoinRequestRecordManager.get_pending_decision_count)

    async def get_pending_decisions(self, limit=20):
        return await run_storage(JoinRequestRecordManager.get_pending_decisions, limit)


@dataclass
class JoinRequestContext:
    group_id: str
    user_id: str
    group_config: GroupConfig
    bot: Any = None
    event: OneBotEvent | None = None
    dry_run: bool = False
    input_bili_name: str = ""
    actor_qq_for_binding: str = ""
    bili_name: str = ""
    bili_uid: int | None = None
    bili_level: int | None = None
    conflict_qq: str | None = None
    conflict_lookup_known: bool = False
    qq_level: int | None = None
    bili_search_result: CheckResult = field(default_factory=lambda: CheckResult("not_required"))
    follow_result: CheckResult = field(default_factory=lambda: CheckResult("not_required"))
    medal_result: CheckResult = field(default_factory=lambda: CheckResult("not_required"))
    leave_status: str = "not_required"
    leave_record: Any = None
    owned_other_bili_uids: list[int] = field(default_factory=list)
    identity_change_lookup_known: bool = False
    condition_statuses: dict[str, RuleConditionStatus] = field(default_factory=dict)
    condition_states: dict[str, str] = field(default_factory=dict)
    condition_reasons: dict[str, str] = field(default_factory=dict)
    stage_results: list[str] = field(default_factory=list)


def qq_level_hidden(payload: dict) -> bool:
    for key in ("isHideQQLevel", "is_hide_qq_level", "isHideLevel"):
        value = payload.get(key)
        if isinstance(value, (bool, int)):
            return bool(value)
        if isinstance(value, str):
            if value.strip().lower() in {"1", "true", "yes", "on"}:
                return True
            if value.strip().lower() in {"0", "false", "no", "off", ""}:
                return False
    return False


class GatekeeperService:
    def __init__(self, config=ConfigManager, *, settings=None, bili_requests=None,
                 decision_service=None, client_provider=None):
        self.config = config
        self.settings = settings or get_settings()
        self.bili_requests = bili_requests or BiliRequestCoordinator(max_concurrent=4, total_timeout=60.0)
        self.decisions = decision_service or ScopedDecisionService(SqliteDecisionStorage())
        self.decision_service = self.decisions
        self.client_provider = client_provider or (lambda: None)
        self.ownership = None
        self.patrol = None
        self.credentials = None
        self.request_locks = DecisionLocks()

    @staticmethod
    def ownership_required(group):
        policy = group.verification or {}
        mode = str(policy.get("mode", "off")).lower()
        return mode not in {"", "off", "none", "disabled"} and parse_bool(policy.get("enabled", True))

    def superadmins(self) -> set[str]:
        return {str(qq).strip() for qq in getattr(self.settings, "superusers", [])
                if str(qq).strip().isdigit()}

    def admins(self) -> set[str]:
        return self.superadmins() | {str(qq).strip() for qq in self.config.get_admin_qqs()
                                     if str(qq).strip().isdigit()}

    async def notify_admins(self, bot, message: str, include_normal_admins: bool = False) -> bool:
        targets = self.superadmins()
        if include_normal_admins:
            mode = self.config.get_notify_mode()
            if mode == "none":
                targets = set()
            elif mode == "all":
                targets = self.admins()
        if bot is None:
            logger.warning("Administrator notification deferred: OneBot unavailable")
            return False
        delivered = False
        for qq in sorted(targets):
            try:
                await bot.send_private_msg(user_id=int(qq), message=message)
                delivered = True
            except Exception:
                logger.warning("Unable to deliver administrator notification")
        return delivered

    async def fetch_qq_level(self, bot, user_id: str, event=None) -> int | None:
        info = None
        if bot:
            try:
                info = await bot.get_stranger_info(user_id=int(user_id), no_cache=True)
            except Exception:
                logger.debug("QQ level action unavailable", exc_info=True)
        payloads = [info, event.dict() if isinstance(event, OneBotEvent) else event]
        for payload in payloads:
            if isinstance(payload, dict) and not qq_level_hidden(payload):
                for key in ("level", "qq_level", "qqLevel"):
                    level = parse_qq_level(payload.get(key))
                    if level is not None:
                        return level
        return None

    async def populate_bili_context(self, context, conditions, cookie, *, lookup_bindings=True):
        async def populate(api: BiliApi):
            context.input_bili_name = context.input_bili_name or context.bili_name
            context.bili_search_result = await api.resolve_user_profile(context.input_bili_name or "")
            context.bili_uid = context.bili_search_result.matched_target_uid
            context.bili_level = context.bili_search_result.matched_level
            context.bili_name = context.bili_search_result.matched_name or ""
            if not context.bili_uid:
                return
            group = context.group_config
            if "follow" in conditions and group.target_uids:
                context.follow_result = await api.check_follow_any_status(
                    context.bili_uid, group.target_uids,
                    common_negative_is_definitive=self.bili_requests.common_negative_is_definitive(
                        cookie, group.target_uids))
            if "medal" in conditions and group.target_medal_uids:
                context.medal_result = await api.check_medal_any_status(
                    context.bili_uid, group.target_medal_uids, group.required_medal_level)

        await self.bili_requests.run(cookie, populate, cache_scope=f"group:{context.group_id}:policy:{self.config.get_version()}")
        if lookup_bindings:
            await self.populate_identity_bindings(context, conditions)

    async def populate_identity_bindings(self, context, conditions):
        if context.bili_uid is None or not context.actor_qq_for_binding:
            return
        if "no_conflict" in conditions:
            context.conflict_qq = await run_storage(
                JoinRequestRecordManager.get_conflict_qq, context.actor_qq_for_binding, context.bili_uid)
            context.conflict_lookup_known = True
        if "no_identity_change" in conditions:
            context.owned_other_bili_uids = [uid for uid in await run_storage(
                JoinRequestRecordManager.get_owned_bili_uids, context.actor_qq_for_binding)
                if uid != context.bili_uid]
            context.identity_change_lookup_known = True

    def condition_snapshot(self, context, conditions, *, identity_known=True, leave_known=True):
        context.condition_statuses = build_rule_condition_statuses(
            context.group_config, conditions, qq_level=context.qq_level,
            bili_search_result=context.bili_search_result, bili_level=context.bili_level,
            follow_result=context.follow_result, medal_result=context.medal_result,
            leave_record=context.leave_record, conflict_qq=context.conflict_qq,
            owned_other_bili_uids=context.owned_other_bili_uids,
            leave_record_known=leave_known, identity_binding_known=identity_known,
            conflict_lookup_known=context.conflict_lookup_known,
            identity_change_lookup_known=context.identity_change_lookup_known)
        context.condition_states, context.condition_reasons = build_condition_snapshot(context.condition_statuses)

    @staticmethod
    def reject_reasons(context, reasons, *, stage=None, outcome="false"):
        reasons = list(reasons)
        if stage:
            reasons = build_stage_reject_reasons(stage, outcome, context.condition_statuses, reasons)
        if context.bili_search_result.state == "failed":
            reasons = [reason for reason in reasons if reason != LEGACY_NICKNAME_NOT_FOUND_MESSAGE]
            detail = context.bili_search_result.detail
            if detail and not any(detail in reason for reason in reasons):
                reasons.append(detail)
            if not any(ACCOUNT_INPUT_GUIDANCE in reason for reason in reasons):
                reasons.append(ACCOUNT_INPUT_GUIDANCE)
        return reasons

    @staticmethod
    def audit_kwargs(context, *, result, reasons):
        return dict(group_id=context.group_id, qq=context.user_id, bili_name=context.bili_name,
                    bili_uid=context.bili_uid, result=result, reasons=reasons,
                    conflict_qq=context.conflict_qq, follow_status=context.follow_result.state,
                    medal_status=context.medal_result.state, leave_status=context.leave_status,
                    condition_states=context.condition_states, condition_reasons=context.condition_reasons,
                    stage_results=list(context.stage_results))

    def stage_notification(self, context, stage, outcome, dispatch, reasons):
        if dispatch.flow_action == "reject":
            reasons = self.reject_reasons(context, reasons, stage=stage, outcome=outcome)
        return (f"审核阶段命中通知\n群号：{context.group_id}\n申请 QQ：{context.user_id}"
                f"\n申请填写：{context.input_bili_name or '未填写'}"
                f"\nB站昵称：{context.bili_name or '未确认'}\nB站 UID：{context.bili_uid or '未记录'}"
                f"\n阶段：{stage.name}\n分组模式：{stage.mode}\n条件：{'、'.join(stage.conditions)}"
                f"\n结果：{dict(true='通过', false='未通过', unknown='无法判断').get(outcome, outcome)}"
                f"\n动作：{format_flow_action_text(dispatch.flow_action, dispatch.target_stage_index)}"
                f"\n原因：{'；'.join(reasons) or '无'}")

    async def run_pipeline(self, context, stages) -> StageExecutionResult:
        steps = iter_pipeline(stages, context.condition_statuses)
        dispatch = None
        while True:
            try:
                step = steps.send(dispatch)
            except StopIteration as done:
                return done.value
            context.stage_results.append(step.trace)
            dispatch = None
            if step.dispatch_actions:
                stage, outcome, reasons = step.execution.stage, step.execution.outcome, step.execution.reasons
                actions = get_stage_outcome_actions(stage, outcome)
                dispatch = StageDispatchResult(flow_action=get_stage_flow_action(actions),
                                                target_stage_index=get_stage_outcome_target(stage, outcome))
                if not context.dry_run:
                    if "notify_admin" in actions:
                        await self.notify_admins(context.bot, self.stage_notification(
                            context, stage, outcome, dispatch, reasons), include_normal_admins=True)
                    if "log_warn" in actions:
                        logger.warning("Review stage warning group=%s qq=%s stage=%s outcome=%s reasons=%s",
                                       context.group_id, context.user_id, stage.name, outcome, "；".join(reasons))

    async def finalize(self, context, stage, outcome, flow_action, reasons):
        if context.dry_run or context.bot is None or context.event is None:
            return
        if flow_action == "ignore":
            request = DecisionRequest(str(context.bot.self_id), context.group_id,
                                      str(context.event.sub_type), str(context.event.flag))
            audit = await run_storage(JoinRequestRecordManager.add_audit,
                                     request_key=request.request_key,
                                     **self.audit_kwargs(context, result="ignored", reasons=reasons or ["按阶段配置忽略"]))
            if audit is None:
                await self.notify_admins(context.bot, f"严重：QQ {context.user_id} 的忽略审计写入失败，请检查 SQLite 和磁盘空间。")
            return
        if flow_action not in {"approve", "reject"}:
            raise ValueError(f"未知终止动作：{flow_action}")
        approve = flow_action == "approve"
        if approve and self.ownership is not None:
            if await self.decisions.has_pending_identity(context.user_id, context.bili_uid):
                await self.notify_admins(context.bot,
                    f"QQ {context.user_id} 或该B站UID存在待确认审批，本次申请保持未处理；已完成的所有权验证不会被本次申请消耗。")
                return
            try:
                challenge = await self.ownership.before_decision(context)
            except Exception:
                logger.exception("Ownership verification unavailable; leaving QQ request untouched")
                await self.finalize_early(context, "ignore", ["B站账号所有权暂无法确认，请稍后重试或联系管理员"], "B站账号所有权验证")
                await self.notify_admins(context.bot, f"QQ {context.user_id} 的所有权验证发生异常，申请保持未处理，请检查日志。")
                return
            if challenge:
                if getattr(challenge, "hold", False):
                    if not getattr(challenge, "notify", True):
                        return
                    context.stage_results.append("B站账号所有权验证 | 等待扫码 | 原入群申请保持未处理")
                    audit = await run_storage(JoinRequestRecordManager.add_audit, **self.audit_kwargs(
                        context, result="ignored", reasons=["等待申请者完成B站扫码所有权验证，原申请保持未处理"]))
                    if audit is None:
                        await self.notify_admins(context.bot, f"QQ {context.user_id} 的扫码等待审计写入失败，未执行审批动作。")
                        return
                    try:
                        await context.bot.send_private_msg(user_id=int(context.user_id), message=str(challenge))
                    except DefiniteActionFailure:
                        # The applicant definitely didn't receive the private
                        # link; a rejection can deliver it and request reapply.
                        context.stage_results.append("所有权验证链接私聊发送明确失败，改为拒绝反馈后重新申请")
                    except Exception:
                        await self.notify_admins(context.bot,
                            f"QQ {context.user_id} 的扫码验证通知结果不明，原申请保持未处理，请人工核对；未重复发送链接。")
                        return
                    else:
                        return
                reasons = [challenge]
                approve = False
                stage = StageRuntime(index=0, name="B站账号所有权验证", conditions=[], mode="all_pass",
                                     on_true=["reject"], on_false=["reject"], on_unknown=["reject"])
                outcome = "false"
                context.stage_results.append(f"B站账号所有权验证 | 拒绝 | {challenge}")
        if not approve:
            reasons = self.reject_reasons(context, reasons, stage=stage, outcome=outcome)
        reasons = reasons or [f"通过阶段：{stage.name}" if approve else "未满足阶段规则"]
        params = {"flag": str(context.event.flag), "sub_type": str(context.event.sub_type), "approve": approve}
        if not approve:
            params["reason"] = build_reject_reason(reasons)
        request = DecisionRequest(bot_id=str(context.bot.self_id), group_id=context.group_id,
                                  sub_type=params["sub_type"], flag=params["flag"])

        async def perform_action():
            await context.bot.set_group_add_request(**params)

        async def notify(message):
            await self.notify_admins(context.bot, message)

        await self.decisions.execute(request, audit_kwargs=self.audit_kwargs(
            context, result="approved" if approve else "rejected", reasons=reasons),
            perform_action=perform_action, notify=notify, action_params=params)

    async def finalize_early(self, context, flow_action, reasons, stage_name):
        stage = StageRuntime(index=0, name=stage_name, conditions=[], mode="all_pass",
                             on_true=[flow_action], on_false=[flow_action], on_unknown=[flow_action])
        await self.finalize(context, stage, "unknown", flow_action, reasons)

    async def handle_group_request(self, bot, event):
        if isinstance(event, dict):
            event = OneBotEvent(event)
        if getattr(event, "sub_type", "") != "add":
            return
        request = DecisionRequest(str(bot.self_id), str(event.group_id), "add", str(getattr(event, "flag", "")))
        if not request.request_key:
            logger.warning("Ignoring group request without an exact request identity")
            return
        if request.flag.startswith("slreq:") and add_request_identity(request.flag, request.group_id) is None:
            logger.warning("SnowLuma join flag does not match its event group/type; leaving request untouched")
            return
        # Separate outer locks avoid nesting the decision service's identity locks.
        # Live events and startup catch-up share this entire evaluation boundary.
        request_keys = request_identity_keys(request)
        async with self.request_locks.hold(request_keys[-1]):
            for key in request_keys:
                audit = await run_storage(JoinRequestRecordManager.get_request_audit, key)
                if audit is not None:
                    if audit.qq != str(event.user_id):
                        logger.warning("Request identity conflicts with its durable applicant; leaving request untouched")
                    return
            await self._handle_group_request(bot, event)

    async def _handle_group_request(self, bot, event):
        group_id, user_id = str(event.group_id), str(event.user_id)
        group = self.config.get_group_config(group_id)
        if not group or not group.enabled:
            return
        context = JoinRequestContext(group_id, user_id, group, bot=bot, event=event,
                                     actor_qq_for_binding=user_id,
                                     input_bili_name=extract_bili_name(getattr(event, "comment", "") or ""))
        try:
            stages = get_review_stages(group)
        except ValueError as exc:
            await self.finalize_early(context, "ignore", [f"审核阶段配置无效：{exc}"], "审核配置检查")
            return
        conditions = collect_pipeline_conditions(stages)
        if "qq_level" in conditions:
            context.qq_level = await self.fetch_qq_level(bot, user_id, event)
        if "no_leave_record" in conditions:
            context.leave_record = await run_storage(LeaveRecordManager.get_leave_record, group_id, user_id)
            context.leave_status = "matched" if context.leave_record else "clear"
        if requires_bili_identity(conditions) or self.ownership_required(group) or group.custom_reject_uids:
            possible_qq = mistaken_qq_input(context.input_bili_name, user_id)
            if possible_qq:
                context.bili_search_result = possible_qq
            elif not context.input_bili_name:
                context.bili_search_result = CheckResult("failed", ACCOUNT_REQUIRED_MESSAGE)
            else:
                cookie = self.config.get_bili_cookie()
                if not cookie:
                    context.bili_search_result = CheckResult("inaccessible", "未配置可用的 B站 Cookie")
                else:
                    try:
                        await self.populate_bili_context(context, conditions, cookie, lookup_bindings=False)
                    except Exception:
                        logger.exception("Bilibili request workflow failed")
                        if context.bili_search_result.state == "not_required":
                            context.bili_search_result = CheckResult("inaccessible", "B站 API 流程异常")
            restriction = group.custom_reject_uids.get(str(context.bili_uid)) if context.bili_uid is not None else None
            if group.custom_reject_uids and context.bili_uid is None:
                failed_input = context.bili_search_result.state == "failed"
                detail = context.bili_search_result.detail or "B站账号暂无法确认，无法完成本群限制名单核验，请稍后重试或联系管理员"
                await self.finalize_early(context, "reject" if failed_input else "ignore", [detail], "限制名单账号确认")
                return
            if restriction:
                reason = format_uid_restriction_reason(restriction)
                context.stage_results.append(f"自定义拒绝名单 | UID {context.bili_uid} | 拒绝 | {reason}")
                await self.finalize_early(context, "reject", [reason], "自定义拒绝名单")
                return
        async with self.decisions.locks.hold(*identity_decision_keys(user_id, context.bili_uid)):
            try:
                await self.populate_identity_bindings(context, conditions)
            except Exception:
                logger.exception("Identity binding lookup failed; treating affected conditions as unknown")
            self.condition_snapshot(context, conditions)
            try:
                execution = await self.run_pipeline(context, stages)
            except ValueError as exc:
                await self.finalize_early(context, "ignore", [f"审核分组路由无效：{exc}"], "审核路由检查")
                return
            await self.finalize(context, execution.stage, execution.outcome,
                                execution.dispatch_result.flow_action, execution.reasons)

    async def handle_group_decrease(self, bot, event):
        if isinstance(event, dict):
            event = OneBotEvent(event)
        if getattr(event, "notice_type", "") != "group_decrease":
            return
        group_id, user_id = str(event.group_id), str(event.user_id)
        # Keep history while a configured group's review is temporarily disabled.
        if self.config.get_group_config(group_id) is None:
            return
        self_id = getattr(event, "self_id", None) or getattr(bot, "self_id", "")
        if user_id == str(self_id):
            return
        nickname = f"用户{user_id}"
        try:
            info = await bot.get_group_member_info(group_id=int(group_id), user_id=int(user_id), no_cache=True)
            nickname = info.get("card") or info.get("nickname") or nickname
        except Exception:
            logger.debug("Departed member lookup unavailable", exc_info=True)
        await run_storage(LeaveRecordManager.add_leave_record, group_id, user_id, nickname)

    async def simulate(self, group_id, answer, qq=None):
        group_id = str(group_id)
        group = self.config.get_group_config(group_id)
        if not group:
            raise ValueError(f"群 {group_id} 未配置")
        if not group.enabled:
            raise ValueError(f"群 {group_id} 审核功能已关闭")
        stages = get_review_stages(group)
        conditions = collect_pipeline_conditions(stages)
        context = JoinRequestContext(group_id, str(qq or ""), group, dry_run=True,
                                     input_bili_name=str(answer).strip(),
                                     actor_qq_for_binding=str(qq or ""))
        if qq:
            if "qq_level" in conditions:
                context.qq_level = await self.fetch_qq_level(self.client_provider(), str(qq))
            if "no_leave_record" in conditions:
                context.leave_record = await run_storage(LeaveRecordManager.get_leave_record, group_id, str(qq))
                context.leave_status = "matched" if context.leave_record else "clear"
        if requires_bili_identity(conditions) or self.ownership_required(group) or group.custom_reject_uids:
            possible_qq = mistaken_qq_input(context.input_bili_name, str(qq) if qq else None)
            if possible_qq:
                context.bili_search_result = possible_qq
            elif not context.input_bili_name:
                context.bili_search_result = CheckResult("failed", ACCOUNT_REQUIRED_MESSAGE)
            elif not self.config.get_bili_cookie():
                context.bili_search_result = CheckResult("inaccessible", "未配置可用的 B站 Cookie")
            else:
                try:
                    await self.populate_bili_context(context, conditions, self.config.get_bili_cookie(), lookup_bindings=False)
                except Exception:
                    logger.exception("Bilibili simulation workflow failed")
                    if context.bili_search_result.state == "not_required":
                        context.bili_search_result = CheckResult("inaccessible", "B站 API 流程异常")
        async with self.decisions.locks.hold(*identity_decision_keys(str(qq or ""), context.bili_uid)):
            if qq:
                try:
                    await self.populate_identity_bindings(context, conditions)
                except Exception:
                    logger.exception("Simulation binding lookup failed")
            self.condition_snapshot(context, conditions, identity_known=bool(qq), leave_known=bool(qq))
        restriction = group.custom_reject_uids.get(str(context.bili_uid)) if context.bili_uid is not None else None
        if group.custom_reject_uids and context.bili_uid is None:
            failed_input = context.bili_search_result.state == "failed"
            action, reasons, outcome = ("reject" if failed_input else "ignore"), self.reject_reasons(
                context, [context.bili_search_result.detail or "B站账号暂无法确认，无法完成本群限制名单核验"]), "false" if failed_input else "unknown"
        elif restriction:
            action, reasons, outcome = "reject", [format_uid_restriction_reason(restriction)], "false"
        else:
            execution = await self.run_pipeline(context, stages)
            action, reasons, outcome = execution.dispatch_result.flow_action, execution.reasons, execution.outcome
            if action == "reject":
                reasons = self.reject_reasons(context, reasons, stage=execution.stage, outcome=outcome)
        incomplete = any(status.state == "unknown" for status in context.condition_statuses.values())
        lines = ["【B站审核测试】", f"群号：{group_id}", f"B站输入：{answer}"]
        if context.bili_uid:
            lines.append(f"B站 UID：{context.bili_uid}")
            lines.append(f"B站昵称：{context.bili_name or '未获取到昵称'}")
        if context.bili_level is not None:
            lines.append(f"B站等级：{context.bili_level}")
        lines += ["", "【条件结果】", *format_condition_details(context.condition_states, context.condition_reasons),
                  "", "【流程详情】", *format_stage_results(context.stage_results), "", "【最终结果】",
                  f"{'模拟动作' if incomplete else '最终动作'}：{format_action_text(action)}",
                  f"最终原因：{'；'.join(reasons) or '无'}"]
        if not qq:
            lines += ["说明：本命令未提供 QQ。", "QQ等级、退群记录和绑定关系会按‘无法判断’参与流程。"]
        ownership_required = self.ownership_required(group)
        if ownership_required:
            lines.append("说明：本次仅模拟资格规则；所有权验证不会在模拟中创建、消费或确认，实际申请仍须完成本群所有权验证。")
        return {"group_id": group_id, "qq": str(qq) if qq else None, "answer": str(answer),
                "bili_uid": context.bili_uid, "bili_name": context.bili_name, "bili_level": context.bili_level, "action": action,
                "outcome": outcome, "reasons": reasons, "condition_states": context.condition_states,
                "condition_reasons": context.condition_reasons, "stage_results": context.stage_results,
                "incomplete": incomplete, "ownership_verification": "not_simulated" if ownership_required else "disabled",
                "text": "\n".join(lines)}

    async def ensure_login_account_follows_targets(self, cookie):
        targets = list(dict.fromkeys(uid for group in self.config.get_config().groups.values() for uid in group.target_uids))
        self.bili_requests.set_verified_common_targets(cookie, set())
        if not targets:
            return "未配置关注目标 UID，无需自动关注"
        try:
            results = await self.bili_requests.run(cookie, lambda api: api.ensure_following_targets(targets))
        except Exception:
            logger.exception("Login account target check failed")
            return "目标主播关注检查失败，请查看日志"
        self.bili_requests.set_verified_common_targets(cookie, {uid for uid, result in results.items() if result.state == "passed"})
        existing = sum(result.state == "passed" and result.detail == "已关注" for result in results.values())
        added = sum(result.state == "passed" and result.detail == "已自动关注" for result in results.values())
        return f"目标主播关注检查完成：已关注 {existing}，自动关注 {added}，失败 {len(results) - existing - added}"

    async def close(self):
        await self.bili_requests.close()
