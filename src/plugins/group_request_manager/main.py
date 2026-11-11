"""
插件主逻辑模块。

负责处理 QQ 群的加群申请、退群记录以及相关命令。
通过 OneBot V11 适配器接收事件，调用 B站 API 进行验证。
"""

import asyncio
import itertools
import os
import shlex
import shutil
from dataclasses import dataclass, field

from nonebot import get_driver, on_command, on_notice, on_request
from nonebot.log import logger
from nonebot.adapters.onebot.v11 import (
    Bot,
    GroupDecreaseNoticeEvent,
    GroupRequestEvent,
    Message,
    MessageEvent,
)
from nonebot.adapters.onebot.v11.exception import ActionFailed
from nonebot.params import CommandArg

from src.gatekeeper.conditions import (
    LEGACY_NICKNAME_NOT_FOUND_MESSAGE,
    build_condition_snapshot as _build_condition_snapshot,
    build_rule_condition_statuses as _build_rule_condition_statuses,
    requires_bili_identity as _requires_bili_identity,
)
from src.gatekeeper.decisions import DecisionRequest, DecisionService, DefiniteActionFailure
from src.gatekeeper.formatters import (
    build_reject_reason as _build_reject_reason,
    build_stage_reject_reasons,
    format_action_text as _format_action_text,
    format_flow_action_text as _format_flow_action_text,
    format_group_config as _format_group_config,
    format_stage_pipeline_text as _format_stage_pipeline_text,  # noqa: F401 - compatibility export
    format_uid_restriction_reason,
)
from src.gatekeeper.models import (
    RuleConditionStatus,
    StageDispatchResult,
    StageExecutionResult,
    StageRuntime,
)
from src.gatekeeper.parsing import (
    extract_bili_name as _extract_bili_name,
    parse_qq_level as _parse_qq_level,
)
from src.gatekeeper.pipeline import (
    collect_pipeline_conditions as _collect_pipeline_conditions,
    get_review_stages as _get_review_stages,
    get_stage_flow_action as _get_stage_flow_action,
    get_stage_outcome_actions as _get_stage_outcome_actions,
    get_stage_outcome_target as _get_stage_outcome_target,
    iter_pipeline,
)

from .bili_api import ACCOUNT_INPUT_GUIDANCE, ACCOUNT_REQUIRED_MESSAGE, BiliApi, CheckResult
from .bili_runtime import BiliRequestCoordinator
from .async_storage import run_storage
from .config import (
    ConfigManager,
    GroupConfig,
    parse_bool,
    parse_reject_reasons,
    parse_uid_list,
)
from .cookie_manager import CookieManager
from . import cookie_monitor, email_notifier
from .database import initialize_database
from .decision_adapter import SqliteDecisionStorage, configure_decision_logging
from .decision_runtime import identity_decision_keys
from .formatters import (
    format_condition_details as _format_condition_details,
    format_stage_results as _format_stage_results,
)
from .leave_record import LeaveRecordManager
from .request_record import JoinRequestRecordManager
from .user_report import format_user_check_reply
from .webui import start_webui

# ---------------------------------------------------------------------------
# 事件与命令注册
# ---------------------------------------------------------------------------


def _command_case_variants(command: str) -> set[str]:
    """生成命令字符串中英文字母的所有大小写变体。

    非英文字母字符保持不变。用于实现命令名大小写不敏感匹配。
    """
    letter_positions = [i for i, c in enumerate(command) if c.isascii() and c.isalpha()]
    if not letter_positions:
        return {command}

    base = list(command)
    variants: set[str] = set()
    for case_combo in itertools.product("aA", repeat=len(letter_positions)):
        for idx, pos in enumerate(letter_positions):
            base[pos] = base[pos].lower() if case_combo[idx] == "a" else base[pos].upper()
        variants.add("".join(base))
    return variants


def _on_case_insensitive_command(
    cmd: str, aliases: set[str] | None = None, **kwargs
):
    """注册命令响应器，使命令名中英文字母大小写不敏感。"""
    variants = _command_case_variants(cmd)
    for alias in aliases or set():
        variants |= _command_case_variants(alias)
    variant_list = sorted(variants)
    primary = variant_list[0]
    rest = set(variant_list[1:])
    return on_command(primary, aliases=rest, **kwargs)


group_request_handler = on_request()
group_decrease_handler = on_notice()

# 所有命令都允许被触发，但只有 superadmin / 手动指定 admin 的私聊才会实际响应
set_group_config = _on_case_insensitive_command("设置群")
set_cookie = _on_case_insensitive_command("设置cookie")
fetch_cookie_cloud = _on_case_insensitive_command("获取cookie")
show_config = _on_case_insensitive_command("查看配置")
remove_leave_record = _on_case_insensitive_command("移除退群记录")
clear_identity_binding = _on_case_insensitive_command("解除绑定")
make_login_qrcode = _on_case_insensitive_command("登录二维码")
check_qq = _on_case_insensitive_command("检查qq")
check_bili = _on_case_insensitive_command("检查bili")
show_pending_decisions = _on_case_insensitive_command("待确认审批")
resolve_pending_decision = _on_case_insensitive_command("确认审批")
show_help = _on_case_insensitive_command("help", aliases={"帮助"})
driver = get_driver()
_disk_monitor_task: asyncio.Task | None = None
_cookie_monitor_task: asyncio.Task | None = None
_webui_server = None
_bili_requests = BiliRequestCoordinator(max_concurrent=4, total_timeout=60.0)
configure_decision_logging()
_decision_service = DecisionService(SqliteDecisionStorage())
_disk_alert_email_state = {
    "current_level": None,
    "email_sent_for_current_alert": False,
}


@dataclass
class JoinRequestContext:
    """一次审核请求在整个管线中的上下文。"""

    group_id: str
    user_id: str
    group_config: GroupConfig
    bot: Bot | None = None
    event: GroupRequestEvent | None = None
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
    leave_record: object = None
    owned_other_bili_uids: list[int] = field(default_factory=list)
    identity_change_lookup_known: bool = False
    condition_statuses: dict[str, RuleConditionStatus] = field(default_factory=dict)
    condition_states: dict[str, str] = field(default_factory=dict)
    condition_reasons: dict[str, str] = field(default_factory=dict)
    stage_results: list[str] = field(default_factory=list)


def _is_valid_qq(value: object) -> bool:
    """判断配置值是否是可用于 OneBot 的纯数字 QQ 号。"""
    text = str(value).strip()
    return text.isdigit()


ACTION_HANDLERS: dict[str, object] = {}


def register_action(name: str):
    """注册阶段动作处理器。"""

    def decorator(func):
        ACTION_HANDLERS[name] = func
        return func

    return decorator


@register_action("next")
async def _action_next(
    context: JoinRequestContext,
    stage: StageRuntime,
    outcome: str,
    reasons: list[str],
    result: StageDispatchResult,
) -> None:
    result.flow_action = "next"


@register_action("approve")
async def _action_approve(
    context: JoinRequestContext,
    stage: StageRuntime,
    outcome: str,
    reasons: list[str],
    result: StageDispatchResult,
) -> None:
    result.flow_action = "approve"


@register_action("reject")
async def _action_reject(
    context: JoinRequestContext,
    stage: StageRuntime,
    outcome: str,
    reasons: list[str],
    result: StageDispatchResult,
) -> None:
    result.flow_action = "reject"


@register_action("ignore")
async def _action_ignore(
    context: JoinRequestContext,
    stage: StageRuntime,
    outcome: str,
    reasons: list[str],
    result: StageDispatchResult,
) -> None:
    result.flow_action = "ignore"


@register_action("notify_admin")
async def _action_notify_admin(
    context: JoinRequestContext,
    stage: StageRuntime,
    outcome: str,
    reasons: list[str],
    result: StageDispatchResult,
) -> None:
    if context.dry_run or context.bot is None:
        return
    await _notify_admins(
        context.bot,
        _build_stage_notify_message(
            context,
            stage,
            outcome,
            result.flow_action,
            reasons,
            target_stage_index=result.target_stage_index,
        ),
        include_normal_admins=True,
    )


@register_action("log_warn")
async def _action_log_warn(
    context: JoinRequestContext,
    stage: StageRuntime,
    outcome: str,
    reasons: list[str],
    result: StageDispatchResult,
) -> None:
    if context.dry_run:
        return
    logger.warning(
        "审核阶段命中告警：group=%s qq=%s stage=%s outcome=%s reasons=%s",
        context.group_id,
        context.user_id,
        stage.name or f"阶段{stage.index}",
        outcome,
        "；".join(reasons) or "无",
    )


async def _dispatch_stage_actions(
    context: JoinRequestContext,
    stage: StageRuntime,
    outcome: str,
    reasons: list[str],
) -> StageDispatchResult:
    """执行某个阶段结果对应的动作列表。"""
    actions = _get_stage_outcome_actions(stage, outcome)
    result = StageDispatchResult(
        flow_action=_get_stage_flow_action(actions),
        target_stage_index=_get_stage_outcome_target(stage, outcome),
    )
    for action in actions:
        handler = ACTION_HANDLERS.get(action)
        if handler is None:
            raise ValueError(f"未注册的阶段动作：{action}")
        await handler(context, stage, outcome, reasons, result)
    return result


async def _run_stage_pipeline(
    context: JoinRequestContext,
    stages: list[StageRuntime],
) -> StageExecutionResult:
    """执行纯规则核心生成的步骤，并按原有顺序发送阶段副作用。"""
    steps = iter_pipeline(stages, context.condition_statuses)
    dispatch = None
    while True:
        try:
            step = steps.send(dispatch)
        except StopIteration as completed:
            return completed.value
        context.stage_results.append(step.trace)
        dispatch = None
        if step.dispatch_actions:
            execution = step.execution
            dispatch = await _dispatch_stage_actions(
                context, execution.stage, execution.outcome, execution.reasons
            )


def _build_audit_kwargs_from_context(
    context: JoinRequestContext,
    *,
    result: str,
    reasons: list[str],
) -> dict:
    """从请求上下文构造审计写入参数。"""
    return {
        "group_id": context.group_id,
        "qq": context.user_id,
        "bili_name": context.bili_name,
        "bili_uid": context.bili_uid,
        "result": result,
        "reasons": reasons,
        "conflict_qq": context.conflict_qq,
        "follow_status": context.follow_result.state,
        "medal_status": context.medal_result.state,
        "leave_status": context.leave_status,
        "condition_states": context.condition_states,
        "condition_reasons": context.condition_reasons,
        "stage_results": list(context.stage_results),
    }


def _build_stage_notify_message(
    context: JoinRequestContext,
    stage: StageRuntime,
    outcome: str,
    flow_action: str,
    reasons: list[str],
    *,
    target_stage_index: int | None = None,
) -> str:
    """生成阶段命中后的管理员通知文本。"""
    if flow_action == "reject":
        reasons = _request_reject_reasons(context, reasons, stage=stage, outcome=outcome)
    outcome_text = {"true": "通过", "false": "未通过", "unknown": "无法判断"}.get(outcome, outcome)
    flow_text = _format_flow_action_text(flow_action, target_stage_index)
    return (
        f"审核阶段命中通知\n"
        f"群号：{context.group_id}\n"
        f"申请 QQ：{context.user_id}\n"
        f"B站昵称：{context.bili_name or '未填写'}\n"
        f"B站 UID：{context.bili_uid or '未记录'}\n"
        f"阶段：{stage.name}\n"
        f"分组模式：{stage.mode}\n"
        f"条件：{'、'.join(stage.conditions)}\n"
        f"结果：{outcome_text}\n"
        f"动作：{flow_text}\n"
        f"原因：{'；'.join(reasons) or '无'}"
    )


async def _finalize_stage_decision(
    context: JoinRequestContext,
    stage: StageRuntime,
    outcome: str,
    flow_action: str,
    reasons: list[str],
) -> None:
    """将阶段决策落地为实际审批动作与审计。"""
    if context.bot is None or context.event is None:
        return

    if flow_action == "approve":
        success_reasons = reasons or [f"通过阶段：{stage.name}"]
        if not await _set_group_request_with_durable_audit(
            context.bot,
            context.event,
            approve=True,
            audit_kwargs=_build_audit_kwargs_from_context(
                context,
                result="approved",
                reasons=success_reasons,
            ),
        ):
            return
    elif flow_action == "reject":
        reasons = _request_reject_reasons(context, reasons, stage=stage, outcome=outcome)
        reject_reason = _build_reject_reason(reasons)
        if not await _set_group_request_with_durable_audit(
            context.bot,
            context.event,
            approve=False,
            reason=reject_reason,
            audit_kwargs=_build_audit_kwargs_from_context(
                context,
                result="rejected",
                reasons=reasons or ["未满足阶段规则"],
            ),
        ):
            return
    elif flow_action == "ignore":
        audit = await run_storage(
            JoinRequestRecordManager.add_audit,
            **_build_audit_kwargs_from_context(context, result="ignored", reasons=reasons or ["按阶段配置忽略"])
        )
        if audit is None:
            await _notify_admins(
                context.bot,
                f"严重：QQ {context.user_id} 的忽略审计未能写入 SQLite，请立即检查数据库和磁盘空间。",
            )



def _request_reject_reasons(
    context: JoinRequestContext, reasons: list[str], *, stage: StageRuntime | None = None, outcome: str = "false"
) -> list[str]:
    """Include actual failed checks and correction guidance in every rejection."""
    if stage is not None:
        reasons = build_stage_reject_reasons(stage, outcome, context.condition_statuses, reasons)
    if context.bili_search_result.state == "failed":
        reasons = [reason for reason in reasons if reason != LEGACY_NICKNAME_NOT_FOUND_MESSAGE]
        detail = context.bili_search_result.detail
        if detail and not any(detail in reason for reason in reasons):
            reasons = [*reasons, detail]
        if not any(ACCOUNT_INPUT_GUIDANCE in reason for reason in reasons):
            reasons = [*reasons, ACCOUNT_INPUT_GUIDANCE]
    return reasons


async def _finalize_early_decision(
    context: JoinRequestContext,
    *,
    flow_action: str,
    reasons: list[str],
    stage_name: str,
) -> None:
    """将预检阶段产生的终止结果直接落地。"""
    stage = StageRuntime(
        index=0,
        name=stage_name,
        conditions=[],
        mode="all_pass",
        on_true=[flow_action],
        on_false=[flow_action],
        on_unknown=[flow_action],
    )
    await _finalize_stage_decision(context, stage, "unknown", flow_action, reasons)


def _get_superadmin_qqs() -> set[str]:
    """读取 NoneBot SUPERUSERS 配置。"""
    driver_config = get_driver().config
    return {
        str(user_id).strip()
        for user_id in getattr(driver_config, "superusers", set())
        if _is_valid_qq(user_id)
    }


def _get_normal_admin_qqs() -> set[str]:
    """获取手动配置的普通管理员 QQ 列表。"""
    return {
        str(user_id).strip()
        for user_id in ConfigManager.get_admin_qqs()
        if _is_valid_qq(user_id)
    }


def _get_allowed_admin_qqs() -> set[str]:
    """获取允许使用管理命令的 QQ 列表。"""
    return _get_superadmin_qqs() | _get_normal_admin_qqs()


def _is_private_admin_event(event: MessageEvent) -> bool:
    """判断当前命令事件是否来自管理员私聊。"""
    return (
        getattr(event, "message_type", "") == "private"
        and str(event.user_id) in _get_allowed_admin_qqs()
    )


def _is_private_superadmin_event(event: MessageEvent) -> bool:
    """判断当前命令事件是否来自超级管理员私聊。"""
    return (
        getattr(event, "message_type", "") == "private"
        and str(event.user_id) in _get_superadmin_qqs()
    )


def _get_cookiecloud_config() -> tuple[str, str, str]:
    """统一读取 CookieCloud 配置。"""
    driver_config = get_driver().config
    host = getattr(driver_config, "cookiecloud_host", os.getenv("COOKIECLOUD_HOST", ""))
    uuid = getattr(driver_config, "cookiecloud_uuid", os.getenv("COOKIECLOUD_UUID", ""))
    key = getattr(driver_config, "cookiecloud_key", os.getenv("COOKIECLOUD_KEY", ""))
    return str(host).strip(), str(uuid).strip(), str(key).strip()


def _mark_bili_flow_exception(context: JoinRequestContext, detail: str = "B站 API 流程异常") -> None:
    """标记 B站流程异常，但保留异常前已经成功拿到的搜索结果。"""
    if context.bili_search_result.state == "not_required":
        context.bili_search_result = CheckResult("inaccessible", detail)


def _is_qq_level_hidden(payload: dict) -> bool:
    """判断 OneBot 响应是否明确标记 QQ 等级不可见。"""
    for key in ("isHideQQLevel", "is_hide_qq_level", "isHideLevel"):
        value = payload.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return value != 0
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on"}:
                return True
            if normalized in {"0", "false", "no", "off", ""}:
                return False
    return False


async def _fetch_qq_level(bot: Bot, user_id: str, event: GroupRequestEvent | None = None) -> int | None:
    """尝试读取申请者 QQ 等级，读取失败时返回 None。

    获取顺序：
    1. 通过 OneBot get_stranger_info 接口获取（主要来源）
    2. 回退到加群请求事件的原始数据（部分用户设置了等级不可见时，
       get_stranger_info 拿不到等级，但 QQ 协议在加群通知中会携带等级；
       NapCat 可能将其作为扩展字段放在请求事件内）
    """
    # 方式 1：get_stranger_info
    try:
        stranger_info = await bot.get_stranger_info(user_id=int(user_id), no_cache=True)
    except Exception:
        stranger_info = None

    if isinstance(stranger_info, dict) and not _is_qq_level_hidden(stranger_info):
        for key in ("level", "qq_level", "qqLevel"):
            level = _parse_qq_level(stranger_info.get(key))
            if level is not None:
                return level

    # 方式 2：加群请求事件的扩展字段（规避等级不可见）
    if event is not None:
        try:
            event_data = event.model_dump()
        except Exception:
            event_data = {}
        if not _is_qq_level_hidden(event_data):
            for key in ("level", "qq_level", "qqLevel"):
                level = _parse_qq_level(event_data.get(key))
                if level is not None:
                    return level

    return None


def _get_abnormal_notify_qqs() -> set[str]:
    """获取异常申请时应接收私聊汇报的管理员 QQ 列表。

    根据 NOTIFY_ADMINS_ON_ABNORMAL_REQUEST 配置：
    - all：通知 superusers + admin_qqs
    - superusers_only：仅通知 superusers
    - none：不通知任何人
    """
    mode = ConfigManager.get_notify_mode()
    if mode == "none":
        return set()
    notify_targets = set(_get_superadmin_qqs())
    if mode == "all":
        notify_targets.update(_get_normal_admin_qqs())
    return {qq for qq in notify_targets if _is_valid_qq(qq)}


async def _notify_admins(bot: Bot, message: str, include_normal_admins: bool = False) -> None:
    """将关键信息私聊汇报给管理员。"""
    target_qqs = _get_superadmin_qqs()
    if include_normal_admins:
        target_qqs = _get_abnormal_notify_qqs()
    for admin_qq in target_qqs:
        try:
            await bot.send_private_msg(user_id=int(admin_qq), message=message)
        except Exception:
            continue


async def _set_group_request_with_durable_audit(
    bot: Bot,
    event: GroupRequestEvent,
    *,
    approve: bool,
    reason: str = "",
    audit_kwargs: dict,
) -> bool:
    """把 OneBot 请求和明确失败转换为独立审批服务的输入。"""
    request = DecisionRequest(
        bot_id=str(getattr(bot, "self_id", "")),
        group_id=str(audit_kwargs.get("group_id", "")),
        sub_type=str(event.sub_type),
        flag=str(event.flag),
    )

    async def perform_action() -> None:
        kwargs = {"flag": event.flag, "sub_type": event.sub_type, "approve": approve}
        if reason:
            kwargs["reason"] = reason
        try:
            await bot.set_group_add_request(**kwargs)
        except ActionFailed as exc:
            raise DefiniteActionFailure(type(exc).__name__) from exc

    async def notify(message: str) -> None:
        await _notify_admins(bot, message)

    return await _decision_service.execute(
        request,
        audit_kwargs=audit_kwargs,
        perform_action=perform_action,
        notify=notify,
    )


def _reset_disk_alert_email_state() -> None:
    """重置磁盘告警邮件去重状态（用于测试）。"""
    _disk_alert_email_state["current_level"] = None
    _disk_alert_email_state["email_sent_for_current_alert"] = False


def _mark_disk_space_ok() -> None:
    """磁盘空间恢复正常后，允许未来再次发送告警邮件。"""
    _reset_disk_alert_email_state()


def _should_attempt_disk_alert_email(level: str) -> bool:
    """同一级别磁盘告警成功发送一次；失败则下轮重试。"""
    if _disk_alert_email_state["current_level"] != level:
        _disk_alert_email_state["current_level"] = level
        _disk_alert_email_state["email_sent_for_current_alert"] = False
        return True
    return not _disk_alert_email_state["email_sent_for_current_alert"]


def _mark_disk_alert_email_sent(level: str) -> None:
    """标记当前磁盘告警级别的邮件已成功发送。"""
    if _disk_alert_email_state["current_level"] == level:
        _disk_alert_email_state["email_sent_for_current_alert"] = True


async def _notify_disk_space_by_email(
    *,
    level: str,
    message: str,
    free_gb: float | None,
    monitored_path: str,
) -> None:
    """通过邮件发送磁盘空间异常告警。"""
    if not _should_attempt_disk_alert_email(level):
        return

    subject = f"【Bilibili QQ Group Gatekeeper 磁盘告警】{level}"
    free_space_text = f"{free_gb:.2f} GB" if free_gb is not None else "无法获取"
    body = (
        "项目检测到日志目录所在磁盘空间异常。\n\n"
        f"告警级别：{level}\n"
        f"剩余空间：{free_space_text}\n"
        f"监控路径：{monitored_path}\n"
        f"告警内容：{message}\n\n"
        "当前策略：仅通知，不自动清理本地日志或运行数据。\n"
    )
    sent = await email_notifier.send_notification(subject, body)
    if sent:
        _mark_disk_alert_email_sent(level)
        logger.info(f"磁盘告警邮件已发送: {level}")
    else:
        logger.warning("磁盘告警邮件发送失败，将在下次检查时重试")


async def _check_disk_space_once() -> None:
    """检查磁盘空间并告警；本任务不会删除任何本地文件。"""
    warn_threshold_gb = 1
    critical_threshold_gb = 0.2
    log_directory = getattr(get_driver().config, "local_log_directory", "logs") or "logs"
    monitored_path = os.path.abspath(str(log_directory))

    try:
        usage = shutil.disk_usage(monitored_path)
    except Exception as exc:
        message = f"[磁盘告警-检查失败] 无法读取日志目录所在磁盘空间：{type(exc).__name__}"
        logger.warning("磁盘空间检查失败", exc_info=True)
        await _notify_disk_space_by_email(
            level="检查失败",
            message=message,
            free_gb=None,
            monitored_path=monitored_path,
        )
        return

    free_gb = usage.free / (1024**3)
    if free_gb < critical_threshold_gb:
        level = "严重"
        message = (
            f"[磁盘告警-严重] 日志目录所在磁盘剩余空间仅 {free_gb:.2f} GB，"
            "可能存在写入失败风险，请尽快清理。"
        )
    elif free_gb < warn_threshold_gb:
        level = "警告"
        message = (
            f"[磁盘告警-警告] 日志目录所在磁盘剩余空间仅 {free_gb:.2f} GB，"
            "建议尽快关注。"
        )
    else:
        _mark_disk_space_ok()
        return

    logger.warning(message)
    await _notify_disk_space_by_email(
        level=level,
        message=message,
        free_gb=free_gb,
        monitored_path=monitored_path,
    )

    try:
        from nonebot import get_bots

        bots = get_bots()
        if not bots:
            return
        bot = next(iter(bots.values()))
    except Exception:
        logger.warning("获取 Bot 实例失败，无法发送磁盘告警私聊", exc_info=True)
        return

    for admin_qq in _get_superadmin_qqs():
        try:
            await bot.send_private_msg(user_id=int(admin_qq), message=message)
        except Exception:
            continue


async def _disk_space_monitor() -> None:
    """启动后立即检查，之后每 6 小时告警；不执行自动清理。"""
    check_interval_seconds = 6 * 3600


    try:
        await _check_disk_space_once()
    except Exception:
        logger.exception("磁盘首次检查异常，将在下次周期重试")

    while True:
        await asyncio.sleep(check_interval_seconds)
        try:
            await _check_disk_space_once()
        except Exception:
            logger.exception("磁盘检查循环异常，将在下次周期重试")


async def _format_user_check_reply(qq: str) -> str:
    """Load a bounded user summary without blocking the event loop."""
    binding, summary, audits = await run_storage(
        JoinRequestRecordManager.get_user_report, qq, 5
    )
    return format_user_check_reply(qq, binding, summary, audits)


def _format_help_text() -> str:
    """生成管理员私聊可用的帮助文本。"""
    return "\n".join(
        [
            "━━━━ 可用指令 ━━━━",
            "",
            "▎常规命令",
            "  /help                        查看本帮助",
            "  /查看配置 [<群号>]           查看全局或指定群的配置",
            "  /解除绑定 <QQ号>             清除该 QQ 当前的B站绑定归属",
            "  /检查qq <QQ号>               查看该 QQ 的申请与绑定记录",
            "  /检查bili <群号> <B站UID或昵称>   模拟B站侧审核，不写申请记录",
            "",
            "▎超级管理员专属",
            "  /设置群 <群号> [<参数> <值> ...] 配置加群审核参数",
            "  /移除退群记录 <QQ号>         删除该 QQ 在所有群的退群记录",
            "  /设置cookie <B站Cookie>      手动设置 B 站 Cookie",
            "  /获取cookie                  从已配置的 CookieCloud 同步 Cookie（可选）",
            "  /登录二维码                  生成 B 站扫码登录二维码",
            "  /待确认审批                  查看尚未确认的审批",
            "  /确认审批 <审计ID> <applied|failed> 人工确认审批结果",
            "",
            "━━━━ 设置群示例 ━━━━",
            "  /设置群 123456789 enable on",
            "  /设置群 123456789 required_qq_level 32",
            "  /设置群 123456789 target_uids 200000003",
            "  /设置群 123456789 target_medal_uids 200000003 required_medal_level 10",
            "  # 复杂配置请优先通过 .env.prod 配置 GROUP_CONFIG_FILE，再在 groups.yaml 中指定 review_pipeline_file",
            "",
            "━━━━ 常用参数 ━━━━",
            "  enable                       开启/关闭整个群审核 (on/off)",
            "  required_qq_level            QQ 最低等级要求 (数字)",
            "  target_uids                  需要关注的 B站 UID，逗号分隔",
            "  target_medal_uids            需要检查的主播 UID，逗号分隔",
            "  required_medal_level         粉丝牌最低等级 (数字)",
            "  reject_reason_with_leave_time 拒绝时是否附带退群时间 (on/off)",
            "  自定义拒绝名单 custom_reject_uids 请在群 YAML 或环境配置中设置，不是 /设置群 参数",
            "",
            "━━━━ 说明 ━━━━",
            "  1. 所有命令仅对管理员私聊响应，群聊中不回复",
            "  2. on/off、true/false、1/0 都可以作为开关值",
            "  3. 当前支持 groups.yaml + review_pipeline.yaml：前者管群基础参数，后者管审核分组流转",
            "  4. review_pipeline.yaml 使用 conditions + mode 定义分组，不支持布尔表达式",
            "  5. mode 可选 all_pass（全部通过）或 any_pass（任一通过）",
            "  6. 详细参数用 /设置群 无参数查看，审核分组请编辑 review_pipeline.yaml",
            "  7. /检查bili 不提供申请QQ，QQ等级、退群及绑定条件按无法判断参与模拟",
        ]
    )


@driver.on_startup
async def _start_background_tasks() -> None:
    """启动磁盘监控、Cookie 监控和可选 WebUI。"""
    global _disk_monitor_task, _cookie_monitor_task, _webui_server
    await run_storage(initialize_database)
    logger.success("SQLite 业务记录数据库初始化完成")
    await _decision_service.restore_pending()
    _disk_monitor_task = asyncio.create_task(_disk_space_monitor())
    _cookie_monitor_task = asyncio.create_task(cookie_monitor.cookie_monitor_loop())
    config = ConfigManager.get_config()
    if config.webui_enabled:
        try:
            _webui_server = start_webui(config)
        except Exception:
            logger.exception("WebUI 启动失败")


@driver.on_shutdown
async def _stop_background_tasks() -> None:
    """Stop optional workers cleanly during reload or shutdown."""
    tasks_to_cancel = []
    if _disk_monitor_task:
        _disk_monitor_task.cancel()
        tasks_to_cancel.append(_disk_monitor_task)
    if _cookie_monitor_task:
        _cookie_monitor_task.cancel()
        tasks_to_cancel.append(_cookie_monitor_task)
    if tasks_to_cancel:
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
    if _webui_server:
        _webui_server.shutdown()
    await _bili_requests.close()


@driver.on_startup
async def ensure_bili_cookie_on_startup() -> None:
    """启动时检查 B站 Cookie，已配置 CookieCloud 时尝试同步并输出登录指引。"""
    existing_cookie = ConfigManager.get_bili_cookie().strip()
    if existing_cookie:
        logger.info("已检测到 B站 Cookie，正在检查目标主播关注状态。")
        await _ensure_login_account_follows_targets(existing_cookie)
        return

    host, uuid, key = _get_cookiecloud_config()
    if CookieManager.has_cookiecloud_config(host, uuid, key):
        logger.warning("启动时未检测到 B站 Cookie，正在尝试从 CookieCloud 自动获取。")
        cookie = await CookieManager.fetch_from_cookiecloud(host, uuid, key)
        if cookie:
            CookieManager.set_cookie(cookie)
            logger.success("已在启动阶段通过 CookieCloud 获取并保存 B站 Cookie。")
            await _ensure_login_account_follows_targets(cookie)
            return
        logger.warning("CookieCloud 已配置，但本次未获取到可用的 B站 Cookie。")

    try:
        qr_url, img_path, terminal_qrcode = CookieManager.generate_terminal_qrcode()
    except Exception:
        qr_url = ""
        img_path = ""
        terminal_qrcode = "（终端二维码生成失败，请使用登录链接）"
        logger.warning("终端二维码生成失败", exc_info=True)

    logger.warning("启动时未检测到可用的 B站 Cookie，审核功能当前无法正常工作。")
    print(
        "\n"
        + "=" * 72
        + "\n[B站 Cookie 引导]\n"
        + "当前未获取到可用的 B站 Cookie。\n"
        + "你可以任选以下方式：\n"
        + "1. 在 .env.prod 的 BILI_COOKIE 中填入 Cookie\n"
        + "2. 超级管理员私聊命令：/设置cookie <B站Cookie>\n"
        + "3. 先扫码登录，再自行将 Cookie 提供给本项目\n"
        + "4. 可选支持：已配置 CookieCloud 时，重启后会自动尝试同步\n\n"
        + "终端二维码如下：\n"
        + terminal_qrcode
        + "\n登录链接："
        + qr_url
        + "\n二维码图片："
        + img_path
        + "\n"
        + "=" * 72
        + "\n"
    )


def _configured_follow_target_uids() -> list[int]:
    """汇总所有群配置中的关注目标 UID，并保持稳定顺序。"""
    return list(
        dict.fromkeys(
            target_uid
            for group in ConfigManager.get_config().groups.values()
            for target_uid in group.target_uids
        )
    )


async def _ensure_login_account_follows_targets(cookie: str) -> str:
    """确保 Cookie 对应账号关注全部目标主播，并返回可展示摘要。"""
    target_uids = _configured_follow_target_uids()
    _bili_requests.set_verified_common_targets(cookie, set())
    if not target_uids:
        message = "未配置关注目标 UID，无需自动关注"
        logger.info(message)
        return message

    try:
        results = await _bili_requests.run(
            cookie, lambda api: api.ensure_following_targets(target_uids)
        )
    except Exception:
        logger.exception("目标主播关注检查发生未预期异常")
        return "目标主播关注检查失败，请查看日志"

    _bili_requests.set_verified_common_targets(
        cookie,
        {uid for uid, result in results.items() if result.state == "passed"},
    )

    already_following = sum(
        result.state == "passed" and result.detail == "已关注"
        for result in results.values()
    )
    auto_followed = sum(
        result.state == "passed" and result.detail == "已自动关注"
        for result in results.values()
    )
    failed = len(results) - already_following - auto_followed
    message = (
        f"目标主播关注检查完成：已关注 {already_following}，"
        f"自动关注 {auto_followed}，失败 {failed}"
    )
    if failed:
        details = "；".join(
            f"UID {uid}: {result.detail or result.state}"
            for uid, result in results.items()
            if result.state != "passed"
        )
        logger.warning("%s（%s）", message, details)
    else:
        logger.success(message)
    return message


# ---------------------------------------------------------------------------
# 事件处理：退群记录
# ---------------------------------------------------------------------------

@group_decrease_handler.handle()
async def handle_group_decrease(bot: Bot, event: GroupDecreaseNoticeEvent) -> None:
    """处理群成员减少事件，将退群信息写入持久化记录。"""
    if getattr(event, "notice_type", None) != "group_decrease":
        return

    group_id = str(event.group_id)
    user_id = str(event.user_id)

    try:
        member_info = await bot.get_group_member_info(
            group_id=event.group_id,
            user_id=event.user_id,
            no_cache=True,
        )
        nickname = (
            member_info.get("card", "")
            or member_info.get("nickname", "")
            or f"用户{user_id}"
        )
    except Exception:
        nickname = f"用户{user_id}"

    await run_storage(LeaveRecordManager.add_leave_record, group_id, user_id, nickname)


# ---------------------------------------------------------------------------
# 事件处理：加群申请审核
# ---------------------------------------------------------------------------

async def _populate_bili_context(
    context: JoinRequestContext,
    group_config: GroupConfig,
    pipeline_conditions: set[str],
    bili_cookie: str,
    *,
    lookup_bindings: bool = True,
) -> None:
    """填充审核上下文的 B站相关字段（搜索用户、关注、勋章等）。

    此函数被 handle_group_request 和 /检查bili 命令共用，统一 B站 API 调用逻辑。
    调用后检查 context.bili_uid 是否为 None 来判断搜索是否成功。
    """
    async def populate(api: BiliApi) -> None:
        context.bili_search_result = await api.resolve_user_profile(context.bili_name or "")
        context.bili_uid = context.bili_search_result.matched_target_uid
        context.bili_level = context.bili_search_result.matched_level
        if not context.bili_uid:
            return

        if "follow" in pipeline_conditions and group_config.target_uids:
            context.follow_result = await api.check_follow_any_status(
                context.bili_uid,
                group_config.target_uids,
                common_negative_is_definitive=(
                    _bili_requests.common_negative_is_definitive(
                        bili_cookie, group_config.target_uids
                    )
                ),
            )
        if "medal" in pipeline_conditions and group_config.target_medal_uids:
            context.medal_result = await api.check_medal_any_status(
                context.bili_uid,
                group_config.target_medal_uids,
                group_config.required_medal_level,
            )

    await _bili_requests.run(bili_cookie, populate)
    if lookup_bindings:
        await _populate_identity_bindings(context, pipeline_conditions)


async def _populate_identity_bindings(
    context: JoinRequestContext, pipeline_conditions: set[str]
) -> None:
    """Read binding conditions inside the live request's identity decision locks."""
    if context.bili_uid is None:
        return
    if "no_conflict" in pipeline_conditions:
        context.conflict_qq = await run_storage(
            JoinRequestRecordManager.get_conflict_qq,
            context.actor_qq_for_binding,
            context.bili_uid,
        )
        context.conflict_lookup_known = True
    if "no_identity_change" in pipeline_conditions:
        context.owned_other_bili_uids = [
            owned_uid
            for owned_uid in await run_storage(
                JoinRequestRecordManager.get_owned_bili_uids,
                context.actor_qq_for_binding,
            )
            if owned_uid != context.bili_uid
        ]
        context.identity_change_lookup_known = True


async def _review_request_with_identity_guard(
    context: JoinRequestContext,
    stages: list[StageRuntime],
    pipeline_conditions: set[str],
) -> None:
    """Keep identity reads and their resulting audit/approval in one critical section."""
    async with _decision_service.locks.hold(*identity_decision_keys(context.user_id, context.bili_uid)):
        try:
            await _populate_identity_bindings(context, pipeline_conditions)
        except Exception:
            logger.exception("身份绑定查询失败，未确认的绑定条件按 I 处理")
        context.condition_statuses = _build_rule_condition_statuses(
            context.group_config,
            pipeline_conditions,
            qq_level=context.qq_level,
            bili_search_result=context.bili_search_result,
            bili_level=context.bili_level,
            follow_result=context.follow_result,
            medal_result=context.medal_result,
            leave_record=context.leave_record,
            conflict_qq=context.conflict_qq,
            owned_other_bili_uids=context.owned_other_bili_uids,
            conflict_lookup_known=context.conflict_lookup_known,
            identity_change_lookup_known=context.identity_change_lookup_known,
        )
        context.condition_states, context.condition_reasons = _build_condition_snapshot(
            context.condition_statuses
        )

        try:
            execution = await _run_stage_pipeline(context, stages)
        except ValueError as exc:
            audit = await run_storage(
                JoinRequestRecordManager.add_audit,
                **_build_audit_kwargs_from_context(
                    context,
                    result="ignored",
                    reasons=[f"审核分组路由无效：{exc}"],
                )
            )
            if audit is None:
                logger.critical("审核分组路由异常后无法写入忽略审计")
                await _notify_admins(
                    context.bot,
                    f"严重：QQ {context.user_id} 的路由异常审计未能写入 SQLite，请检查数据库和磁盘空间。",
                )
            return
        await _finalize_stage_decision(
            context,
            execution.stage,
            execution.outcome,
            execution.dispatch_result.flow_action,
            execution.reasons,
        )


@group_request_handler.handle()
async def handle_group_request(bot: Bot, event: GroupRequestEvent) -> None:
    """处理加群申请，并按照阶段化审核流程分发动作。"""
    if event.sub_type != "add":
        return

    group_id = str(event.group_id)
    user_id = str(event.user_id)
    group_config = ConfigManager.get_group_config(group_id)
    if not group_config or not group_config.enabled:
        return

    context = JoinRequestContext(
        group_id=group_id,
        user_id=user_id,
        group_config=group_config,
        bot=bot,
        event=event,
        actor_qq_for_binding=user_id,
        bili_name=_extract_bili_name(event.comment or ""),
    )

    try:
        stages = _get_review_stages(group_config)
    except ValueError as exc:
        await _finalize_early_decision(
            context,
            flow_action="ignore",
            reasons=[f"审核阶段配置无效：{exc}"],
            stage_name="审核配置检查",
        )
        return
    pipeline_conditions = _collect_pipeline_conditions(stages)

    if "qq_level" in pipeline_conditions:
        context.qq_level = await _fetch_qq_level(bot, user_id, event)

    if "no_leave_record" in pipeline_conditions:
        context.leave_record = await run_storage(
            LeaveRecordManager.get_leave_record, group_id, user_id
        )
        context.leave_status = "matched" if context.leave_record else "clear"

    if _requires_bili_identity(pipeline_conditions):
        if not context.bili_name:
            context.bili_search_result = CheckResult("failed", ACCOUNT_REQUIRED_MESSAGE)
        else:
            bili_cookie = ConfigManager.get_bili_cookie()
            if not bili_cookie:
                context.bili_search_result = CheckResult(
                    "inaccessible", "未配置可用的 B站 Cookie"
                )
            else:
                try:
                    await _populate_bili_context(
                        context, group_config, pipeline_conditions, bili_cookie,
                        lookup_bindings=False,
                    )
                except Exception:
                    logger.exception("B站 API 流程异常")
                    _mark_bili_flow_exception(context)

        custom_reject_reason = (
            group_config.custom_reject_uids.get(str(context.bili_uid))
            if context.bili_uid is not None
            else None
        )
        if custom_reject_reason:
            context.stage_results.append(
                f"自定义拒绝名单 | UID {context.bili_uid} | 拒绝 | {format_uid_restriction_reason(custom_reject_reason)}"
            )
            await _finalize_early_decision(
                context,
                flow_action="reject",
                reasons=[format_uid_restriction_reason(custom_reject_reason)],
                stage_name="自定义拒绝名单",
            )
            return

    await _review_request_with_identity_guard(context, stages, pipeline_conditions)


# ---------------------------------------------------------------------------
# 命令处理：设置群配置
# ---------------------------------------------------------------------------

@set_group_config.handle()
async def handle_set_group_config(event: MessageEvent, arg: Message = CommandArg()) -> None:
    """命令：/设置群 <群号> [<参数名> <值> ...]。"""
    if not _is_private_superadmin_event(event):
        return

    raw_args = str(arg).strip()
    try:
        args = shlex.split(raw_args)
    except ValueError:
        args = raw_args.split()
    if not args:
        await set_group_config.finish(
            "\n".join(
                [
                    "用法：/设置群 <群号> [<参数名> <值> ...]",
                    "",
                    "示例：",
                    "/设置群 123456789 enable on",
                    "/设置群 123456789 required_qq_level 32",
                    "/设置群 123456789 required_bili_level 2",
                    "/设置群 123456789 target_uids 主播UID",
                    "/设置群 123456789 target_medal_uids 主播UID required_medal_level 10",
                    "",
                    "参数说明：",
                    "enable: 开启/关闭整个群审核",
                    "required_qq_level: QQ 最低等级",
                    "required_bili_level: B站最低等级",
                    "target_uids: 需要关注的 B站 UID，多个用逗号分隔",
                    "target_medal_uids: 需要检查的主播 UID，多个用逗号分隔",
                    "required_medal_level: 粉丝牌最低等级",
                    "reject_reason_with_leave_time: 拒绝时是否带退群时间",
                    "可用条件: bili_account、bili_level、qq_level、follow、medal、no_leave_record、no_conflict、no_identity_change",
                    "说明: 审核条件与模式统一在 review_pipeline.yaml 的分组中配置",
                ]
            )
        )

    group_id = args[0]
    existing_config = ConfigManager.get_group_config(group_id)
    group_config = (
        existing_config.model_copy(deep=True) if existing_config
        else GroupConfig(group_id=group_id)
    )

    index = 1
    updated_fields: list[str] = []
    while index < len(args):
        key = args[index].lower()
        if index + 1 >= len(args):
            await set_group_config.finish(f"参数 {key} 缺少对应的值")
        value = args[index + 1]

        if key == "enable":
            group_config.enabled = parse_bool(value)
            updated_fields.append(key)
        elif key == "required_qq_level":
            try:
                parsed_value = int(value)
            except (ValueError, TypeError):
                await set_group_config.finish("required_qq_level 需要输入合法的数字等级")
            if parsed_value < 0:
                await set_group_config.finish("required_qq_level 不能小于 0")
            group_config.required_qq_level = parsed_value
            updated_fields.append(key)
        elif key == "required_bili_level":
            try:
                parsed_value = int(value)
            except (ValueError, TypeError):
                await set_group_config.finish("required_bili_level 需要输入合法的数字等级")
            if parsed_value < 0:
                await set_group_config.finish("required_bili_level 不能小于 0")
            group_config.required_bili_level = parsed_value
            updated_fields.append(key)
        elif key == "target_uids":
            group_config.target_uids = parse_uid_list(value)
            updated_fields.append(key)
        elif key == "target_medal_uids":
            group_config.target_medal_uids = parse_uid_list(value)
            updated_fields.append(key)
        elif key == "required_medal_level":
            try:
                parsed_value = int(value)
            except (ValueError, TypeError):
                await set_group_config.finish("required_medal_level 需要输入合法的数字等级")
            if parsed_value < 0:
                await set_group_config.finish("required_medal_level 不能小于 0")
            group_config.required_medal_level = parsed_value
            updated_fields.append(key)
        elif key == "reject_reason_with_leave_time":
            group_config.reject_reason_with_leave_time = parse_bool(value)
            updated_fields.append(key)
        elif key == "reject_reasons":
            try:
                group_config.reject_reasons = parse_reject_reasons(value)
            except (ValueError, TypeError):
                await set_group_config.finish("reject_reasons 需要输入合法的 JSON 对象")
            updated_fields.append(key)
        else:
            await set_group_config.finish(f"不支持的参数：{key}")
        index += 2

    try:
        ConfigManager.set_group_config(group_config, updated_fields)
    except Exception:
        logger.exception("群配置保存失败，保留原运行配置")
        await set_group_config.finish("配置保存失败，原配置继续生效，请检查日志。")
    if updated_fields:
        follow_summary = ""
        if "target_uids" in updated_fields:
            cookie = ConfigManager.get_bili_cookie().strip()
            if cookie:
                follow_summary = await _ensure_login_account_follows_targets(cookie)
        suffix = f"\n{follow_summary}" if follow_summary else ""
        await set_group_config.finish(
            f"群 {group_id} 配置已更新：{'、'.join(updated_fields)}{suffix}"
        )
    await set_group_config.finish(f"群 {group_id} 配置已更新")


# ---------------------------------------------------------------------------
# 命令处理：B 站 Cookie
# ---------------------------------------------------------------------------

@set_cookie.handle()
async def handle_set_cookie(event: MessageEvent, arg: Message = CommandArg()) -> None:
    """命令：/设置cookie <B站Cookie> —— 手动设置 Cookie（仅超级管理员可用）。"""
    if not _is_private_superadmin_event(event):
        return

    cookie = str(arg).strip()
    if not cookie:
        await set_cookie.finish("用法：/设置cookie <B站Cookie>")
    CookieManager.set_cookie(cookie)
    follow_summary = await _ensure_login_account_follows_targets(cookie)
    await set_cookie.finish(f"B站 Cookie 已保存\n{follow_summary}")


@fetch_cookie_cloud.handle()
async def handle_fetch_cookie_cloud(event: MessageEvent) -> None:
    """命令：/获取cookie —— 从 CookieCloud 同步 B 站 Cookie（仅超级管理员可用）。"""
    if not _is_private_superadmin_event(event):
        return

    driver_config = get_driver().config
    host = getattr(driver_config, "cookiecloud_host", os.getenv("COOKIECLOUD_HOST", ""))
    uuid = getattr(driver_config, "cookiecloud_uuid", os.getenv("COOKIECLOUD_UUID", ""))
    key = getattr(driver_config, "cookiecloud_key", os.getenv("COOKIECLOUD_KEY", ""))

    if not all([host, uuid, key]):
        await fetch_cookie_cloud.finish(
            "请先配置 COOKIECLOUD_HOST、COOKIECLOUD_UUID、COOKIECLOUD_KEY"
        )

    cookie = await CookieManager.fetch_from_cookiecloud(host, uuid, key)
    if not cookie:
        await fetch_cookie_cloud.finish("从 CookieCloud 获取 Cookie 失败")

    CookieManager.set_cookie(cookie)
    follow_summary = await _ensure_login_account_follows_targets(cookie)
    await fetch_cookie_cloud.finish(
        f"已从 CookieCloud 获取并保存 B站 Cookie\n{follow_summary}"
    )


# ---------------------------------------------------------------------------
# 命令处理：登录二维码
# ---------------------------------------------------------------------------

@make_login_qrcode.handle()
async def handle_make_login_qrcode(event: MessageEvent) -> None:
    """命令：/登录二维码 —— 生成 B 站登录二维码（仅超级管理员可用）。"""
    if not _is_private_superadmin_event(event):
        return

    qr_url, img_path = CookieManager.generate_qrcode()
    await make_login_qrcode.finish(f"二维码已生成：{img_path}\n登录链接：{qr_url}")


# ---------------------------------------------------------------------------
# 命令处理：查看配置 / 退群记录 / 绑定管理 / 用户申请记录
# ---------------------------------------------------------------------------

@show_config.handle()
async def handle_show_config(event: MessageEvent, arg: Message = CommandArg()) -> None:
    """命令：/查看配置 [<群号>] —— 查看全局或指定群的配置。"""
    if not _is_private_admin_event(event):
        return

    group_id = str(arg).strip()
    if group_id:
        group_config = ConfigManager.get_group_config(group_id)
        if not group_config:
            await show_config.finish(f"群 {group_id} 未配置")
        await show_config.finish(_format_group_config(group_config))

    driver_config = get_driver().config
    config = ConfigManager.get_config()
    cookiecloud_host = getattr(driver_config, "cookiecloud_host", os.getenv("COOKIECLOUD_HOST", ""))
    cookiecloud_uuid = getattr(driver_config, "cookiecloud_uuid", os.getenv("COOKIECLOUD_UUID", ""))
    cookiecloud_key = getattr(driver_config, "cookiecloud_key", os.getenv("COOKIECLOUD_KEY", ""))

    if config.bili_cookie:
        cookie_status = "已设置"
    elif all([cookiecloud_host, cookiecloud_uuid, cookiecloud_key]):
        cookie_status = "已设置 CookieCloud"
    else:
        cookie_status = "未设置"

    admin_qqs = sorted(_get_allowed_admin_qqs())
    abnormal_normal_admins = (
        sorted(_get_normal_admin_qqs()) if ConfigManager.get_notify_mode() == "all" else []
    )
    lines = [
        "【全局配置】",
        "",
        f"B站 Cookie：{cookie_status}",
        f"命令私聊管理员：{'、'.join(admin_qqs) or '未设置'}",
        f"异常申请通知普通管理员：{'、'.join(abnormal_normal_admins) or '无'}",
        f"异常通知模式：{ConfigManager.get_notify_mode()}",
    ]
    if not config.groups:
        lines.append("群配置：暂无")
    else:
        lines.append(f"群配置数量：{len(config.groups)}")
        for index, current_group_id in enumerate(sorted(config.groups)):
            if index > 0:
                lines.append("")
            lines.append(_format_group_config(config.groups[current_group_id]))
    await show_config.finish("\n".join(lines))


@remove_leave_record.handle()
async def handle_remove_leave_record(event: MessageEvent, arg: Message = CommandArg()) -> None:
    """命令：/移除退群记录 <QQ号> —— 删除该 QQ 在所有群中的退群记录。"""
    if not _is_private_superadmin_event(event):
        return

    qq = str(arg).strip()
    if not qq:
        await remove_leave_record.finish("用法：/移除退群记录 <QQ号>")

    affected_group_ids = await run_storage(
        LeaveRecordManager.remove_leave_record_from_all_groups, qq
    )
    if not affected_group_ids:
        await remove_leave_record.finish(f"未找到 QQ {qq} 的退群记录")

    await remove_leave_record.finish(
        f"已移除 QQ {qq} 的退群记录，共影响 {len(affected_group_ids)} 个群："
        + "、".join(affected_group_ids)
    )


@show_pending_decisions.handle()
async def handle_show_pending_decisions(event: MessageEvent) -> None:
    """命令：/待确认审批 —— 查看尚未确认实际结果的 pending 审计。"""
    if not _is_private_superadmin_event(event):
        return
    audits = await run_storage(JoinRequestRecordManager.get_pending_decisions, 20)
    if not audits:
        await show_pending_decisions.finish("当前没有待确认审批，自动审批可正常运行")
    lines = ["【待确认审批】", ""]
    for audit in audits:
        planned_result = "同意" if audit.result == "approved" else "拒绝"
        lines.append(
            f"审计ID：{audit.id} | 时间：{audit.created_at} | 群：{audit.group_id} | "
            f"QQ：{audit.qq} | 原计划：{planned_result}"
        )
    lines.extend(
        [
            "",
            "核对 QQ 群中的实际结果后使用：",
            "/确认审批 <审计ID> applied",
            "/确认审批 <审计ID> failed",
        ]
    )
    await show_pending_decisions.finish("\n".join(lines))


@resolve_pending_decision.handle()
async def handle_resolve_pending_decision(
    event: MessageEvent, arg: Message = CommandArg()
) -> None:
    """命令：/确认审批 <审计ID> <applied|failed>。"""
    if not _is_private_superadmin_event(event):
        return
    parts = str(arg).strip().split()
    if len(parts) != 2 or not parts[0].isdigit():
        await resolve_pending_decision.finish(
            "用法：/确认审批 <审计ID> <applied|failed>"
        )
    audit_id = int(parts[0])
    resolution = parts[1].lower()
    if resolution not in {"applied", "failed"}:
        await resolve_pending_decision.finish("确认结果只能是 applied 或 failed")

    result = await _decision_service.resolve_pending(audit_id, applied=resolution == "applied")
    if not result.found:
        await resolve_pending_decision.finish(f"审计 {audit_id} 不存在或已经确认")
    if not result.finalized:
        await resolve_pending_decision.finish(f"审计 {audit_id} 确认失败，请检查 SQLite")
    await resolve_pending_decision.finish(
        f"审计 {audit_id} 已标记为 {resolution}；剩余待确认：{result.remaining}"
    )


@clear_identity_binding.handle()
async def handle_clear_identity_binding(event: MessageEvent, arg: Message = CommandArg()) -> None:
    """命令：/解除绑定 <QQ号> —— 清除该 QQ 当前的 B站 绑定归属。"""
    if not _is_private_admin_event(event):
        return

    qq = str(arg).strip()
    if not qq:
        await clear_identity_binding.finish("用法：/解除绑定 <QQ号>")

    async with _decision_service.locks.hold(*identity_decision_keys(qq, None)):
        had_binding = (
            await run_storage(JoinRequestRecordManager.get_user_binding, qq)
        ) is not None
        removed_uids = await run_storage(JoinRequestRecordManager.clear_identity_binding, qq)
    if not removed_uids and not had_binding:
        await clear_identity_binding.finish(f"未找到 QQ {qq} 的绑定记录")

    uid_text = "、".join(str(uid) for uid in removed_uids) if removed_uids else "无当前归属 UID"
    await clear_identity_binding.finish(
        f"已清除 QQ {qq} 的绑定信息，解除的 B站 UID：{uid_text}"
    )


@check_qq.handle()
async def handle_check_qq(event: MessageEvent, arg: Message = CommandArg()) -> None:
    """命令：/检查qq <QQ号> —— 查看该 QQ 的加群申请与绑定记录。"""
    if not _is_private_admin_event(event):
        return

    qq = str(arg).strip()
    if not qq:
        await check_qq.finish("用法：/检查qq <QQ号>")

    await check_qq.finish(await _format_user_check_reply(qq))


@check_bili.handle()
async def handle_check_bili(event: MessageEvent, arg: Message = CommandArg()) -> None:
    """命令：/检查bili <群号> <B站UID或昵称> —— 模拟审核，不写申请、绑定或退群记录。"""
    if not _is_private_admin_event(event):
        return

    raw_args = str(arg).strip()
    try:
        args = shlex.split(raw_args)
    except ValueError:
        args = raw_args.split()

    if len(args) < 2:
        await check_bili.finish("用法：/检查bili <群号> <B站UID或昵称>")

    group_id = args[0]
    bili_name = " ".join(args[1:]).strip()
    if not bili_name:
        await check_bili.finish("用法：/检查bili <群号> <B站UID或昵称>")

    group_config = ConfigManager.get_group_config(group_id)
    if not group_config:
        await check_bili.finish(f"群 {group_id} 未配置")
    if not group_config.enabled:
        await check_bili.finish(f"群 {group_id} 审核功能已关闭")

    bili_cookie = ConfigManager.get_bili_cookie()
    if not bili_cookie:
        await check_bili.finish("未配置可用的 B站 Cookie，无法检查")

    try:
        stages = _get_review_stages(group_config)
    except ValueError as exc:
        await check_bili.finish(f"审核阶段配置无效：{exc}")

    context = JoinRequestContext(
        group_id=group_id,
        user_id=str(event.user_id),
        group_config=group_config,
        dry_run=True,
        input_bili_name=bili_name,
        bili_name=bili_name,
    )
    pipeline_conditions = _collect_pipeline_conditions(stages)

    lines = [
        "【B站审核测试】",
        "",
        f"群号：{group_id}",
        f"B站输入：{bili_name}",
    ]

    try:
        await _populate_bili_context(context, group_config, pipeline_conditions, bili_cookie)
    except Exception:
        logger.exception("测试命令 B站 API 调用异常")
        lines.append("B站 API 调用异常，部分条件会被判定为无法判断")

    if context.bili_uid:
        lines.append(f"B站 UID：{context.bili_uid}")
    if context.bili_level is not None:
        lines.append(f"B站等级：{context.bili_level}")

    custom_reject_reason = (
        group_config.custom_reject_uids.get(str(context.bili_uid))
        if context.bili_uid is not None
        else None
    )
    if custom_reject_reason:
        lines.append("")
        lines.append(f"拒绝：{format_uid_restriction_reason(custom_reject_reason)}")
        await check_bili.finish("\n".join(lines))

    context.condition_statuses = _build_rule_condition_statuses(
        group_config,
        pipeline_conditions,
        qq_level=None,
        bili_search_result=context.bili_search_result,
        bili_level=context.bili_level,
        follow_result=context.follow_result,
        medal_result=context.medal_result,
        leave_record=None,
        conflict_qq=context.conflict_qq,
        owned_other_bili_uids=context.owned_other_bili_uids,
        leave_record_known=False,
        identity_binding_known=False,
    )
    context.condition_states, context.condition_reasons = _build_condition_snapshot(
        context.condition_statuses
    )
    has_incomplete_conditions = any(
        status.state == "unknown" for status in context.condition_statuses.values()
    )

    lines.extend(["", "【条件结果】"])
    lines.extend(_format_condition_details(context.condition_states, context.condition_reasons))

    try:
        execution = await _run_stage_pipeline(context, stages)
    except ValueError as exc:
        await check_bili.finish(f"审核分组路由无效：{exc}")
    lines.extend(["", "【流程详情】"])
    lines.extend(_format_stage_results(context.stage_results))

    lines.extend(["", "【最终结果】"])
    if has_incomplete_conditions:
        lines.append(f"模拟动作：{_format_action_text(execution.dispatch_result.flow_action)}")
        lines.extend(
            [
                "说明：本命令未提供 QQ。",
                "QQ等级、退群记录和绑定关系会按“无法判断”参与流程。",
            ]
        )
    else:
        lines.append(f"最终动作：{_format_action_text(execution.dispatch_result.flow_action)}")
    reasons = execution.reasons
    if execution.dispatch_result.flow_action == "reject":
        reasons = _request_reject_reasons(context, reasons, stage=execution.stage, outcome=execution.outcome)
    lines.append(f"最终原因：{'；'.join(reasons) or '无'}")
    await check_bili.finish("\n".join(lines))


@show_help.handle()
async def handle_show_help(event: MessageEvent) -> None:
    """命令：/help —— 查看所有管理员指令。"""
    if not _is_private_admin_event(event):
        return

    await show_help.finish(_format_help_text())
