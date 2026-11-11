"""Evaluate observed account and membership data into T/F/I conditions."""

from .config import GroupConfig
from .formatters import format_leave_time
from .models import CheckResult, LeaveRecordLike, RuleConditionStatus

LEGACY_NICKNAME_NOT_FOUND_MESSAGE = "未搜索到你填写的B站昵称，请不要输入与b站昵称无关的字段并检查你的昵称是否输入有误"
LEGACY_REJECT_REASONS = {
    "bili_account": {LEGACY_NICKNAME_NOT_FOUND_MESSAGE},
    "bili_level": {"你的B站等级过低,疑似机器人账号"},
    "qq_level": {"你的Q等级过低,疑似机器人账号", "你的QQ等级过低,疑似机器人账号"},
    "follow": {"你没有关注主播,本群为粉丝群,禁止非粉丝加入,若你确定已经关注了主播,请检查你填写的b站昵称是否正确"},
    "no_leave_record": {"检测到退群记录", "命中退群记录"},
}


def build_condition_snapshot(
    condition_statuses: dict[str, RuleConditionStatus],
) -> tuple[dict[str, str], dict[str, str]]:
    """将条件求值结果压缩成适合审计持久化的快照。"""
    states: dict[str, str] = {}
    reasons: dict[str, str] = {}
    for name, status in condition_statuses.items():
        states[name] = status.state
        if status.reason:
            reasons[name] = status.reason
    return states, reasons


def requires_bili_identity(rule_conditions: set[str]) -> bool:
    """判断规则是否依赖 B站身份信息。"""
    return bool(
        rule_conditions & {"bili_account", "bili_level", "follow", "medal", "no_conflict", "no_identity_change"}
    )


def configured_reject_reason(group_config: GroupConfig, condition: str, default: str) -> str:
    """Preserve custom explanations without hiding observed facts or requirements."""
    reason = group_config.reject_reasons.get(condition, "").strip()
    if not reason or reason in LEGACY_REJECT_REASONS.get(condition, set()):
        return default
    return reason if default in reason else f"{reason}；{default}"


def build_rule_condition_statuses(
    group_config: GroupConfig,
    rule_conditions: set[str],
    *,
    qq_level: int | None,
    bili_search_result: CheckResult,
    bili_level: int | None,
    follow_result: CheckResult,
    medal_result: CheckResult,
    leave_record: LeaveRecordLike | None,
    conflict_qq: str | None,
    owned_other_bili_uids: list[int],
    leave_record_known: bool = True,
    identity_binding_known: bool = True,
    conflict_lookup_known: bool = False,
    identity_change_lookup_known: bool = False,
) -> dict[str, RuleConditionStatus]:
    """基于各项检查结果生成分组管道所需的条件状态。"""
    statuses: dict[str, RuleConditionStatus] = {}

    if "bili_account" in rule_conditions:
        if bili_search_result.state == "passed":
            statuses["bili_account"] = RuleConditionStatus("true")
        elif bili_search_result.state == "failed":
            statuses["bili_account"] = RuleConditionStatus(
                "false",
                configured_reject_reason(
                    group_config,
                    "bili_account",
                    bili_search_result.detail or "未找到你填写的B站账号，请检查UID或昵称是否填写正确后重新申请",
                ),
            )
        else:
            statuses["bili_account"] = RuleConditionStatus(
                "unknown", bili_search_result.detail or "B站账号搜索暂时不可用"
            )

    if "bili_level" in rule_conditions:
        if group_config.required_bili_level <= 0:
            statuses["bili_level"] = RuleConditionStatus("unknown", "规则引用了 bili_level，但未配置有效的 B站最低等级")
        elif bili_search_result.state != "passed" or bili_level is None:
            if bili_search_result.state == "failed":
                reason = "未搜索到B站账号，无法读取 B站等级"
            elif bili_search_result.state == "inaccessible":
                reason = f"{bili_search_result.detail or 'B站账号搜索暂时不可用'}，无法读取 B站等级"
            else:
                reason = "无法读取 B站等级"
            statuses["bili_level"] = RuleConditionStatus("unknown", reason)
        elif bili_level >= group_config.required_bili_level:
            statuses["bili_level"] = RuleConditionStatus("true")
        else:
            statuses["bili_level"] = RuleConditionStatus(
                "false",
                configured_reject_reason(
                    group_config, "bili_level",
                    f"B站等级不足：当前{bili_level}级，本群要求至少{group_config.required_bili_level}级；请达到要求后重新申请",
                ),
            )

    if "qq_level" in rule_conditions:
        if group_config.required_qq_level <= 0:
            statuses["qq_level"] = RuleConditionStatus("unknown", "规则引用了 qq_level，但未配置有效的 QQ 最低等级")
        elif qq_level is None:
            statuses["qq_level"] = RuleConditionStatus(
                "unknown", f"暂无法读取你的QQ等级，无法确认是否达到本群要求（至少{group_config.required_qq_level}级），请稍后重试或联系管理员"
            )
        elif qq_level >= group_config.required_qq_level:
            statuses["qq_level"] = RuleConditionStatus("true")
        else:
            statuses["qq_level"] = RuleConditionStatus(
                "false",
                configured_reject_reason(
                    group_config,
                    "qq_level",
                    f"QQ等级不足：当前{qq_level}级，本群要求至少{group_config.required_qq_level}级；请达到要求后重新申请",
                ),
            )

    if "follow" in rule_conditions:
        if not group_config.target_uids:
            statuses["follow"] = RuleConditionStatus("unknown", "规则引用了 follow，但未配置关注目标 UID")
        elif follow_result.state == "passed":
            statuses["follow"] = RuleConditionStatus("true")
        elif follow_result.state == "failed":
            statuses["follow"] = RuleConditionStatus(
                "false",
                configured_reject_reason(
                    group_config,
                    "follow",
                    "未检测到该B站账号关注指定主播（UID："
                    + "、".join(str(uid) for uid in group_config.target_uids)
                    + "，关注任一位即可）；请关注后重新申请，如确认已关注请核对所填UID或昵称",
                ),
            )
        else:
            detail = follow_result.detail.strip() or "关注列表不可见或无法读取"
            statuses["follow"] = RuleConditionStatus("unknown", f"暂无法确认关注状态：{detail}；请稍后重试或联系管理员")

    if "medal" in rule_conditions:
        if not group_config.target_medal_uids:
            statuses["medal"] = RuleConditionStatus("unknown", "规则引用了 medal，但未配置粉丝牌目标 UID")
        elif medal_result.state == "passed":
            statuses["medal"] = RuleConditionStatus("true")
        elif medal_result.state == "failed":
            reason = (
                "粉丝牌未达到要求：目标主播UID为"
                + "、".join(str(uid) for uid in group_config.target_medal_uids)
                + f"，持有任一位的粉丝牌且至少{group_config.required_medal_level}级即可；请核对账号和粉丝牌，满足要求后再申请"
            )
            detail = medal_result.detail.strip()
            if detail and detail not in reason:
                reason = f"{detail}；{reason}"
            statuses["medal"] = RuleConditionStatus(
                "false",
                configured_reject_reason(group_config, "medal", reason),
            )
        else:
            detail = medal_result.detail.strip() or "粉丝牌列表不可见或无法读取"
            statuses["medal"] = RuleConditionStatus("unknown", f"暂无法确认粉丝牌或等级：{detail}；请稍后重试或联系管理员")

    if "no_leave_record" in rule_conditions:
        if not leave_record_known:
            statuses["no_leave_record"] = RuleConditionStatus("unknown", "未提供 QQ，无法判断是否存在退群记录")
        elif leave_record is None:
            statuses["no_leave_record"] = RuleConditionStatus("true")
        else:
            reason = "检测到你在本群的退群记录，未满足本群入群要求；如需重新加入，请联系管理员核实"
            if group_config.reject_reason_with_leave_time:
                formatted_leave_time = format_leave_time(leave_record.leave_time)
                if formatted_leave_time:
                    reason = f"你曾于{formatted_leave_time}退出本群，未满足本群入群要求；如需重新加入，请联系管理员核实"
            reason = configured_reject_reason(group_config, "no_leave_record", reason)
            statuses["no_leave_record"] = RuleConditionStatus("false", reason)

    bili_identity_known = (
        bili_search_result.state == "passed"
        and isinstance(bili_search_result.matched_target_uid, int)
        and bili_search_result.matched_target_uid > 0
    )

    if "no_conflict" in rule_conditions:
        if not (identity_binding_known and bili_identity_known and conflict_lookup_known):
            statuses["no_conflict"] = RuleConditionStatus(
                "unknown", "暂无法确认该B站账号与QQ的绑定关系，请稍后重试或联系管理员核实"
            )
        elif conflict_qq:
            statuses["no_conflict"] = RuleConditionStatus(
                "false", configured_reject_reason(
                    group_config, "no_conflict",
                    "该B站账号已与另一个QQ绑定，无法用于当前QQ申请；请核对你本人的UID或昵称，如绑定有误请联系管理员核实",
                )
            )
        else:
            statuses["no_conflict"] = RuleConditionStatus("true")

    if "no_identity_change" in rule_conditions:
        if not (identity_binding_known and bili_identity_known and identity_change_lookup_known):
            statuses["no_identity_change"] = RuleConditionStatus(
                "unknown", "B站身份或绑定记录未能确认，无法判断是否存在改绑记录"
            )
        elif owned_other_bili_uids:
            joined_uids = "、".join(str(uid) for uid in owned_other_bili_uids)
            statuses["no_identity_change"] = RuleConditionStatus(
                "false",
                configured_reject_reason(
                    group_config, "no_identity_change",
                    f"当前QQ已绑定B站UID：{joined_uids}，与本次填写的UID不一致；请使用原绑定账号，或联系管理员核实解绑后再申请",
                ),
            )
        else:
            statuses["no_identity_change"] = RuleConditionStatus("true")

    # Expressions compare observations, never the boolean threshold states.
    # A missing configured v2 threshold may therefore stay I while a measured
    # value remains available to a v3 numeric comparison.
    if "qq_level" in statuses and isinstance(qq_level, (int, float)) and not isinstance(qq_level, bool):
        statuses["qq_level"].observation = qq_level
    if (
        "bili_level" in statuses and bili_search_result.state == "passed"
        and isinstance(bili_level, (int, float)) and not isinstance(bili_level, bool)
    ):
        statuses["bili_level"].observation = bili_level
    if (
        "medal" in statuses and medal_result.state in {"passed", "failed"}
        and isinstance(medal_result.matched_level, (int, float))
        and not isinstance(medal_result.matched_level, bool)
    ):
        statuses["medal"].observation = medal_result.matched_level
    return statuses
