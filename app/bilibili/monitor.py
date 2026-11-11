"""
B站 Cookie 有效性定时检查模块。

通过请求 B站 nav API 验证 Cookie 是否有效，过期或检查异常时发送邮件通知。
网络异常与明确过期区分，网络异常会通知但不误报 Cookie 过期。

环境变量：
- COOKIE_CHECK_INTERVAL_SECONDS : 检查周期（秒），默认 3600（1小时）
"""

import asyncio
import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import httpx

from app.notifications import email as email_notifier
from app.notifications.dedup import NotificationDeduplicator
from app.config import ConfigManager
from .client import HEADERS
from .credential import CookieManager
from .pacing import HTTP_LIMITER

logger = logging.getLogger("group_request_manager.cookie_monitor")

# 环境变量名称
ENV_CHECK_INTERVAL = "COOKIE_CHECK_INTERVAL_SECONDS"

# 默认值
DEFAULT_CHECK_INTERVAL_SECONDS = 3600
MIN_CHECK_INTERVAL_SECONDS = 60
MAX_CHECK_INTERVAL_SECONDS = 86400
STARTUP_CHECK_DELAY_SECONDS = 10

# B站 nav API
BILI_NAV_URL = "https://api.bilibili.com/x/web-interface/nav"
NAV_REQUEST_TIMEOUT = 30.0


class CookieCheckResult(Enum):
    """Cookie 检查结果枚举。"""

    VALID = "valid"
    EXPIRED = "expired"
    NETWORK_ERROR = "network_error"
    API_ERROR = "api_error"


@dataclass
class CookieCheckDetail:
    """Cookie 检查的详细结果。"""

    result: CookieCheckResult
    message: str = ""
    api_code: Optional[int] = None
    is_login: Optional[bool] = None


# ---------------------------------------------------------------------------
# 去重状态管理
# ---------------------------------------------------------------------------

_email_notifications = NotificationDeduplicator()


def reset_dedup_state() -> None:
    """重置去重状态（用于测试）。"""
    _email_notifications.reset()


# ---------------------------------------------------------------------------
# Cookie 检查
# ---------------------------------------------------------------------------


async def check_cookie_validity(cookie: str) -> CookieCheckDetail:
    """
    检查 B站 Cookie 是否有效。

    请求 nav API 并检查 code=0 且 data.isLogin=true。
    网络异常返回 NETWORK_ERROR，不误报过期。

    参数:
        cookie: B站 Cookie 字符串

    返回:
        CookieCheckDetail 包含检查结果和详细信息
    """
    if not cookie or not cookie.strip():
        return CookieCheckDetail(
            result=CookieCheckResult.EXPIRED,
            message="Cookie 为空",
        )

    headers = HEADERS.copy()
    headers["Cookie"] = cookie

    try:
        async with httpx.AsyncClient(timeout=NAV_REQUEST_TIMEOUT) as client:
            await HTTP_LIMITER.acquire()
            response = await client.get(BILI_NAV_URL, headers=headers)
            response.raise_for_status()
            data = response.json()
    except httpx.TimeoutException:
        return CookieCheckDetail(
            result=CookieCheckResult.NETWORK_ERROR,
            message="nav API 请求超时",
        )
    except httpx.HTTPStatusError as exc:
        return CookieCheckDetail(
            result=CookieCheckResult.NETWORK_ERROR,
            message=f"nav API HTTP 错误: {exc.response.status_code}",
        )
    except httpx.RequestError as exc:
        return CookieCheckDetail(
            result=CookieCheckResult.NETWORK_ERROR,
            message=f"nav API 请求失败: {type(exc).__name__}",
        )
    except ValueError:
        return CookieCheckDetail(
            result=CookieCheckResult.NETWORK_ERROR,
            message="nav API 响应非 JSON",
        )
    except Exception as exc:
        return CookieCheckDetail(
            result=CookieCheckResult.NETWORK_ERROR,
            message=f"未知异常: {type(exc).__name__}",
        )

    api_code = data.get("code")
    message = data.get("message") or data.get("msg") or ""
    data_obj = data.get("data")

    if api_code == -101:
        return CookieCheckDetail(
            result=CookieCheckResult.EXPIRED,
            message=f"Cookie 无效 (code={api_code}, msg={message[:100]})",
            api_code=api_code,
            is_login=False,
        )

    if not isinstance(data_obj, dict):
        return CookieCheckDetail(
            result=CookieCheckResult.API_ERROR,
            message=f"nav API 返回无效 data (code={api_code})",
            api_code=api_code,
        )

    is_login = data_obj.get("isLogin")

    if api_code == 0 and is_login is True:
        return CookieCheckDetail(
            result=CookieCheckResult.VALID,
            message="Cookie 有效",
            api_code=api_code,
            is_login=True,
        )

    if api_code == 0 and is_login is False:
        return CookieCheckDetail(
            result=CookieCheckResult.EXPIRED,
            message=f"Cookie 无效 (code={api_code}, isLogin={is_login})",
            api_code=api_code,
            is_login=False,
        )

    return CookieCheckDetail(
        result=CookieCheckResult.API_ERROR,
        message=f"nav API 异常 (code={api_code}, msg={message[:100]})",
        api_code=api_code,
        is_login=is_login,
    )


# ---------------------------------------------------------------------------
# 邮件通知
# ---------------------------------------------------------------------------


async def _notify_cookie_expired(detail: CookieCheckDetail) -> None:
    """
    尝试发送 Cookie 过期通知邮件。

    仅在去重逻辑允许时尝试发送。
    发送成功后标记已通知，发送失败不标记（下一周期重试）。
    """
    issue_key = CookieCheckResult.EXPIRED.value
    if not _email_notifications.should_attempt(issue_key):
        return

    subject = "【B站 Cookie 过期通知】请及时更新"
    body = (
        "B站 Cookie 已失效，请及时更新。\n\n"
        f"检测详情：{detail.message}\n\n"
        "更新方式：\n"
        "1. 使用 /设置cookie 命令手动设置\n"
        "2. 已配置 CookieCloud 时，可用 /获取cookie 命令同步（可选支持）\n"
        "3. 使用 /登录二维码 命令扫码登录\n"
    )

    sent = await email_notifier.send_notification(subject, body)
    if sent:
        _email_notifications.mark_sent(issue_key)
        logger.info("Cookie 过期通知邮件已发送")
    else:
        # 不标记已通知，下一周期将重试
        logger.warning("Cookie 过期通知邮件发送失败，将在下次检查时重试")


async def _notify_cookie_check_abnormal(detail: CookieCheckDetail) -> None:
    """
    尝试发送 Cookie 检查异常邮件。

    这类异常表示“当前无法可靠判断 Cookie 状态”，不会当作 Cookie 过期处理。
    """
    issue_key = detail.result.value
    if not _email_notifications.should_attempt(issue_key):
        return

    result_text = {
        CookieCheckResult.NETWORK_ERROR: "网络异常",
        CookieCheckResult.API_ERROR: "B站接口异常",
    }.get(detail.result, detail.result.value)
    subject = f"【B站 Cookie 检查异常】{result_text}"
    body = (
        "B站 Cookie 定时检查出现异常，当前不会自动认定 Cookie 已过期。\n\n"
        f"异常类型：{result_text}\n"
        f"检测详情：{detail.message or '无'}\n"
        f"API code：{detail.api_code if detail.api_code is not None else '无'}\n"
        f"isLogin：{detail.is_login if detail.is_login is not None else '无'}\n\n"
        "建议检查：\n"
        "1. 服务器到 B站 API 的网络连通性\n"
        "2. B站接口是否临时风控或返回异常\n"
        "3. 如持续异常，再使用 /获取cookie 或 /登录二维码 刷新 Cookie\n"
    )

    sent = await email_notifier.send_notification(subject, body)
    if sent:
        _email_notifications.mark_sent(issue_key)
        logger.info("Cookie 检查异常通知邮件已发送: %s", result_text)
    else:
        logger.warning("Cookie 检查异常通知邮件发送失败，将在下次检查时重试")


# ---------------------------------------------------------------------------
# 定时任务
# ---------------------------------------------------------------------------


def get_check_interval_seconds() -> int:
    """Read the authoritative business configuration after initial import."""
    raw = (getattr(ConfigManager.get_config(), 'cookie_check_interval_seconds', DEFAULT_CHECK_INTERVAL_SECONDS)
           if ConfigManager._initialized else os.getenv(ENV_CHECK_INTERVAL, str(DEFAULT_CHECK_INTERVAL_SECONDS)))
    try:
        seconds = int(raw)
    except (TypeError, ValueError):
        seconds = DEFAULT_CHECK_INTERVAL_SECONDS
    return max(MIN_CHECK_INTERVAL_SECONDS, min(MAX_CHECK_INTERVAL_SECONDS, seconds))


async def run_cookie_check_once() -> CookieCheckDetail:
    """
    执行一次 Cookie 检查并处理通知。

    返回检查详情。网络错误不误报过期。
    """
    cookie = CookieManager.get_cookie()
    detail = await check_cookie_validity(cookie)

    if detail.result == CookieCheckResult.VALID:
        logger.debug("B站 Cookie 有效")
        _email_notifications.reset()
    elif detail.result == CookieCheckResult.EXPIRED:
        logger.warning("B站 Cookie 已过期: %s", detail.message)
        await _notify_cookie_expired(detail)
    elif detail.result == CookieCheckResult.NETWORK_ERROR:
        logger.warning("B站 Cookie 检查网络异常: %s", detail.message)
        await _notify_cookie_check_abnormal(detail)
        # 网络异常只通知，不误判 Cookie 过期
    elif detail.result == CookieCheckResult.API_ERROR:
        logger.warning("B站 Cookie 检查 API 异常: %s", detail.message)
        await _notify_cookie_check_abnormal(detail)
        # API 异常也只通知，不误报过期

    return detail


async def cookie_monitor_loop() -> None:
    """
    Cookie 监控定时循环。

    启动后延迟执行首次检查，然后按配置周期循环检查。
    任何单次检查异常都不会终止循环。
    """
    interval = get_check_interval_seconds()
    logger.info("B站 Cookie 监控启动，检查周期: %d 秒", interval)

    # 启动后延迟执行首次检查，避免与其他初始化竞争
    await asyncio.sleep(STARTUP_CHECK_DELAY_SECONDS)

    # 首次检查也在异常保护内
    try:
        await run_cookie_check_once()
    except Exception:
        logger.exception("Cookie 首次检查异常，将在下次周期重试")

    while True:
        await asyncio.sleep(interval)
        try:
            await run_cookie_check_once()
        except Exception:
            logger.exception("Cookie 检查循环异常，将在下次周期重试")
