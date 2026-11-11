"""
B 站 API 封装模块。

提供 B 站用户搜索、关注检查、粉丝牌检查等功能。
通过 httpx 异步客户端请求 B 站公开 API。
"""

import asyncio
import hashlib
import logging
import re
import secrets
import time
from http.cookies import CookieError, SimpleCookie
from typing import Optional
from urllib.parse import quote

import httpx

from src.gatekeeper.models import CheckResult
from src.gatekeeper.parsing import parse_bili_identity
from .pacing import HTTP_LIMITER, OBSERVATIONS, cached_observation

# 请求头模拟浏览器，避免被反爬
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    ),
    "Referer": "https://www.bilibili.com/",
}

logger = logging.getLogger("group_request_manager.bili")

FOLLOWINGS_PAGE_SIZE = 50
FOLLOWINGS_MAX_PAGES = 20

SEARCH_URL = "https://api.bilibili.com/x/web-interface/wbi/search/type"
SEARCH_FALLBACK_URL = "https://api.bilibili.com/x/web-interface/search/type"
NAV_URL = "https://api.bilibili.com/x/web-interface/nav"
UID_PROFILE_URL = "https://api.bilibili.com/x/web-interface/card"

ACCOUNT_INPUT_GUIDANCE = "请不要填写B站昵称或UID以外的内容"
NICKNAME_NOT_FOUND_MESSAGE = "未搜索到你填写的B站昵称，请检查昵称是否填写正确，或填写正确的UID后重新申请。" + ACCOUNT_INPUT_GUIDANCE
UID_NOT_FOUND_MESSAGE = "未查到你填写的B站UID，请检查UID是否填写正确后重新申请。" + ACCOUNT_INPUT_GUIDANCE
UID_INVALID_MESSAGE = "你填写的B站UID格式有误，请检查并填写正确的纯数字UID后重新申请。" + ACCOUNT_INPUT_GUIDANCE
ACCOUNT_REQUIRED_MESSAGE = "未填写B站账号，请检查并填写正确的UID或昵称后重新申请。" + ACCOUNT_INPUT_GUIDANCE
# 搜索结果分页不完整时不能断定“昵称不存在”，只能按无法判断处理
SEARCH_INCOMPLETE_MESSAGE = "B站搜索结果不完整，无法确认是否存在该昵称"

# Wbi 密钥全站统一且每日更替；进程内缓存并定期刷新，避免每次搜索都请求 nav
_WBI_KEY_TTL_SECONDS = 6 * 3600
_wbi_key_cache: dict = {"keys": None, "fetched_at": 0.0}
_wbi_key_lock = asyncio.Lock()

# 社区实现中记录的 WBI 混淆表；B站未对这些网页接口提供稳定性保证。
MIXIN_KEY_ENC_TAB = (
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49,
    33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40,
    61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11,
    36, 20, 34, 44, 52,
)


def _wbi_key_from_url(url: object) -> str:
    """从伪装成 png 地址的实时口令中截取密钥文件名。"""
    if not isinstance(url, str) or not url:
        return ""
    filename = url.rstrip("/").rsplit("/", 1)[-1]
    return filename.removesuffix(".png")


def _get_mixin_key(img_key: str, sub_key: str) -> str:
    """按 WBI 混淆表重排 img_key + sub_key，得到签名用的 mixin_key。"""
    raw_wbi_key = img_key + sub_key
    return "".join(
        raw_wbi_key[index] for index in MIXIN_KEY_ENC_TAB if index < len(raw_wbi_key)
    )[:32]


def _wbi_sign_query(params: dict, img_key: str, sub_key: str, *, wts: int) -> str:
    """按 Wbi 算法对请求参数签名，返回可直接发送的 query 字符串。

    编码必须与参与签名的字符串完全一致（十六进制大写、空格编码为 %20），
    因此这里手动序列化后整体交给 HTTP 客户端，避免客户端二次编码
    （如空格编码为 +）导致签名失效。
    """
    mixin_key = _get_mixin_key(img_key, sub_key)
    signed_items = {**params, "wts": wts}
    encoded_pairs = []
    for key in sorted(signed_items):
        value = "".join(ch for ch in str(signed_items[key]) if ch not in "!'()*")
        encoded_pairs.append(f"{quote(str(key), safe='')}={quote(value, safe='')}")
    query = "&".join(encoded_pairs)
    w_rid = hashlib.md5((query + mixin_key).encode("utf-8")).hexdigest()
    return f"{query}&w_rid={w_rid}"


def reset_wbi_key_cache() -> None:
    """清空 Wbi 密钥进程内缓存；测试或 Cookie 更换后可用。"""
    _wbi_key_cache["keys"] = None
    _wbi_key_cache["fetched_at"] = 0.0


def create_bili_http_client(
    *, max_connections: int = 8, max_keepalive_connections: int = 4
) -> httpx.AsyncClient:
    """Create a consistently configured client for Bilibili API traffic."""
    return httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=5.0),
        limits=httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
        ),
    )


class BiliApi:
    """B 站 API 客户端，封装搜索用户、检查关注和粉丝牌的方法。"""

    def __init__(
        self, cookie: str = "", *, client: httpx.AsyncClient | None = None, cache_scope: str = ""
    ):
        self.cookie = cookie
        self._owns_client = client is None
        self.client = client or create_bili_http_client()
        self.cache_scope = cache_scope

    async def close(self) -> None:
        """关闭 HTTP 客户端，释放连接资源。"""
        if self._owns_client:
            await self.client.aclose()

    async def _get(self, url: str, params: Optional[dict] = None) -> dict:
        """
        发起 GET 请求并返回 JSON 响应。

        自动附带 Cookie 和 User-Agent。传输异常和 502/503/504 最多重试两次，
        每次实际请求均受共享限流器约束；耗尽重试或响应无效时保留异常。
        """
        headers = HEADERS.copy()
        if self.cookie:
            headers["Cookie"] = self.cookie
        for attempt in range(3):
            await HTTP_LIMITER.acquire()
            try:
                response = await self.client.get(url, headers=headers, params=params)
            except httpx.RequestError:
                if attempt == 2:
                    raise
            else:
                if response.status_code not in {502, 503, 504} or attempt == 2:
                    response.raise_for_status()
                    data = response.json()
                    return data if isinstance(data, dict) else {}
            await asyncio.sleep(0.25 * (2**attempt) + secrets.randbelow(251) / 1000)
        return {}

    async def _post(self, url: str, data: Optional[dict] = None) -> dict:
        """发起带登录 Cookie 的表单 POST 请求并返回 JSON 对象。"""
        headers = HEADERS.copy()
        if self.cookie:
            headers["Cookie"] = self.cookie
        await HTTP_LIMITER.acquire()
        response = await self.client.post(url, headers=headers, data=data)
        OBSERVATIONS.clear()
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def _cookie_value(self, name: str) -> str:
        """从 Cookie 字符串安全提取指定字段。"""
        jar = SimpleCookie()
        try:
            jar.load(self.cookie)
        except CookieError:
            return ""
        morsel = jar.get(name)
        return morsel.value if morsel else ""

    # ------------------------------------------------------------------
    # 用户搜索
    # ------------------------------------------------------------------

    @cached_observation
    async def resolve_user_profile(self, answer: str) -> CheckResult:
        """Route cleaned UID input to a UID lookup, and character input to search."""
        identity = parse_bili_identity(answer)
        if identity.kind == "uid":
            return await self.get_user_profile_by_uid(identity.value)
        return await self.search_user_profile(identity.value)

    async def get_user_profile_by_uid(self, uid: int | str) -> CheckResult:
        """Resolve only the requested UID; never fall back to nickname search."""
        text = str(uid)
        if (isinstance(uid, bool) or not re.fullmatch(r"[0-9]{1,20}", text)
                or not 0 < int(text) <= 2**63 - 1):
            return CheckResult("failed", UID_INVALID_MESSAGE)
        requested_uid = int(text)
        try:
            data = await self._get(UID_PROFILE_URL, {"mid": requested_uid})
        except Exception:
            logger.warning("按 UID 查询 B站账号失败, uid=%s", requested_uid, exc_info=True)
            return CheckResult("inaccessible", "B站UID查询接口请求失败，请稍后重试")
        if data.get("code") == -404:
            return CheckResult("failed", UID_NOT_FOUND_MESSAGE)
        if data.get("code") != 0:
            return CheckResult("inaccessible", f"B站UID查询接口不可用(code={data.get('code')})，请稍后重试")
        payload = data.get("data")
        card = payload.get("card") if isinstance(payload, dict) else None
        level_info = card.get("level_info") if isinstance(card, dict) else None
        if not isinstance(card, dict) or not isinstance(level_info, dict):
            return CheckResult("inaccessible", "B站UID资料不完整，无法确认账号和等级")
        try:
            actual_uid, level = card["mid"], level_info["current_level"]
            if isinstance(actual_uid, bool) or isinstance(level, bool):
                raise ValueError("boolean account metadata")
            if not re.fullmatch(r"[0-9]+", str(actual_uid)) or not re.fullmatch(r"[0-9]+", str(level)):
                raise ValueError("invalid account metadata")
            actual_uid, level = int(actual_uid), int(level)
        except (KeyError, ValueError, TypeError):
            return CheckResult("inaccessible", "B站UID资料不完整，无法确认账号和等级")
        if actual_uid != requested_uid:
            return CheckResult("inaccessible", "B站UID查询结果与输入不一致，无法确认账号")
        name = card.get("name")
        name = name.strip() if isinstance(name, str) else ""
        return CheckResult("passed", matched_target_uid=actual_uid, matched_level=level, matched_name=name or None)

    async def _get_wbi_keys(self) -> Optional[tuple[str, str]]:
        """获取 Wbi 签名密钥；进程内缓存定期刷新，nav 失败时返回 None。"""
        now = time.monotonic()
        cached = _wbi_key_cache["keys"]
        if cached and now - _wbi_key_cache["fetched_at"] < _WBI_KEY_TTL_SECONDS:
            return cached
        async with _wbi_key_lock:
            cached = _wbi_key_cache["keys"]
            if (
                cached
                and time.monotonic() - _wbi_key_cache["fetched_at"]
                < _WBI_KEY_TTL_SECONDS
            ):
                return cached
            try:
                data = await self._get(NAV_URL)
            except Exception:
                logger.warning(
                    "获取 Wbi 签名密钥失败，搜索将回退到未签名路径", exc_info=True
                )
                return None
            payload = data.get("data") if isinstance(data, dict) else None
            wbi_img = payload.get("wbi_img") if isinstance(payload, dict) else None
            if not isinstance(wbi_img, dict):
                logger.warning("Wbi 签名密钥响应结构异常，搜索将回退到未签名路径")
                return None
            img_key = _wbi_key_from_url(wbi_img.get("img_url"))
            sub_key = _wbi_key_from_url(wbi_img.get("sub_url"))
            if not img_key or not sub_key:
                logger.warning("Wbi 签名密钥内容缺失，搜索将回退到未签名路径")
                return None
            _wbi_key_cache["keys"] = (img_key, sub_key)
            _wbi_key_cache["fetched_at"] = time.monotonic()
            return img_key, sub_key

    async def _search_bili_users(self, keyword: str) -> dict:
        """请求 B站用户搜索接口；优先 Wbi 签名路径，密钥不可用时回退。"""
        params = {
            "search_type": "bili_user",
            "keyword": keyword,
            "page": 1,
            "page_size": 20,
        }
        keys = await self._get_wbi_keys()
        if keys is None:
            return await self._get(SEARCH_FALLBACK_URL, params)
        if self.cookie and "buvid3=" not in self.cookie:
            logger.info("当前 Cookie 缺少 buvid3，带签名的搜索请求可能被风控拦截")
        query = _wbi_sign_query(params, keys[0], keys[1], wts=int(time.time()))
        return await self._get(SEARCH_URL, query)

    @staticmethod
    def _search_results_complete(payload: dict, users: list) -> bool:
        """判断第 1 页搜索结果能否代表完整结果集。

        numResults 为 B站返回的总条数；字段缺失、非法或大于实际返回条数
        时都视为不完整，避免把分页截断误判成“昵称不存在”。
        """
        try:
            total = int(payload.get("numResults"))
        except (TypeError, ValueError):
            return False
        if total < 0:
            return False
        return total <= len(users)

    async def search_user_profile(self, keyword: str) -> CheckResult:
        """根据昵称搜索用户，并返回 UID、等级及可判定状态。

        搜索请求优先携带 Wbi 签名以降低风控风险。第 1 页没有精确匹配、
        且结果分页不完整（numResults 超过实际返回条数或字段缺失）时，
        按“无法判断”处理，而不是断定昵称不存在。
        """
        if not keyword.strip():
            return CheckResult("failed", ACCOUNT_REQUIRED_MESSAGE)

        try:
            data = await self._search_bili_users(keyword)
        except Exception:
            logger.warning("搜索 B站用户失败, keyword=%s", keyword, exc_info=True)
            return CheckResult("inaccessible", "B站搜索接口请求失败")

        if data.get("code") != 0:
            code = data.get("code")
            logger.info("B站搜索接口返回非 0, keyword=%s, code=%s", keyword, code)
            return CheckResult("inaccessible", f"B站搜索接口不可用(code={code})")

        payload = data.get("data")
        payload = payload if isinstance(payload, dict) else {}
        users = payload.get("result", [])

        if not users:
            if not self._search_results_complete(payload, users):
                logger.info(
                    "B站搜索结果分页不完整, 无法据此断定昵称不存在, keyword=%s",
                    keyword,
                )
                return CheckResult("inaccessible", SEARCH_INCOMPLETE_MESSAGE)
            logger.info("未找到匹配的 B站用户, keyword=%s", keyword)
            return CheckResult("failed", NICKNAME_NOT_FOUND_MESSAGE)

        exact_matches = [
            user
            for user in users
            if str(user.get("uname", "")).strip().lower() == keyword.strip().lower()
        ]
        if not exact_matches:
            if not self._search_results_complete(payload, users):
                logger.info(
                    "B站搜索结果分页不完整, 无法排除后续分页存在精确匹配, keyword=%s",
                    keyword,
                )
                return CheckResult("inaccessible", SEARCH_INCOMPLETE_MESSAGE)
            logger.info("B站搜索结果中没有昵称精确匹配, keyword=%s", keyword)
            return CheckResult("failed", NICKNAME_NOT_FOUND_MESSAGE)
        if len(exact_matches) != 1:
            logger.info(
                "B站搜索到多个同名精确匹配，无法唯一确定账号, keyword=%s, count=%d",
                keyword,
                len(exact_matches),
            )
            return CheckResult(
                "failed",
                "搜索到多个同名B站账号，无法唯一确定账号，请检查昵称或填写正确的UID后重新申请。" + ACCOUNT_INPUT_GUIDANCE,
            )

        selected_user = exact_matches[0]
        try:
            uid = int(selected_user["mid"])
            level = int(selected_user["level"])
        except (KeyError, ValueError, TypeError):
            logger.warning("无法从搜索结果中解析 UID 或等级, keyword=%s, user=%s", keyword, selected_user)
            return CheckResult("inaccessible", "B站账号资料不完整，无法读取 UID 或等级")
        return CheckResult("passed", matched_target_uid=uid, matched_level=level,
                           matched_name=str(selected_user["uname"]).strip())

    async def search_user(self, keyword: str) -> Optional[int]:
        """根据昵称搜索 B 站用户，返回匹配的 UID。

        仅在存在唯一精确匹配（大小写不敏感）时返回其 UID；
        无精确匹配、存在多个同名精确匹配或结果无法判读时返回 None。
        """
        result = await self.search_user_profile(keyword)
        return result.matched_target_uid if result.state == "passed" else None

    # ------------------------------------------------------------------
    # 关注检查

    async def check_common_follow_any_status(
        self, uid: int, target_uids: list[int]
    ) -> CheckResult:
        """检查当前登录账号与 uid 的共同关注中是否存在任一目标。"""
        if not target_uids:
            return CheckResult("passed")

        url = "https://api.bilibili.com/x/relation/same/followings"
        targets = set(target_uids)
        seen_uids: set[int] = set()
        expected_total: Optional[int] = None

        for page in range(1, FOLLOWINGS_MAX_PAGES + 1):
            try:
                data = await self._get(
                    url,
                    {"vmid": uid, "pn": page, "ps": FOLLOWINGS_PAGE_SIZE},
                )
            except Exception:
                logger.warning("获取共同关注失败, uid=%d, page=%d", uid, page, exc_info=True)
                return CheckResult("inaccessible", "共同关注接口请求失败")

            code = data.get("code")
            if code != 0:
                message = str(data.get("message") or data.get("msg") or "")
                return CheckResult(
                    "inaccessible",
                    f"共同关注不可见或无法读取(code={code}, message={message})",
                )

            payload = data.get("data")
            if not isinstance(payload, dict) or not isinstance(payload.get("list"), list):
                return CheckResult("inaccessible", "共同关注接口返回结构异常")

            try:
                page_uids = {
                    int(user["mid"])
                    for user in payload["list"]
                    if isinstance(user, dict) and user.get("mid") is not None
                }
                page_total = int(payload.get("total"))
            except (TypeError, ValueError):
                return CheckResult("inaccessible", "共同关注接口返回数据异常")
            if page_total < 0:
                return CheckResult("inaccessible", "共同关注总数信息异常")

            matched = targets & page_uids
            if matched:
                target_uid = next(uid for uid in target_uids if uid in matched)
                return CheckResult("passed", matched_target_uid=target_uid)

            seen_uids.update(page_uids)
            expected_total = max(expected_total or 0, page_total)
            if len(seen_uids) >= expected_total:
                return CheckResult("failed", "共同关注中未找到目标主播")
            if not payload["list"]:
                return CheckResult("inaccessible", "共同关注列表未能完整读取")

        return CheckResult(
            "inaccessible",
            f"共同关注达到分页上限，仅能读取 {len(seen_uids)}/{expected_total or '?'} 项",
        )

    async def check_following_any_status(
        self, uid: int, target_uids: list[int]
    ) -> CheckResult:
        """
        扫描一次关注列表，检查 uid 是否关注任一目标并区分失败原因。

        - passed: 已关注
        - failed: 明确未关注
        - inaccessible: 关注列表不可见或接口无法正常读取
        """
        if not target_uids:
            return CheckResult("passed")

        url = "https://api.bilibili.com/x/relation/followings"
        targets = set(target_uids)
        seen_uids: set[int] = set()
        expected_total: Optional[int] = None

        for page in range(1, FOLLOWINGS_MAX_PAGES + 1):
            params = {
                "vmid": uid,
                "pn": page,
                "ps": FOLLOWINGS_PAGE_SIZE,
            }
            try:
                data = await self._get(url, params)
            except Exception:
                logger.warning(
                    "获取关注列表失败, uid=%d, page=%d", uid, page, exc_info=True
                )
                return CheckResult("inaccessible", "关注列表接口请求失败")

            code = data.get("code")
            if code != 0:
                message = str(data.get("message") or data.get("msg") or "")
                logger.info(
                    "关注列表接口返回非 0, uid=%d, targets=%s, page=%d, code=%s, message=%s",
                    uid,
                    target_uids,
                    page,
                    code,
                    message,
                )
                return CheckResult(
                    "inaccessible",
                    f"关注列表不可见或无法读取(code={code}, message={message})",
                )

            payload = data.get("data")
            if not isinstance(payload, dict) or not isinstance(payload.get("list"), list):
                return CheckResult("inaccessible", "关注列表接口返回结构异常")

            followings = payload["list"]
            try:
                page_uids = {
                    int(user["mid"])
                    for user in followings
                    if isinstance(user, dict) and user.get("mid") is not None
                }
            except (TypeError, ValueError):
                return CheckResult("inaccessible", "关注列表包含无法识别的用户数据")

            matched = targets & page_uids
            if matched:
                target_uid = next(uid for uid in target_uids if uid in matched)
                return CheckResult("passed", matched_target_uid=target_uid)

            seen_uids.update(page_uids)

            raw_total = payload.get("total")
            try:
                page_total = int(raw_total)
            except (TypeError, ValueError):
                return CheckResult("inaccessible", "关注列表缺少有效的总数信息")
            if page_total < 0:
                return CheckResult("inaccessible", "关注列表总数信息异常")
            expected_total = max(expected_total or 0, page_total)

            if len(seen_uids) >= expected_total:
                return CheckResult(
                    "failed",
                    "未关注任一目标 UID：" + "、".join(str(uid) for uid in target_uids),
                )

            if not followings:
                return CheckResult(
                    "inaccessible",
                    f"关注列表仅能读取 {len(seen_uids)}/{expected_total} 项",
                )

        return CheckResult(
            "inaccessible",
            f"关注列表达到分页上限，仅能读取 {len(seen_uids)}/{expected_total or '?'} 项",
        )

    async def check_follow_status(self, uid: int, target_uid: int) -> CheckResult:
        """Compatibility wrapper for checking a single target UID."""
        return await self.check_following_any_status(uid, [target_uid])

    # ------------------------------------------------------------------
    # 关注检查（多目标汇总）

    @cached_observation
    async def check_follow_any_status(
        self,
        uid: int,
        target_uids: list[int],
        *,
        common_negative_is_definitive: bool = True,
    ) -> CheckResult:
        """检查是否关注了任意一个目标 UID，并区分失败与不可见。"""
        if not target_uids:
            return CheckResult("passed")

        common_result = await self.check_common_follow_any_status(uid, target_uids)
        if common_result.state == "passed" or (
            common_result.state == "failed" and common_negative_is_definitive
        ):
            return common_result

        if common_result.state == 'failed' and not common_negative_is_definitive:
            # Startup verification expires: an operator may unfollow a target
            # later. Refresh the prerequisite instead of trusting it forever.
            relations = [await self.get_login_account_relation(target) for target in target_uids]
            if all(relation.state == 'passed' for relation in relations):
                refreshed = await self.check_common_follow_any_status(uid, target_uids)
                if refreshed.state in {'passed', 'failed'}:
                    return refreshed

        common_detail = common_result.detail.strip()
        result = await self.check_following_any_status(uid, target_uids)
        if result.state != "inaccessible":
            return result
        owner_result = await self.check_follow_from_owner(uid, target_uids)
        if owner_result.state in {'passed', 'failed'}:
            return owner_result
        if result.detail:
            diagnostic_details = [common_detail, result.detail, owner_result.detail]
            return CheckResult(
                "inaccessible",
                "；".join(detail for detail in dict.fromkeys(diagnostic_details) if detail),
            )
        return CheckResult("inaccessible", common_detail or "关注列表无法读取")

    async def check_follow_from_owner(self, uid: int, target_uids: list[int]) -> CheckResult:
        """Only the validated target account may use its own followers list.

        A hit is positive evidence. A negative is returned only for a single
        target and an entirely parsed, complete list; multi-target unknowns
        cannot be turned into a negative by checking just one owner.
        """
        try:
            nav = await self._get(NAV_URL)
            owner = nav.get('data') or {}
            owner_uid = int(owner.get('mid', 0))
            if nav.get('code') != 0 or owner.get('isLogin') is not True or owner_uid not in target_uids:
                return CheckResult('inaccessible', '当前登录账号不是目标主播，无法使用主播视角核验')
            seen, expected = set(), 0
            for page in range(1, FOLLOWINGS_MAX_PAGES + 1):
                payload = await self._get('https://api.bilibili.com/x/relation/followers', {
                    'vmid': owner_uid, 'pn': page, 'ps': FOLLOWINGS_PAGE_SIZE, 'order': 'desc'})
                data = payload.get('data') or {}
                rows = data.get('list')
                if payload.get('code') != 0 or not isinstance(rows, list):
                    return CheckResult('inaccessible', '主播粉丝列表暂时无法完整读取')
                complete = all(isinstance(row, dict) and str(row.get('mid', '')).isdigit() for row in rows)
                values = {int(row['mid']) for row in rows if isinstance(row, dict) and str(row.get('mid', '')).isdigit()}
                if uid in values:
                    return CheckResult('passed', '已从目标主播本人的粉丝列表确认', matched_target_uid=owner_uid)
                if not complete or not str(data.get('total', '')).isdigit():
                    return CheckResult('inaccessible', '主播粉丝列表包含缺失字段，无法判断未关注')
                expected = max(expected, int(data['total']))
                seen.update(values)
                if len(seen) >= expected:
                    if set(target_uids) == {owner_uid}:
                        return CheckResult('failed', f'目标主播UID {owner_uid} 的完整粉丝列表中未找到此账号')
                    return CheckResult('inaccessible', '只核验了当前主播，其余目标的关注状态仍无法判断')
                if not rows:
                    break
            return CheckResult('inaccessible', '主播粉丝列表未完整读取，无法判断未关注')
        except Exception:
            return CheckResult('inaccessible', '主播视角关注核验暂时不可用')

    # ------------------------------------------------------------------
    # 当前登录账号关系维护

    @cached_observation
    async def get_login_account_relation(self, target_uid: int) -> CheckResult:
        """查询当前登录账号是否关注目标 UID。"""
        try:
            data = await self._get(
                "https://api.bilibili.com/x/relation",
                {"fid": target_uid},
            )
        except Exception:
            logger.warning("查询登录账号关注关系失败, target_uid=%d", target_uid, exc_info=True)
            return CheckResult("inaccessible", "关注关系接口请求失败")

        code = data.get("code")
        if code != 0:
            message = str(data.get("message") or data.get("msg") or "")
            return CheckResult(
                "inaccessible",
                f"关注关系接口不可用(code={code}, message={message})",
            )

        payload = data.get("data")
        try:
            attribute = int(payload["attribute"])
        except (KeyError, TypeError, ValueError):
            return CheckResult("inaccessible", "关注关系接口返回结构异常")

        if attribute in {2, 6}:
            return CheckResult("passed", "已关注", matched_target_uid=target_uid)
        if attribute == 0:
            return CheckResult("failed", "未关注", matched_target_uid=target_uid)
        return CheckResult("inaccessible", f"无法处理的关注关系状态：{attribute}")

    async def follow_target(self, target_uid: int) -> CheckResult:
        """使用当前登录账号关注目标 UID。"""
        csrf = self._cookie_value("bili_jct")
        if not csrf:
            return CheckResult("inaccessible", "Cookie 缺少 bili_jct，无法自动关注")

        try:
            data = await self._post(
                "https://api.bilibili.com/x/relation/modify",
                {"fid": target_uid, "act": 1, "re_src": 11, "csrf": csrf},
            )
        except Exception:
            logger.warning("自动关注目标主播失败, target_uid=%d", target_uid, exc_info=True)
            return CheckResult("inaccessible", "自动关注接口请求失败")

        code = data.get("code")
        if code == 0:
            return CheckResult("passed", "已自动关注", matched_target_uid=target_uid)
        message = str(data.get("message") or data.get("msg") or "")
        return CheckResult(
            "inaccessible",
            f"自动关注失败(code={code}, message={message})",
        )

    async def ensure_following_targets(
        self, target_uids: list[int]
    ) -> dict[int, CheckResult]:
        """确保当前登录账号关注所有目标 UID；仅在明确未关注时执行关注。"""
        results: dict[int, CheckResult] = {}
        for target_uid in dict.fromkeys(target_uids):
            relation = await self.get_login_account_relation(target_uid)
            if relation.state == "failed":
                relation = await self.follow_target(target_uid)
            results[target_uid] = relation
        return results

    # ------------------------------------------------------------------
    # 粉丝牌检查

    async def get_user_medals_status(self, uid: int) -> tuple[str, list[tuple[int, int]], str]:
        """
        获取用户粉丝牌并区分结果状态。

        返回 (状态, 粉丝牌列表, 说明)。
        """
        url = "https://api.live.bilibili.com/xlive/web-ucenter/user/MedalWall"
        params = {"target_id": uid}
        try:
            data = await self._get(url, params)
        except Exception:
            logger.warning("获取粉丝牌列表失败, uid=%d", uid, exc_info=True)
            return "inaccessible", [], "粉丝牌列表接口请求失败"

        code = data.get("code")
        if code != 0:
            message = str(data.get("message") or data.get("msg") or "")
            logger.info(
                "粉丝牌接口返回非 0, uid=%d, code=%s, message=%s",
                uid,
                code,
                message,
            )
            return (
                "inaccessible",
                [],
                f"粉丝牌列表不可见或无法读取(code={code}, message={message})",
            )

        medal_data = data.get("data")
        if not isinstance(medal_data, dict):
            return "inaccessible", [], "粉丝牌接口未返回有效列表"

        if medal_data.get("close_space_medal"):
            return "inaccessible", [], "用户已关闭粉丝牌展示，无法获取完整粉丝牌列表"

        if medal_data.get("only_show_wearing"):
            return "inaccessible", [], "用户仅展示当前佩戴的粉丝牌，无法获取完整粉丝牌列表"

        medals = medal_data.get("list")
        if not isinstance(medals, list):
            return "inaccessible", [], "粉丝牌接口未返回有效列表"

        medals, complete = self._parse_medals(data)
        if not complete:
            return "incomplete", medals, "粉丝牌列表包含缺失或无法解析的字段"
        return "ok", medals, ""

    def _parse_medals(self, data: dict) -> tuple[list[tuple[int, int]], bool]:
        """解析粉丝牌列表，并标记是否每个条目都可判读。"""
        medals: list[tuple[int, int]] = []
        complete = True
        for item in data.get("data", {}).get("list", []):
            if not isinstance(item, dict):
                complete = False
                continue
            medal_info = (
                item.get("medal_info")
                or item.get("medal")
                or item.get("uinfo_medal")
                or {}
            )
            if not isinstance(medal_info, dict):
                complete = False
                continue
            uinfo_medal = item.get("uinfo_medal")
            anchor_info = item.get("anchor_info")
            anchor_uid_raw = (
                medal_info.get("target_id")
                or medal_info.get("ruid")
                or (uinfo_medal.get("ruid") if isinstance(uinfo_medal, dict) else None)
                or (anchor_info.get("uid") if isinstance(anchor_info, dict) else None)
            )
            try:
                anchor_uid = int(anchor_uid_raw)
                level = int(medal_info["level"])
            except (KeyError, ValueError, TypeError):
                complete = False
                continue
            if anchor_uid <= 0 or level < 0:
                complete = False
                continue
            medals.append((anchor_uid, level))
        return medals, complete

    @cached_observation
    async def check_medal_any_status(
        self, uid: int, target_uids: list[int], min_level: int
    ) -> CheckResult:
        """检查用户是否满足任一目标粉丝牌要求，并区分失败与不可见。"""
        if not target_uids:
            return CheckResult("passed")

        status, medals, detail = await self.get_user_medals_status(uid)
        target_uid_set = set(target_uids)
        candidates = [(level, anchor_uid) for anchor_uid, level in medals if anchor_uid in target_uid_set]
        best_level, best_uid = max(candidates, default=(0, None))
        if best_uid is not None and best_level >= min_level:
            return CheckResult('passed', detail if status != 'ok' else '', matched_target_uid=best_uid,
                               matched_level=best_level if status == 'ok' else None)

        if status != "ok":
            return CheckResult("inaccessible", detail)

        return CheckResult(
            "failed",
            (
                "未持有目标粉丝牌，或等级不足："
                + "、".join(str(uid) for uid in target_uids)
                + f"（要求 >= {min_level} 级）"
            ),
            matched_level=best_level,
        )
