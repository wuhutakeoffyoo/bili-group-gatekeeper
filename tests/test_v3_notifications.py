"""Continuous alerts retry failed mail and notify again after a state change."""
import unittest
from unittest.mock import AsyncMock, patch

from app.bilibili import monitor


class CookieNotificationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        monitor.reset_dedup_state()
        self.addCleanup(monitor.reset_dedup_state)

    async def test_failed_delivery_retries_and_success_deduplicates_until_recovery(self):
        expired = monitor.CookieCheckDetail(monitor.CookieCheckResult.EXPIRED)
        valid = monitor.CookieCheckDetail(monitor.CookieCheckResult.VALID)
        sender = AsyncMock(side_effect=[False, True, True])
        with patch.object(monitor.CookieManager, "get_cookie", return_value="fixture"), patch.object(
            monitor, "check_cookie_validity", new=AsyncMock(side_effect=[expired, expired, expired, valid, expired])
        ), patch.object(monitor.email_notifier, "send_notification", sender):
            for _ in range(5):
                await monitor.run_cookie_check_once()
        self.assertEqual(sender.await_count, 3)

    async def test_issue_changes_notify_once_per_continuous_issue_without_reporting_false_expiry(self):
        network = monitor.CookieCheckDetail(monitor.CookieCheckResult.NETWORK_ERROR)
        api = monitor.CookieCheckDetail(monitor.CookieCheckResult.API_ERROR)
        expired = monitor.CookieCheckDetail(monitor.CookieCheckResult.EXPIRED)
        sender = AsyncMock(return_value=True)
        with patch.object(monitor.CookieManager, "get_cookie", return_value="fixture"), patch.object(
            monitor, "check_cookie_validity", new=AsyncMock(side_effect=[network, network, api, api, expired, network])
        ), patch.object(monitor.email_notifier, "send_notification", sender):
            for _ in range(6):
                await monitor.run_cookie_check_once()
        self.assertEqual(sender.await_count, 4)
        subjects = [call.args[0] for call in sender.await_args_list]
        self.assertIn("网络异常", subjects[0])
        self.assertIn("接口异常", subjects[1])
        self.assertIn("过期", subjects[2])
        self.assertIn("网络异常", subjects[3])
