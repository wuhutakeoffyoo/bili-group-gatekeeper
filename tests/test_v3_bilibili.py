"""Standalone wire-protocol fixtures and cache/pacing/credential invariants."""
import asyncio
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs

import httpx

from app.bilibili.client import BiliApi, reset_wbi_key_cache
from app.bilibili.credentials import CredentialService, cookie_fields
from app.bilibili.applicant_qr import ApplicantQrService
from app.bilibili.runtime import BiliRequestCoordinator
from app.bilibili.pacing import ObservationCache, RequestLimiter
from app.bilibili.proof import ProofClient
from app.config import ConfigManager
from src.gatekeeper.models import CheckResult


class PacingTests(unittest.IsolatedAsyncioTestCase):
    async def test_startup_follow_prerequisite_expires_instead_of_remaining_true_forever(self):
        coordinator = BiliRequestCoordinator()
        with patch('app.bilibili.runtime.time.monotonic', return_value=100):
            coordinator.set_verified_common_targets('cookie', {8})
            self.assertTrue(coordinator.common_negative_is_definitive('cookie', [8]))
            self.assertFalse(coordinator.common_negative_is_definitive('other-cookie', [8]))
        with patch('app.bilibili.runtime.time.monotonic', return_value=161):
            self.assertFalse(coordinator.common_negative_is_definitive('cookie', [8]))

    async def test_all_waiters_are_spaced_at_actual_wire_reservation(self):
        now, calls = [0.], []
        async def sleep(delay):
            now[0] += delay
        limiter = RequestLimiter(2, clock=lambda: now[0], sleep=sleep)
        async def worker():
            await limiter.acquire()
            calls.append(now[0])
        await asyncio.gather(*(worker() for _ in range(5)))
        self.assertEqual(calls, [0., .5, 1., 1.5, 2.])
        self.assertEqual(limiter.metrics()['waiting'], 0)

    async def test_cache_coalesces_cancellation_copies_ttl_and_shutdown(self):
        now, called, gate = [0.], [], asyncio.Event()
        cache = ObservationCache(5, max_entries=2, clock=lambda: now[0])
        async def load():
            called.append(True)
            await gate.wait()
            return CheckResult('passed', 'original')
        first = asyncio.create_task(cache.get_or_load('same', load))
        second = asyncio.create_task(cache.get_or_load('same', load))
        await asyncio.sleep(0)
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        gate.set()
        result = await second
        self.assertEqual(len(called), 1)
        result.detail = 'mutated'
        self.assertEqual((await cache.get_or_load('same', load)).detail, 'original')
        now[0] = 6
        await cache.get_or_load('same', load)
        self.assertEqual(len(called), 2)
        gate.clear()
        pending = asyncio.create_task(cache.get_or_load('pending', load))
        await asyncio.sleep(0)
        await cache.close()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        self.assertFalse(cache.inflight)

    async def test_unknown_never_cached_and_clear_invalidates_inflight(self):
        cache = ObservationCache(max_entries=2)
        load = AsyncMock(return_value=CheckResult('inaccessible'))
        await cache.get_or_load('unknown', load)
        await cache.get_or_load('unknown', load)
        self.assertEqual(load.await_count, 2)
        gate = asyncio.Event()
        async def slow():
            await gate.wait()
            return CheckResult('passed')
        task = asyncio.create_task(cache.get_or_load('old-generation', slow))
        await asyncio.sleep(0)
        cache.clear()
        gate.set()
        await task
        self.assertNotIn('old-generation', cache.values)
        for item in range(4):
            await cache.get_or_load(item, AsyncMock(return_value=CheckResult('passed')))
        self.assertLessEqual(len(cache.values), 2)

    async def test_last_cancelled_waiter_cancels_unneeded_http_loader(self):
        cache, started, stopped = ObservationCache(), asyncio.Event(), asyncio.Event()
        async def load():
            try:
                started.set()
                await asyncio.Event().wait()
            finally:
                stopped.set()
        waiter = asyncio.create_task(cache.get_or_load('deadline', load))
        await started.wait()
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        await asyncio.wait_for(stopped.wait(), 1)
        self.assertFalse(cache.inflight)


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.limiters = [patch('app.bilibili.client.HTTP_LIMITER.acquire', new=AsyncMock()),
                         patch('app.bilibili.credentials.HTTP_LIMITER.acquire', new=AsyncMock())]
        for limiter in self.limiters:
            limiter.start()
        reset_wbi_key_cache()

    async def asyncTearDown(self):
        for limiter in reversed(self.limiters):
            limiter.stop()

    async def test_get_retries_transport_and_gateway_failures_with_shared_budget_and_pacing(self):
        attempts = []
        def respond(request):
            attempts.append(request)
            if len(attempts) == 1:
                raise httpx.ConnectError("connection reset", request=request)
            return httpx.Response(503 if len(attempts) == 2 else 200, json={"code": 0})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            api = BiliApi("SESSDATA=fixture", client=client)
            with patch("app.bilibili.client.asyncio.sleep", new=AsyncMock()) as sleep, patch(
                "app.bilibili.client.HTTP_LIMITER.acquire", new=AsyncMock()
            ) as acquire:
                self.assertEqual(await api._get("https://example.invalid/list", {"pn": 2}), {"code": 0})
            self.assertEqual(acquire.await_count, 3)
            self.assertEqual(sleep.await_count, 2)
            self.assertTrue(all(request.url.params["pn"] == "2" for request in attempts))
            self.assertTrue(all(request.headers["Cookie"] == "SESSDATA=fixture" for request in attempts))

    async def test_exhausted_get_transport_errors_stay_inaccessible_and_cancellation_is_immediate(self):
        for error_type, count in [(httpx.ReadTimeout, 3), (asyncio.CancelledError, 1)]:
            attempts = []
            def respond(request):
                attempts.append(request)
                if error_type is asyncio.CancelledError:
                    raise asyncio.CancelledError()
                raise error_type("timeout", request=request)
            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                api = BiliApi(client=client)
                with patch("app.bilibili.client.asyncio.sleep", new=AsyncMock()) as sleep:
                    if error_type is asyncio.CancelledError:
                        with self.assertRaises(asyncio.CancelledError):
                            await api.get_user_profile_by_uid(42)
                    else:
                        self.assertEqual((await api.get_user_profile_by_uid(42)).state, "inaccessible")
                self.assertEqual(len(attempts), count)
                self.assertEqual(sleep.await_count, count - 1)

    async def test_get_does_not_retry_http_rejections_or_invalid_json(self):
        for response in [httpx.Response(403), httpx.Response(429), httpx.Response(200, text="not JSON")]:
            requests = []
            def respond(request):
                requests.append(request)
                return response
            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                with patch("app.bilibili.client.asyncio.sleep", new=AsyncMock()) as sleep:
                    with self.assertRaises((httpx.HTTPStatusError, ValueError)):
                        await BiliApi(client=client)._get("https://example.invalid/list")
                self.assertEqual(len(requests), 1)
                sleep.assert_not_awaited()

    async def test_uid_outside_storage_range_fails_locally_without_search(self):
        api = BiliApi('fixture')
        api._get = AsyncMock()
        try:
            for text in ('9223372036854775808', 'UID:99999999999999999999'):
                result = await api.resolve_user_profile(text)
                self.assertEqual(result.state, 'failed')
                self.assertIn('请检查', result.detail)
                self.assertIn('不要填写B站昵称或UID以外', result.detail)
            api._get.assert_not_awaited()
        finally:
            await api.close()

    async def test_owner_fallback_requires_authenticated_target_and_complete_scope(self):
        api = BiliApi('fixture')
        try:
            api._get = AsyncMock(side_effect=[{'code': 0, 'data': {'isLogin': True, 'mid': 8}},
                                             {'code': 0, 'data': {'list': [{'mid': 42}], 'total': 300}}])
            self.assertEqual((await api.check_follow_from_owner(42, [8])).state, 'passed')
            api._get = AsyncMock(return_value={'code': 0, 'data': {'isLogin': True, 'mid': 9}})
            self.assertEqual((await api.check_follow_from_owner(42, [8])).state, 'inaccessible')
            self.assertEqual(api._get.await_count, 1)
            for targets, expected in [([8], 'failed'), ([8, 9], 'inaccessible')]:
                api._get = AsyncMock(side_effect=[{'code': 0, 'data': {'isLogin': True, 'mid': 8}},
                                                 {'code': 0, 'data': {'list': [], 'total': 0}}])
                self.assertEqual((await api.check_follow_from_owner(42, targets)).state, expected)
            api._get = AsyncMock(side_effect=[{'code': 0, 'data': {'isLogin': True, 'mid': 8}},
                                             {'code': 0, 'data': {'list': [{'mid': 9}], 'total': 20}},
                                             {'code': 0, 'data': {'list': [], 'total': 20}}])
            self.assertEqual((await api.check_follow_from_owner(42, [8])).state, 'inaccessible')
        finally:
            await api.close()

    async def test_dynamic_proof_only_accepts_exact_author_fresh_nonce(self):
        api = ProofClient('fixture')
        try:
            now = int(time.time())
            rows = [dict(member={'mid': 42}, content={'message': 'GK-test'}, ctime=now, rpid=10)]
            policy = {'mode': 'dynamic', 'reply_oid': 100, 'reply_type': 17, 'max_pages': 1}
            api._get = AsyncMock(return_value={'code': 0, 'data': {'replies': rows}})
            self.assertEqual((await api.observe(policy, 42, 'GK-test', now - 1, now + 60)).state, 'verified')
            for change in [{'member': {'mid': 43}}, {'ctime': now - 10}, {'content': {'message': 'prefix GK-test'}}]:
                api._get = AsyncMock(return_value={'code': 0, 'data': {'replies': [{**rows[0], **change}]}})
                self.assertEqual((await api.observe(policy, 42, 'GK-test', now - 1, now + 60)).state, 'waiting')
        finally:
            await api.close()

    async def test_dm_proof_checks_recipient_and_signed_query(self):
        api = ProofClient('fixture')
        now = int(time.time())
        try:
            message = {'sender_uid': 42, 'receiver_id': 8, 'timestamp': now, 'msg_type': 1,
                       'msg_key': 123, 'content': json.dumps({'content': 'GK-test'})}
            api._get_wbi_keys = AsyncMock(return_value=('a' * 32, 'b' * 32))
            api._get = AsyncMock(side_effect=[{'code': 0, 'data': {'isLogin': True, 'mid': 8}},
                                             {'code': 0, 'data': {'messages': [message]}}])
            policy = {'mode': 'dm', 'receiver_uid': 8}
            self.assertEqual((await api.observe(policy, 42, 'GK-test', now - 1, now + 60)).state, 'verified')
            self.assertIn('w_rid=', api._get.call_args_list[-1].args[0])
            api._get = AsyncMock(side_effect=[{'code': 0, 'data': {'isLogin': True, 'mid': 8}},
                                             {'code': 0, 'data': {'messages': [{**message, 'receiver_id': 9}]}}])
            self.assertEqual((await api.observe(policy, 42, 'GK-test', now - 1, now + 60)).state, 'waiting')
        finally:
            await api.close()

    async def test_medal_numeric_observation_uses_complete_maximum_not_first_pass(self):
        api = BiliApi('fixture')
        try:
            for values in [[(11, 2), (22, 20)], [(22, 20), (11, 2)]]:
                api.get_user_medals_status = AsyncMock(return_value=('ok', values, ''))
                # A distinct scope bypasses global observation cache between variants.
                api.cache_scope = str(values)
                result = await api.check_medal_any_status(42, [11, 22], 1)
                self.assertEqual((result.state, result.matched_level, result.matched_target_uid), ('passed', 20, 22))
            api.get_user_medals_status = AsyncMock(return_value=('incomplete', [(11, 2)], '部分字段缺失'))
            api.cache_scope = 'incomplete'
            result = await api.check_medal_any_status(42, [11, 22], 1)
            self.assertEqual(result.state, 'passed')
            self.assertIsNone(result.matched_level)
        finally:
            await api.close()

    async def test_refresh_common_follow_after_operator_prerequisite_becomes_known(self):
        api = BiliApi('fixture', cache_scope='prerequisite-order')
        try:
            api.check_common_follow_any_status = AsyncMock(side_effect=[CheckResult('failed'), CheckResult('passed')])
            api.get_login_account_relation = AsyncMock(return_value=CheckResult('passed'))
            api.check_following_any_status = AsyncMock()
            result = await api.check_follow_any_status(42, [8], common_negative_is_definitive=False)
            self.assertEqual(result.state, 'passed')
            self.assertEqual(api.check_common_follow_any_status.await_count, 2)
            api.check_following_any_status.assert_not_called()
        finally:
            await api.close()


class CredentialsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await ProtocolTests.asyncSetUp(self)
        self.temp = tempfile.TemporaryDirectory()
        system = {key: os.environ[key] for key in ('SYSTEMROOT', 'WINDIR', 'PATH', 'TEMP', 'TMP', 'USERPROFILE') if key in os.environ}
        self.env = patch.dict(os.environ, system, clear=True)
        self.env.start()
        ConfigManager.reset_state()
        ConfigManager.initialize(SimpleNamespace(app_root=Path(self.temp.name)))
        self.original = 'SESSDATA=old; bili_jct=old-csrf; DedeUserID=8; buvid3=existing'
        ConfigManager.set_credentials(self.original, 'old-token')
        self.calls = []
        self.fail_confirm, self.wrong_identity, self.race = False, False, False
        self.service = CredentialService(client=httpx.AsyncClient(transport=httpx.MockTransport(self.respond)))

    async def asyncTearDown(self):
        await self.service.client.aclose()
        ConfigManager.reset_state()
        self.env.stop()
        self.temp.cleanup()
        await ProtocolTests.asyncTearDown(self)

    def respond(self, request):
        self.calls.append(request)
        path = request.url.path
        if path.endswith('/nav'):
            uid = 9 if self.wrong_identity and 'SESSDATA=new' in request.headers.get('cookie', '') else 8
            return httpx.Response(200, json={'code': 0, 'data': {'isLogin': True, 'mid': uid}})
        if path.endswith('cookie/info'):
            return httpx.Response(200, json={'code': 0, 'data': {'refresh': True, 'timestamp': 1700000000000}})
        if '/correspond/1/' in path:
            self.assertEqual(len(path.rsplit('/', 1)[-1]), 256)
            if self.race:
                ConfigManager.set_credentials(self.original, 'manual-token')
            return httpx.Response(200, text='<div id="1-name">refresh-csrf</div>')
        if path.endswith('cookie/refresh'):
            data = parse_qs(request.content.decode())
            self.assertEqual(data['refresh_csrf'], ['refresh-csrf'])
            self.assertEqual(data['refresh_token'], ['old-token'])
            return httpx.Response(200, json={'code': 0, 'data': {'refresh_token': 'new-token'}},
                headers=[('set-cookie', 'SESSDATA=new; Path=/; Domain=.bilibili.com'),
                         ('set-cookie', 'bili_jct=new-csrf; Path=/; Domain=.bilibili.com')])
        if path.endswith('confirm/refresh'):
            # New verified credentials and the old-token confirmation recovery
            # record must already be committed before old-token invalidation.
            snapshot = ConfigManager.get_snapshot(include_secrets=True)['config']
            self.assertIn('SESSDATA=new', snapshot['bili_cookie'])
            self.assertEqual(snapshot['bili_refresh_token'], 'new-token')
            self.assertEqual(snapshot['bili_refresh_pending']['old_refresh_token'], 'old-token')
            data = parse_qs(request.content.decode())
            self.assertEqual(data, {'csrf': ['new-csrf'], 'refresh_token': ['old-token']})
            if self.fail_confirm:
                raise httpx.ReadTimeout('fixture lost confirmation')
            return httpx.Response(200, json={'code': 0})
        if path.endswith('finger/spi'):
            return httpx.Response(200, json={'code': 0, 'data': {'b_3': 'replacement', 'b_4': 'new-fingerprint'}})
        if path.endswith('qrcode/generate'):
            self.assertEqual(request.headers.get('cookie'), '')
            return httpx.Response(200, json={'code': 0, 'data': {'url': 'https://passport.bilibili.com/fixture', 'qrcode_key': 'key'}})
        if path.endswith('qrcode/poll'):
            return httpx.Response(200, json={'code': 0, 'data': {'code': 0, 'refresh_token': 'qr-token'}},
                headers=[('set-cookie', 'SESSDATA=new; Path=/; Domain=.bilibili.com'),
                         ('set-cookie', 'bili_jct=new-csrf; Path=/; Domain=.bilibili.com')])
        raise AssertionError(f'Unexpected protocol URL: {request.url}')

    async def test_refresh_persists_before_confirm_and_recovers_after_restart(self):
        self.fail_confirm = True
        with self.assertRaisesRegex(ValueError, '暂时无法连接'):
            await self.service.refresh_if_due()
        self.assertEqual(ConfigManager.get_config().bili_refresh_pending['old_refresh_token'], 'old-token')
        refresh_count = sum(call.url.path.endswith('cookie/refresh') for call in self.calls)
        self.fail_confirm = False
        self.assertEqual((await self.service.refresh_if_due())['state'], 'confirmed')
        self.assertEqual(sum(call.url.path.endswith('cookie/refresh') for call in self.calls), refresh_count)
        self.assertEqual(ConfigManager.get_config().bili_refresh_pending, {})

    async def test_identity_change_never_publishes_or_invalidates_old_credential(self):
        self.wrong_identity = True
        with self.assertRaises(ValueError):
            await self.service.refresh_if_due()
        self.assertEqual(ConfigManager.get_bili_cookie(), self.original)
        self.assertFalse(any(call.url.path.endswith('confirm/refresh') for call in self.calls))

    async def test_same_cookie_generation_race_cannot_overwrite_manual_token(self):
        self.race = True
        with self.assertRaisesRegex(ValueError, '版本'):
            await self.service.refresh_if_due()
        self.assertEqual(ConfigManager.get_config().bili_refresh_token, 'manual-token')
        self.assertFalse(any(call.url.path.endswith('confirm/refresh') for call in self.calls))

    async def test_native_qr_acquires_cookie_without_exposing_secrets(self):
        session = await self.service.start_qr_login()
        self.assertNotIn('key', session)
        self.assertEqual(await self.service.poll_qr_login(session['session_id']), {'state': 'success'})
        self.assertIn('SESSDATA=new', ConfigManager.get_bili_cookie())
        public = str(ConfigManager.get_snapshot()) + str(await self.service.status())
        self.assertNotIn('qr-token', public)
        self.assertNotIn('SESSDATA=new', public)

    async def test_native_qr_same_cookie_manual_edit_invalidates_session(self):
        session = await self.service.start_qr_login()
        ConfigManager.set_credentials(self.original, 'manual-token')
        with self.assertRaisesRegex(ValueError, '版本'):
            await self.service.poll_qr_login(session['session_id'])
        self.assertEqual(ConfigManager.get_config().bili_refresh_token, 'manual-token')

    async def test_fingerprint_fills_only_missing_fields_and_persists(self):
        self.assertEqual(await self.service.ensure_fingerprints(), {'state': 'updated'})
        fields = cookie_fields(ConfigManager.get_bili_cookie())
        self.assertEqual(fields['buvid3'], 'existing')
        self.assertEqual(fields['buvid4'], 'new-fingerprint')
        self.assertIn('b_nut', fields)
        self.assertEqual(ConfigManager.get_config().bili_refresh_token, 'old-token')
        self.assertEqual(await self.service.ensure_fingerprints(), {'state': 'present'})

    async def test_applicant_native_qr_isolated_from_operator_credentials(self):
        original = ConfigManager.get_snapshot(include_secrets=True)
        applicant = ApplicantQrService(client=self.service.client)
        identifier = 'challenge-1'
        secret = 'applicant-secret-fixture'
        import hashlib
        nonce_hash = hashlib.sha256(secret.encode()).hexdigest()
        session = await applicant.start(identifier, nonce_hash, 8)
        with self.assertRaisesRegex(ValueError, '不匹配'):
            await applicant.poll(session['session_id'], 'other-challenge', secret)
        self.assertEqual(await applicant.poll(session['session_id'], identifier, secret), {'state': 'verified'})
        self.assertEqual(ConfigManager.get_snapshot(include_secrets=True), original)
        self.assertFalse(applicant.client.cookies)
        with self.assertRaises(ValueError):
            await applicant.poll(session['session_id'], identifier, secret)

    async def test_applicant_native_qr_rejects_mismatched_uid_without_saving(self):
        original = ConfigManager.get_snapshot(include_secrets=True)
        applicant = ApplicantQrService(client=self.service.client)
        import hashlib
        secret = 'applicant-secret-fixture'
        session = await applicant.start('challenge', hashlib.sha256(secret.encode()).hexdigest(), 9)
        self.assertEqual((await applicant.poll(session['session_id'], 'challenge', secret))['state'], 'wrong_identity')
        self.assertEqual(ConfigManager.get_snapshot(include_secrets=True), original)

    async def test_fingerprint_maintenance_preserves_pending_refresh_confirmation(self):
        snapshot = ConfigManager.get_snapshot(include_secrets=True)
        snapshot['config']['bili_refresh_pending'] = {'old_refresh_token': 'pending-token', 'created_at': 1}
        ConfigManager.replace_config(snapshot['config'], snapshot['version'], actor='credential-service')
        await self.service.ensure_fingerprints()
        self.assertEqual(ConfigManager.get_config().bili_refresh_pending['old_refresh_token'], 'pending-token')
