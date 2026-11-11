import asyncio
import concurrent.futures
import http.client
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot
from nonebot.adapters.onebot.v11.exception import ActionFailed

nonebot.init()

from src.plugins.group_request_manager.bili_api import BiliApi, CheckResult
from src.gatekeeper.decisions import DecisionService
from src.plugins.group_request_manager.decision_adapter import SqliteDecisionStorage
from src.plugins.group_request_manager.bili_runtime import BiliRequestCoordinator
from src.plugins.group_request_manager.async_storage import run_storage
from src.plugins.group_request_manager import main as manager_main
from src.plugins.group_request_manager.config import (
    ConfigManager,
    GroupConfig,
    PluginConfig,
    load_group_configs_from_yaml_file,
    load_review_stages_from_yaml_file,
)
from src.plugins.group_request_manager import config as config_module
from src.plugins.group_request_manager import database as database_module
from src.plugins.group_request_manager.leave_record import LeaveRecordManager
from src.plugins.group_request_manager.main import (
    JoinRequestContext,
    RuleConditionStatus,
    _build_reject_reason,
    _build_rule_condition_statuses,
    _check_disk_space_once,
    _disk_space_monitor,
    _command_case_variants,
    _get_review_stages,
    _format_condition_details,
    _format_stage_pipeline_text,
    _format_stage_results,
    _format_user_check_reply,
    _mark_bili_flow_exception,
    _run_stage_pipeline,
)
from src.plugins.group_request_manager.request_record import JoinRequestRecordManager
from src.plugins.group_request_manager.webui import (
    _resolve_custom_css_path,
    _resolve_pipeline_file,
    start_webui,
)


class ReviewPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = load_group_configs_from_yaml_file("groups.yaml")[0]
        cls.stages = _get_review_stages(cls.config)

    def run_pipeline(self, states: dict[str, str]):
        context = JoinRequestContext(
            group_id=self.config.group_id,
            user_id="10001",
            group_config=self.config,
            dry_run=True,
        )
        context.condition_statuses = {
            name: RuleConditionStatus(state, f"{name}-reason" if state != "true" else "")
            for name, state in states.items()
        }
        return asyncio.run(_run_stage_pipeline(context, self.stages))

    def test_group_a_all_true_approves(self):
        result = self.run_pipeline(
            {
                "no_leave_record": "true",
                "bili_account": "true",
                "bili_level": "true",
                "follow": "true",
                "qq_level": "true",
                "medal": "false",
            }
        )
        self.assertEqual(result.dispatch_result.flow_action, "approve")
        self.assertEqual(result.stage.name, "筛选组A（拒绝组）")

    def test_group_a_false_wins_over_unknown(self):
        result = self.run_pipeline(
            {
                "no_leave_record": "true",
                "bili_account": "true",
                "bili_level": "true",
                "follow": "unknown",
                "qq_level": "false",
                "medal": "true",
            }
        )
        self.assertEqual(result.dispatch_result.flow_action, "reject")
        self.assertEqual(result.reasons, ["qq_level-reason"])

    def test_group_a_unknown_enters_group_b_and_medal_approves(self):
        result = self.run_pipeline(
            {
                "no_leave_record": "true",
                "bili_account": "true",
                "bili_level": "true",
                "follow": "unknown",
                "qq_level": "true",
                "medal": "true",
            }
        )
        self.assertEqual(result.dispatch_result.flow_action, "approve")
        self.assertEqual(result.stage.name, "筛选组B（通过组）")

    def test_group_b_without_medal_ignores(self):
        result = self.run_pipeline(
            {
                "no_leave_record": "true",
                "bili_account": "true",
                "bili_level": "true",
                "follow": "unknown",
                "qq_level": "true",
                "medal": "false",
            }
        )
        self.assertEqual(result.dispatch_result.flow_action, "ignore")

    def test_last_stage_next_on_false_is_invalid(self):
        pipeline_yaml = """
version: 2
groups:
  - id: only_group
    mode: any_pass
    conditions: [medal]
    on_true: approve
    on_false: next
    on_unknown: ignore
"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pipeline.yaml"
            path.write_text(pipeline_yaml, encoding="utf-8")
            stages = load_review_stages_from_yaml_file(path)

        context = JoinRequestContext(
            group_id="1",
            user_id="10001",
            group_config=GroupConfig(group_id="1"),
            dry_run=True,
        )
        context.condition_statuses = {
            "medal": RuleConditionStatus("false", "no medal")
        }

        with self.assertRaisesRegex(ValueError, "不存在下一个分组"):
            asyncio.run(_run_stage_pipeline(context, stages))

    def test_pipeline_uses_group_modes_instead_of_expressions(self):
        self.assertFalse(self.config.enabled)
        self.assertEqual(self.stages[0].mode, "all_pass")
        self.assertEqual(self.stages[1].mode, "any_pass")
        self.assertNotIn("rule_expression", GroupConfig.model_fields)

    def test_legacy_rule_expression_yaml_is_rejected(self):
        legacy_yaml = """
version: 2
groups:
  - id: legacy
    rule: qq_level and follow
    on_true: approve
    on_false: reject
    on_unknown: ignore
"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.yaml"
            path.write_text(legacy_yaml, encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "conditions"):
                load_review_stages_from_yaml_file(path)

    def test_runtime_config_loader_keeps_other_fields_when_legacy_review_stages_exist(self):
        legacy_runtime_payload = {
            "bili_cookie": "cookie-value",
            "admin_qqs": ["10001"],
            "groups": {
                "123": {
                    "group_id": "123",
                    "required_bili_level": 2,
                    "review_stages": [
                        {
                            "index": 1,
                            "name": "legacy",
                            "rule_expression": "bili_level and follow",
                            "on_true": ["approve"],
                            "on_false": ["reject"],
                            "on_unknown": ["ignore"],
                        }
                    ],
                }
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plugin_state.json"
            path.write_text(
                json.dumps(legacy_runtime_payload, ensure_ascii=False),
                encoding="utf-8",
            )
            loaded = ConfigManager._load_runtime_config_file(path)

        self.assertEqual(loaded.bili_cookie, "cookie-value")
        self.assertEqual(loaded.admin_qqs, ["10001"])
        self.assertIn("123", loaded.groups)
        self.assertEqual(loaded.groups["123"].required_bili_level, 2)
        self.assertEqual(loaded.groups["123"].review_stages, [])

    def test_command_group_override_survives_yaml_reload(self):
        with tempfile.TemporaryDirectory() as directory:
            override_path = Path(directory) / "group_overrides.json"
            changed = self.config.model_copy(deep=True)
            changed.required_qq_level = 18

            with patch.object(config_module, "GROUP_OVERRIDES_FILE", override_path):
                ConfigManager._save_group_override(changed, ["required_qq_level"])
                reloaded = PluginConfig(
                    groups={self.config.group_id: self.config.model_copy(deep=True)}
                )
                ConfigManager._merge_runtime_group_overrides(reloaded)

            self.assertEqual(reloaded.groups[self.config.group_id].required_qq_level, 18)
            saved = json.loads(override_path.read_text(encoding="utf-8"))
            self.assertEqual(
                saved["groups"][self.config.group_id], {"required_qq_level": 18}
            )

    def test_admin_check_commands_use_new_names_only(self):
        self.assertIn("检查qq", str(manager_main.check_qq.rule))
        self.assertIn("检查bili", str(manager_main.check_bili.rule))
        self.assertFalse(hasattr(manager_main, "check_user_request"))
        self.assertFalse(hasattr(manager_main, "test_rule"))

    def test_configured_reasons_and_bot_prefix(self):
        config = GroupConfig(
            group_id="1",
            required_qq_level=4,
            required_bili_level=2,
            target_uids=[200000003],
            reject_reasons={"qq_level": "自定义QQ拒绝原因"},
        )
        statuses = _build_rule_condition_statuses(
            config,
            {"bili_account", "bili_level", "follow", "qq_level"},
            qq_level=3,
            bili_search_result=CheckResult(
                "passed", matched_target_uid=123, matched_level=1
            ),
            bili_level=1,
            follow_result=CheckResult("failed"),
            medal_result=CheckResult("not_required"),
            leave_record=None,
            conflict_qq=None,
            owned_other_bili_uids=[],
        )
        self.assertIn("自定义QQ拒绝原因", statuses["qq_level"].reason)
        self.assertIn("当前3级，本群要求至少4级", statuses["qq_level"].reason)
        self.assertEqual(statuses["bili_level"].state, "false")
        self.assertIn("未检测到该B站账号关注指定主播", statuses["follow"].reason)
        self.assertIn("UID：200000003", statuses["follow"].reason)
        self.assertIn("请关注后重新申请", statuses["follow"].reason)
        self.assertEqual(_build_reject_reason(["测试原因"]), "来自BOT:测试原因")

    def test_bili_level_reason_explains_search_failure(self):
        statuses = _build_rule_condition_statuses(
            GroupConfig(group_id="1", required_bili_level=2),
            {"bili_account", "bili_level"},
            qq_level=None,
            bili_search_result=CheckResult("failed", "未搜索到你填写的B站昵称，请检查昵称是否填写正确，或填写正确的UID后重新申请。请不要填写B站昵称或UID以外的内容"),
            bili_level=None,
            follow_result=CheckResult("not_required"),
            medal_result=CheckResult("not_required"),
            leave_record=None,
            conflict_qq=None,
            owned_other_bili_uids=[],
        )

        self.assertEqual(statuses["bili_account"].state, "false")
        self.assertEqual(
            statuses["bili_account"].reason,
            "未搜索到你填写的B站昵称，请检查昵称是否填写正确，或填写正确的UID后重新申请。请不要填写B站昵称或UID以外的内容",
        )
        self.assertEqual(statuses["bili_level"].state, "unknown")
        self.assertIn("未搜索到B站账号", statuses["bili_level"].reason)

    def test_bili_level_without_threshold_is_unknown(self):
        statuses = _build_rule_condition_statuses(
            GroupConfig(group_id="1", required_bili_level=0),
            {"bili_level"},
            qq_level=None,
            bili_search_result=CheckResult(
                "passed", matched_target_uid=123, matched_level=6
            ),
            bili_level=6,
            follow_result=CheckResult("not_required"),
            medal_result=CheckResult("not_required"),
            leave_record=None,
            conflict_qq=None,
            owned_other_bili_uids=[],
        )

        self.assertEqual(statuses["bili_level"].state, "unknown")
        self.assertIn("未配置有效的 B站最低等级", statuses["bili_level"].reason)

    def test_follow_unknown_preserves_api_diagnostic(self):
        statuses = _build_rule_condition_statuses(
            GroupConfig(group_id="1", target_uids=[200000003]),
            {"follow"},
            qq_level=None,
            bili_search_result=CheckResult("not_required"),
            bili_level=None,
            follow_result=CheckResult(
                "inaccessible",
                "共同关注不可见或无法读取；关注列表仅能读取 100/156 项",
            ),
            medal_result=CheckResult("not_required"),
            leave_record=None,
            conflict_qq=None,
            owned_other_bili_uids=[],
        )

        self.assertEqual(statuses["follow"].state, "unknown")
        self.assertEqual(
            statuses["follow"].reason,
            "暂无法确认关注状态：共同关注不可见或无法读取；关注列表仅能读取 100/156 项；请稍后重试或联系管理员",
        )

    def test_bili_flow_exception_preserves_successful_search_result(self):
        context = JoinRequestContext(
            group_id="1",
            user_id="10001",
            group_config=GroupConfig(group_id="1"),
        )
        context.bili_search_result = CheckResult(
            "passed", matched_target_uid=123, matched_level=5
        )

        _mark_bili_flow_exception(context)

        self.assertEqual(context.bili_search_result.state, "passed")
        self.assertEqual(context.bili_search_result.matched_target_uid, 123)

    def test_bili_flow_exception_marks_unstarted_search_inaccessible(self):
        context = JoinRequestContext(
            group_id="1",
            user_id="10001",
            group_config=GroupConfig(group_id="1"),
        )

        _mark_bili_flow_exception(context)

        self.assertEqual(context.bili_search_result.state, "inaccessible")

    def test_complex_command_sections_are_split_into_readable_lines(self):
        condition_lines = _format_condition_details(
            {"bili_account": "true", "follow": "unknown", "medal": "false"},
            {"follow": "关注列表不可见", "medal": "未持有灯牌"},
        )
        self.assertEqual(
            condition_lines,
            [
                "通过：",
                "  - B站账号确认",
                "未通过：",
                "  - 粉丝牌",
                "    原因：未持有灯牌",
                "无法判断：",
                "  - 关注",
                "    原因：关注列表不可见",
            ],
        )

        stage_lines = _format_stage_results(
            [
                "筛选组A（拒绝组） | all_pass[follow,qq_level] | 无法判断 | next(分组2) | 进入筛选组B"
            ]
        )
        self.assertEqual(stage_lines[0], "1. 筛选组A（拒绝组）")
        self.assertTrue(any("规则：全部通过（关注、QQ等级）" in line for line in stage_lines))
        self.assertTrue(any("动作：进入筛选组 2" in line for line in stage_lines))
        self.assertTrue(all(" | " not in line for line in stage_lines))

        pipeline_text = _format_stage_pipeline_text(self.config)
        self.assertIn("1. 筛选组A（拒绝组）", pipeline_text)
        self.assertIn("无法判断：进入筛选组 2", pipeline_text)
        self.assertNotIn(" | ", pipeline_text)

    def test_check_qq_reply_uses_sections_and_human_readable_stage_details(self):
        binding = SimpleNamespace(
            groups=["900000001"],
            latest_bili_name="测试用户",
            latest_bili_uid=200000001,
            attempt_count=1,
            approved_count=0,
            rejected_count=0,
            ignored_count=1,
        )
        audit = SimpleNamespace(
            created_at="2026-06-22 13:04:11",
            group_id="900000001",
            result="ignored",
            bili_name="测试用户",
            reasons=["无法检索目标主播粉丝灯牌"],
            condition_states={"bili_account": "true", "medal": "unknown"},
            condition_reasons={"medal": "粉丝牌列表不可见或无法读取"},
            stage_results=[
                "筛选组B（通过组） | any_pass[medal] | 无法判断 | ignore | 无法检索目标主播粉丝灯牌"
            ],
        )
        summary = SimpleNamespace(
            attempt_count=1,
            approved_count=0,
            rejected_count=0,
            ignored_count=1,
            pending_count=0,
            groups=["900000001"],
        )
        with (
            patch.object(
                manager_main.JoinRequestRecordManager,
                "get_user_report",
                return_value=(binding, summary, [audit]),
            ),
            patch.object(
                manager_main.JoinRequestRecordManager,
                "get_all_user_audits",
                side_effect=AssertionError("unbounded query must not be used"),
            ),
        ):
            reply = asyncio.run(_format_user_check_reply("1000000001"))

        self.assertIn("【绑定与统计】", reply)
        self.assertIn("【最近申请记录】", reply)
        self.assertIn("条件结果\n通过：", reply)
        self.assertIn("流程详情\n1. 筛选组B（通过组）", reply)
        self.assertIn("动作：忽略申请", reply)
        self.assertNotIn(" | ", reply)


class LocalRecordConcurrencyTests(unittest.TestCase):
    def _database_patches(self, directory: str):
        root = Path(directory)
        return (
            patch.object(database_module, "DATABASE_FILE", root / "gatekeeper.sqlite3"),
            patch.object(
                database_module,
                "LEGACY_JOIN_REQUEST_FILE",
                root / "join_request_records.json",
            ),
            patch.object(
                database_module,
                "LEGACY_LEAVE_RECORDS_PATH",
                root / "leave_records",
            ),
            patch.object(
                database_module,
                "VERY_LEGACY_LEAVE_RECORDS_PATH",
                root / "legacy_leave_records",
            ),
        )

    def test_leave_records_are_serialized_with_sqlite_transaction(self):
        with tempfile.TemporaryDirectory() as directory:
            patches = self._database_patches(directory)
            with patches[0], patches[1], patches[2], patches[3]:
                database_module.reset_database_state()
                try:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                        futures = [
                            executor.submit(
                                LeaveRecordManager.add_leave_record,
                                "900000001",
                                "1000000001",
                                f"用户{index}",
                            )
                            for index in range(20)
                        ]
                        for future in futures:
                            future.result()

                    record = LeaveRecordManager.get_leave_record(
                        "900000001", "1000000001"
                    )
                    self.assertIsNotNone(record)
                    self.assertEqual(record.count, 20)
                finally:
                    database_module.reset_database_state()

    def test_join_request_audits_are_serialized_with_sqlite_transaction(self):
        with tempfile.TemporaryDirectory() as directory:
            patches = self._database_patches(directory)
            with patches[0], patches[1], patches[2], patches[3]:
                database_module.reset_database_state()
                JoinRequestRecordManager._save_failure_count = 0
                try:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                        futures = [
                            executor.submit(
                                JoinRequestRecordManager.add_audit,
                                group_id="900000001",
                                qq=f"1000{index}",
                                result="ignored",
                                reasons=["并发测试"],
                                bili_name=f"测试用户{index}",
                                bili_uid=20000 + index,
                            )
                            for index in range(20)
                        ]
                        for future in futures:
                            self.assertIsNotNone(future.result())

                    with database_module.database_connection() as connection:
                        audit_count = connection.execute(
                            "SELECT COUNT(*) FROM join_request_audits"
                        ).fetchone()[0]
                        binding_count = connection.execute(
                            "SELECT COUNT(*) FROM user_bindings"
                        ).fetchone()[0]
                    self.assertEqual(audit_count, 20)
                    self.assertEqual(binding_count, 20)
                finally:
                    database_module.reset_database_state()
                    JoinRequestRecordManager._save_failure_count = 0


class AsyncStorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_blocking_storage_work_does_not_block_event_loop(self):
        started = threading.Event()
        release = threading.Event()

        def blocking_operation():
            started.set()
            release.wait(timeout=2)
            return "ok"

        task = asyncio.create_task(run_storage(blocking_operation))
        await asyncio.to_thread(started.wait, 1)
        heartbeat_ran = False

        async def heartbeat():
            nonlocal heartbeat_ran
            await asyncio.sleep(0)
            heartbeat_ran = True

        await heartbeat()
        self.assertTrue(heartbeat_ran)
        release.set()
        self.assertEqual(await task, "ok")


class SQLiteMigrationTests(unittest.TestCase):
    def test_legacy_json_is_imported_once_and_left_untouched(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            join_file = root / "join_request_records.json"
            leave_path = root / "leave_records"
            leave_path.mkdir()
            legacy_payload = {
                "user_bindings": {
                    "10001": {
                        "qq": "10001",
                        "latest_bili_name": "测试用户",
                        "latest_bili_uid": 20001,
                        "first_seen": "2026-01-01 00:00:00",
                        "last_seen": "2026-01-02 00:00:00",
                        "groups": ["90001"],
                        "used_bili_uids": [20001],
                        "used_bili_names": ["测试用户"],
                        "attempt_count": 1,
                        "approved_count": 1,
                        "rejected_count": 0,
                        "ignored_count": 0,
                    }
                },
                "bili_bindings": {
                    "20001": {
                        "bili_uid": 20001,
                        "owner_qq": "10001",
                        "first_seen": "2026-01-01 00:00:00",
                        "last_seen": "2026-01-02 00:00:00",
                        "bili_names": ["测试用户"],
                    }
                },
                "audits": [
                    {
                        "created_at": "2026-01-02 00:00:00",
                        "group_id": "90001",
                        "qq": "10001",
                        "bili_name": "测试用户",
                        "bili_uid": 20001,
                        "result": "approved",
                        "reasons": ["测试通过"],
                        "condition_states": {"bili_account": "true"},
                    }
                ],
            }
            join_file.write_text(
                json.dumps(legacy_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            leave_file = leave_path / "leave_records_90001.json"
            leave_file.write_text(
                json.dumps(
                    {
                        "group_id": "90001",
                        "records": {
                            "10002": {
                                "qq": "10002",
                                "leave_time": "2026-01-03 00:00:00",
                                "nickname": "退群用户",
                                "count": 2,
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            original_join_bytes = join_file.read_bytes()
            original_leave_bytes = leave_file.read_bytes()

            with (
                patch.object(database_module, "DATABASE_FILE", root / "gatekeeper.sqlite3"),
                patch.object(database_module, "LEGACY_JOIN_REQUEST_FILE", join_file),
                patch.object(database_module, "LEGACY_LEAVE_RECORDS_PATH", leave_path),
                patch.object(
                    database_module,
                    "VERY_LEGACY_LEAVE_RECORDS_PATH",
                    root / "legacy_leave_records",
                ),
            ):
                database_module.reset_database_state()
                try:
                    binding = JoinRequestRecordManager.get_user_binding("10001")
                    self.assertIsNotNone(binding)
                    self.assertEqual(binding.latest_bili_uid, 20001)
                    self.assertEqual(binding.approved_count, 1)
                    self.assertEqual(
                        JoinRequestRecordManager.get_bili_binding(20001).owner_qq,
                        "10001",
                    )
                    audits = JoinRequestRecordManager.get_all_user_audits("10001")
                    self.assertEqual(len(audits), 1)
                    self.assertEqual(audits[0].condition_states, {"bili_account": "true"})
                    leave_record = LeaveRecordManager.get_leave_record("90001", "10002")
                    self.assertIsNotNone(leave_record)
                    self.assertEqual(leave_record.count, 2)

                    JoinRequestRecordManager.add_audit(
                        group_id="90001",
                        qq="10001",
                        result="ignored",
                        reasons=["迁移后写入"],
                    )
                    self.assertEqual(
                        len(JoinRequestRecordManager.get_all_user_audits("10001")),
                        2,
                    )
                    with database_module.database_connection() as connection:
                        marker = connection.execute(
                            "SELECT value FROM metadata WHERE key = ?",
                            (database_module.MIGRATION_MARKER,),
                        ).fetchone()
                    self.assertIsNotNone(marker)
                    self.assertEqual(join_file.read_bytes(), original_join_bytes)
                    self.assertEqual(leave_file.read_bytes(), original_leave_bytes)
                finally:
                    database_module.reset_database_state()

    def test_missing_marker_with_existing_rows_stops_automatic_migration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch.object(database_module, "DATABASE_FILE", root / "gatekeeper.sqlite3"),
                patch.object(
                    database_module,
                    "LEGACY_JOIN_REQUEST_FILE",
                    root / "join_request_records.json",
                ),
                patch.object(
                    database_module,
                    "LEGACY_LEAVE_RECORDS_PATH",
                    root / "leave_records",
                ),
                patch.object(
                    database_module,
                    "VERY_LEGACY_LEAVE_RECORDS_PATH",
                    root / "legacy_leave_records",
                ),
            ):
                database_module.reset_database_state()
                try:
                    JoinRequestRecordManager.add_audit(
                        group_id="90001",
                        qq="10001",
                        result="ignored",
                        reasons=["测试"],
                    )
                    with database_module.database_connection() as connection, connection:
                        connection.execute(
                            "DELETE FROM metadata WHERE key = ?",
                            (database_module.MIGRATION_MARKER,),
                        )
                    database_module.reset_database_state()

                    with self.assertRaisesRegex(
                        sqlite3.DatabaseError, "缺少 JSON 迁移标记"
                    ):
                        database_module.initialize_database()
                finally:
                    database_module.reset_database_state()

    def test_unversioned_v1_database_is_upgraded_to_current_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database_file = root / "gatekeeper.sqlite3"
            connection = sqlite3.connect(database_file)
            connection.executescript(database_module.SCHEMA_V1_SQL)
            connection.execute(
                "INSERT INTO metadata(key, value) VALUES (?, ?)",
                (database_module.MIGRATION_MARKER, "{}"),
            )
            connection.execute(
                """
                INSERT INTO join_request_audits (
                    created_at, group_id, qq, result, reasons_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                ("2026-01-01 00:00:00", "90001", "10001", "ignored", "[]"),
            )
            connection.commit()
            connection.close()

            with (
                patch.object(database_module, "DATABASE_FILE", database_file),
                patch.object(
                    database_module,
                    "LEGACY_JOIN_REQUEST_FILE",
                    root / "join_request_records.json",
                ),
                patch.object(
                    database_module,
                    "LEGACY_LEAVE_RECORDS_PATH",
                    root / "leave_records",
                ),
                patch.object(
                    database_module,
                    "VERY_LEGACY_LEAVE_RECORDS_PATH",
                    root / "legacy_leave_records",
                ),
            ):
                database_module.reset_database_state()
                try:
                    database_module.initialize_database()
                    with database_module.database_connection() as upgraded:
                        version = upgraded.execute("PRAGMA user_version").fetchone()[0]
                        row = upgraded.execute(
                            "SELECT action_status, updated_at FROM join_request_audits"
                        ).fetchone()
                    self.assertEqual(version, database_module.CURRENT_SCHEMA_VERSION)
                    self.assertEqual(row["action_status"], "not_required")
                    self.assertEqual(row["updated_at"], "2026-01-01 00:00:00")
                finally:
                    database_module.reset_database_state()

    def test_pending_decision_updates_stats_exactly_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch.object(
                    database_module, "DATABASE_FILE", root / "gatekeeper.sqlite3"
                ),
                patch.object(
                    database_module,
                    "LEGACY_JOIN_REQUEST_FILE",
                    root / "join_request_records.json",
                ),
                patch.object(
                    database_module,
                    "LEGACY_LEAVE_RECORDS_PATH",
                    root / "leave_records",
                ),
                patch.object(
                    database_module,
                    "VERY_LEGACY_LEAVE_RECORDS_PATH",
                    root / "legacy_leave_records",
                ),
            ):
                database_module.reset_database_state()
                try:
                    pending = JoinRequestRecordManager.begin_pending_decision(
                        group_id="90001",
                        qq="10001",
                        result="approved",
                        reasons=["通过"],
                        bili_name="测试用户",
                        bili_uid=20001,
                    )
                    self.assertIsNotNone(pending)
                    self.assertIsNone(JoinRequestRecordManager.get_user_binding("10001"))
                    summary = JoinRequestRecordManager.get_user_summary("10001")
                    self.assertEqual(summary.pending_count, 1)
                    self.assertEqual(summary.approved_count, 0)

                    self.assertTrue(
                        JoinRequestRecordManager.finalize_pending_decision(
                            pending.id, applied=True
                        )
                    )
                    self.assertTrue(
                        JoinRequestRecordManager.finalize_pending_decision(
                            pending.id, applied=True
                        )
                    )
                    binding = JoinRequestRecordManager.get_user_binding("10001")
                    summary = JoinRequestRecordManager.get_user_summary("10001")
                    self.assertEqual(binding.attempt_count, 1)
                    self.assertEqual(binding.approved_count, 1)
                    self.assertEqual(summary.pending_count, 0)
                    self.assertEqual(summary.approved_count, 1)
                    self.assertFalse(
                        JoinRequestRecordManager.finalize_pending_decision(
                            pending.id, applied=False
                        )
                    )

                    failed = JoinRequestRecordManager.begin_pending_decision(
                        group_id="90001",
                        qq="10002",
                        result="rejected",
                        reasons=["原计划拒绝"],
                    )
                    self.assertTrue(
                        JoinRequestRecordManager.finalize_pending_decision(
                            failed.id,
                            applied=False,
                            failure_reason="OneBot 请求失败",
                        )
                    )
                    failed_audit = JoinRequestRecordManager.get_user_audits(
                        "10002", 1
                    )[0]
                    failed_summary = JoinRequestRecordManager.get_user_summary("10002")
                    self.assertEqual(failed_audit.action_status, "failed")
                    self.assertEqual(failed_audit.result, "ignored")
                    self.assertEqual(failed_summary.ignored_count, 1)
                    self.assertEqual(failed_summary.rejected_count, 0)
                    self.assertFalse(
                        JoinRequestRecordManager.finalize_pending_decision(
                            failed.id, applied=True
                        )
                    )

                    first = JoinRequestRecordManager.begin_pending_decision(
                        group_id="90001",
                        qq="10003",
                        result="approved",
                        reasons=["通过"],
                        request_key="bot:90001:add:unique-flag",
                    )
                    duplicate = JoinRequestRecordManager.begin_pending_decision(
                        group_id="90001",
                        qq="10003",
                        result="approved",
                        reasons=["通过"],
                        request_key="bot:90001:add:unique-flag",
                    )
                    self.assertTrue(first.was_created)
                    self.assertFalse(duplicate.was_created)
                    self.assertEqual(duplicate.id, first.id)
                    self.assertEqual(
                        JoinRequestRecordManager.get_pending_decision_count(), 1
                    )
                finally:
                    database_module.reset_database_state()

    def test_ignored_audit_does_not_claim_a_new_bili_uid(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch.object(database_module, "DATABASE_FILE", root / "gatekeeper.sqlite3"),
                patch.object(
                    database_module,
                    "LEGACY_JOIN_REQUEST_FILE",
                    root / "join_request_records.json",
                ),
                patch.object(
                    database_module,
                    "LEGACY_LEAVE_RECORDS_PATH",
                    root / "leave_records",
                ),
                patch.object(
                    database_module,
                    "VERY_LEGACY_LEAVE_RECORDS_PATH",
                    root / "legacy_leave_records",
                ),
            ):
                database_module.reset_database_state()
                try:
                    JoinRequestRecordManager.add_audit(
                        group_id="90001",
                        qq="10001",
                        result="ignored",
                        reasons=["仅记录"],
                        bili_name="测试用户",
                        bili_uid=20001,
                    )
                    self.assertIsNone(JoinRequestRecordManager.get_bili_binding(20001))

                    JoinRequestRecordManager.add_audit(
                        group_id="90001",
                        qq="10001",
                        result="approved",
                        reasons=["通过"],
                        bili_name="测试用户",
                        bili_uid=20001,
                    )
                    self.assertEqual(
                        JoinRequestRecordManager.get_bili_binding(20001).owner_qq,
                        "10001",
                    )
                finally:
                    database_module.reset_database_state()


class QQLevelTests(unittest.IsolatedAsyncioTestCase):
    def test_zero_is_a_valid_qq_level(self):
        self.assertEqual(manager_main._parse_qq_level(0), 0)
        self.assertEqual(manager_main._parse_qq_level("Lv0"), 0)
        self.assertIsNone(manager_main._parse_qq_level(None))
        self.assertIsNone(manager_main._parse_qq_level(-1))

    async def test_explicit_zero_from_stranger_info_is_rejected_by_threshold(self):
        bot = SimpleNamespace(
            get_stranger_info=AsyncMock(
                return_value={"qqLevel": 0, "isHideQQLevel": 0}
            )
        )
        qq_level = await manager_main._fetch_qq_level(bot, "1000000002")
        self.assertEqual(qq_level, 0)

        config = GroupConfig(
            group_id="900000001",
            required_qq_level=4,
            reject_reasons={"qq_level": "你的Q等级过低,疑似机器人账号"},
        )
        statuses = _build_rule_condition_statuses(
            config,
            {"qq_level"},
            qq_level=qq_level,
            bili_search_result=CheckResult("not_required"),
            bili_level=None,
            follow_result=CheckResult("not_required"),
            medal_result=CheckResult("not_required"),
            leave_record=None,
            conflict_qq=None,
            owned_other_bili_uids=[],
        )
        self.assertEqual(statuses["qq_level"].state, "false")
        self.assertIn("当前0级，本群要求至少4级", statuses["qq_level"].reason)
        self.assertNotIn("疑似机器人", statuses["qq_level"].reason)

    async def test_hidden_zero_from_stranger_info_is_unknown(self):
        bot = SimpleNamespace(
            get_stranger_info=AsyncMock(
                return_value={"qqLevel": 0, "isHideQQLevel": 1}
            )
        )

        qq_level = await manager_main._fetch_qq_level(bot, "1000000003")

        self.assertIsNone(qq_level)

        statuses = _build_rule_condition_statuses(
            GroupConfig(group_id="900000001", required_qq_level=4),
            {"qq_level"},
            qq_level=qq_level,
            bili_search_result=CheckResult("not_required"),
            bili_level=None,
            follow_result=CheckResult("not_required"),
            medal_result=CheckResult("not_required"),
            leave_record=None,
            conflict_qq=None,
            owned_other_bili_uids=[],
        )
        self.assertEqual(statuses["qq_level"].state, "unknown")
        self.assertIn("暂无法读取你的QQ等级", statuses["qq_level"].reason)
        self.assertIn("至少4级", statuses["qq_level"].reason)
        self.assertIn("请稍后重试或联系管理员", statuses["qq_level"].reason)

    async def test_event_level_can_fallback_when_stranger_level_is_hidden(self):
        bot = SimpleNamespace(
            get_stranger_info=AsyncMock(
                return_value={"qqLevel": 0, "isHideQQLevel": "true"}
            )
        )
        event = SimpleNamespace(model_dump=lambda: {"qqLevel": 86})

        qq_level = await manager_main._fetch_qq_level(bot, "1000000003", event)

        self.assertEqual(qq_level, 86)


class DecisionDurabilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        service_patch = patch.object(
            manager_main, "_decision_service", DecisionService(SqliteDecisionStorage())
        )
        service_patch.start()
        self.addCleanup(service_patch.stop)

    async def test_onebot_action_is_not_called_when_pending_audit_cannot_be_saved(self):
        bot = SimpleNamespace(set_group_add_request=AsyncMock())
        event = SimpleNamespace(flag="request-flag", sub_type="add")
        with (
            patch.object(
                JoinRequestRecordManager,
                "begin_pending_decision",
                return_value=None,
            ),
            patch.object(manager_main, "_notify_admins", new=AsyncMock()),
        ):
            result = await manager_main._set_group_request_with_durable_audit(
                bot,
                event,
                approve=True,
                audit_kwargs={
                    "group_id": "90001",
                    "qq": "10001",
                    "result": "approved",
                    "reasons": ["通过"],
                },
            )

        self.assertFalse(result)
        bot.set_group_add_request.assert_not_awaited()

    async def test_successful_onebot_action_finalizes_pending_audit(self):
        bot = SimpleNamespace(set_group_add_request=AsyncMock())
        event = SimpleNamespace(flag="request-flag", sub_type="add")
        pending = SimpleNamespace(id=7, was_created=True, action_status="pending")
        with (
            patch.object(
                JoinRequestRecordManager,
                "begin_pending_decision",
                return_value=pending,
            ),
            patch.object(
                JoinRequestRecordManager,
                "finalize_pending_decision",
                return_value=True,
            ) as finalize,
        ):
            result = await manager_main._set_group_request_with_durable_audit(
                bot,
                event,
                approve=False,
                reason="来自BOT:拒绝原因",
                audit_kwargs={
                    "group_id": "90001",
                    "qq": "10001",
                    "result": "rejected",
                    "reasons": ["拒绝原因"],
                },
            )

        self.assertTrue(result)
        bot.set_group_add_request.assert_awaited_once_with(
            flag="request-flag",
            sub_type="add",
            approve=False,
            reason="来自BOT:拒绝原因",
        )
        self.assertEqual(finalize.call_args.args, (7,))
        self.assertTrue(finalize.call_args.kwargs["applied"])
        self.assertEqual(finalize.call_args.kwargs["failure_reason"], "")

    async def test_onebot_explicit_failure_marks_pending_audit_failed(self):
        bot = SimpleNamespace(
            set_group_add_request=AsyncMock(side_effect=ActionFailed(retcode=100))
        )
        event = SimpleNamespace(flag="request-flag", sub_type="add")
        pending = SimpleNamespace(id=8, was_created=True, action_status="pending")
        with (
            patch.object(
                JoinRequestRecordManager,
                "begin_pending_decision",
                return_value=pending,
            ),
            patch.object(
                JoinRequestRecordManager,
                "finalize_pending_decision",
                return_value=True,
            ) as finalize,
        ):
            result = await manager_main._set_group_request_with_durable_audit(
                bot,
                event,
                approve=True,
                audit_kwargs={
                    "group_id": "90001",
                    "qq": "10001",
                    "result": "approved",
                    "reasons": ["通过"],
                },
            )

        self.assertFalse(result)
        self.assertEqual(finalize.call_args.args, (8,))
        self.assertFalse(finalize.call_args.kwargs["applied"])
        self.assertIn("ActionFailed", finalize.call_args.kwargs["failure_reason"])

    async def test_finalize_failure_pauses_future_automatic_decisions(self):
        bot = SimpleNamespace(set_group_add_request=AsyncMock())
        event = SimpleNamespace(flag="request-flag", sub_type="add")
        with (
            patch.object(
                JoinRequestRecordManager,
                "begin_pending_decision",
                return_value=SimpleNamespace(id=9, was_created=True, action_status="pending"),
            ),
            patch.object(
                JoinRequestRecordManager,
                "finalize_pending_decision",
                return_value=False,
            ),
            patch.object(manager_main, "_notify_admins", new=AsyncMock()) as notify,
        ):
            result = await manager_main._set_group_request_with_durable_audit(
                bot,
                event,
                approve=True,
                audit_kwargs={
                    "group_id": "90001",
                    "qq": "10001",
                    "result": "approved",
                    "reasons": ["通过"],
                },
            )

        self.assertFalse(result)
        self.assertTrue(manager_main._decision_service.paused)
        notify.assert_awaited_once()

    async def test_already_applied_duplicate_does_not_call_onebot_again(self):
        bot = SimpleNamespace(set_group_add_request=AsyncMock(), self_id="bot-1")
        event = SimpleNamespace(flag="same-request", sub_type="add")
        existing = SimpleNamespace(
            id=10,
            was_created=False,
            action_status="applied",
        )
        with patch.object(
            JoinRequestRecordManager,
            "begin_pending_decision",
            return_value=existing,
        ):
            result = await manager_main._set_group_request_with_durable_audit(
                bot,
                event,
                approve=True,
                audit_kwargs={
                    "group_id": "90001",
                    "qq": "10001",
                    "result": "approved",
                    "reasons": ["通过"],
                },
            )

        self.assertTrue(result)
        bot.set_group_add_request.assert_not_awaited()


class BiliRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_coordinator_caps_concurrency(self):
        coordinator = BiliRequestCoordinator(max_concurrent=2, total_timeout=1)
        active = 0
        maximum = 0
        lock = asyncio.Lock()

        async def operation(_api):
            nonlocal active, maximum
            async with lock:
                active += 1
                maximum = max(maximum, active)
            await asyncio.sleep(0.02)
            async with lock:
                active -= 1

        try:
            await asyncio.gather(
                *(coordinator.run("cookie", operation) for _ in range(5))
            )
        finally:
            await coordinator.close()
        self.assertEqual(maximum, 2)

    async def test_coordinator_enforces_total_timeout(self):
        coordinator = BiliRequestCoordinator(max_concurrent=1, total_timeout=0.01)

        async def slow_operation(_api):
            await asyncio.sleep(1)

        try:
            with self.assertRaises(asyncio.TimeoutError):
                await coordinator.run("cookie", slow_operation)
        finally:
            await coordinator.close()

    async def test_total_timeout_includes_waiting_for_concurrency_slot(self):
        coordinator = BiliRequestCoordinator(max_concurrent=1, total_timeout=0.03)
        started = asyncio.Event()
        release = asyncio.Event()

        async def blocking_operation(_api):
            started.set()
            await release.wait()

        first = asyncio.create_task(coordinator.run("cookie", blocking_operation))
        await started.wait()
        try:
            with self.assertRaises(asyncio.TimeoutError):
                await coordinator.run("cookie", blocking_operation)
        finally:
            release.set()
            try:
                await first
            except asyncio.TimeoutError:
                pass
            await coordinator.close()


class BiliSearchTests(unittest.IsolatedAsyncioTestCase):
    async def test_search_profile_returns_uid_and_level(self):
        api = BiliApi()
        api._get = AsyncMock(
            return_value={
                "code": 0,
                "data": {
                    "result": [
                        {"mid": "123", "uname": "other", "level": 6},
                        {"mid": "456", "uname": "Target", "level": 2},
                    ]
                },
            }
        )
        try:
            result = await api.search_user_profile("target")
        finally:
            await api.close()
        self.assertEqual(result.state, "passed")
        self.assertEqual(result.matched_target_uid, 456)
        self.assertEqual(result.matched_level, 2)

    async def test_search_profile_distinguishes_not_found_from_api_error(self):
        api = BiliApi()
        try:
            api._get = AsyncMock(
                return_value={"code": 0, "data": {"result": [], "numResults": 0}}
            )
            not_found = await api.search_user_profile("missing")
            api._get = AsyncMock(side_effect=RuntimeError("offline"))
            unavailable = await api.search_user_profile("target")
        finally:
            await api.close()
        self.assertEqual(not_found.state, "failed")
        self.assertEqual(
            not_found.detail,
            "未搜索到你填写的B站昵称，请检查昵称是否填写正确，或填写正确的UID后重新申请。请不要填写B站昵称或UID以外的内容",
        )
        self.assertEqual(unavailable.state, "inaccessible")

    async def test_search_profile_rejects_fuzzy_first_result(self):
        api = BiliApi()
        api._get = AsyncMock(
            return_value={
                "code": 0,
                "data": {
                    "result": [{"mid": "123", "uname": "相似但不是本人", "level": 6}],
                    "numResults": 1,
                },
            }
        )
        try:
            result = await api.search_user_profile("目标昵称")
        finally:
            await api.close()

        self.assertEqual(result.state, "failed")
        self.assertIsNone(result.matched_target_uid)
        self.assertEqual(
            result.detail,
            "未搜索到你填写的B站昵称，请检查昵称是否填写正确，或填写正确的UID后重新申请。请不要填写B站昵称或UID以外的内容",
        )

    async def test_search_profile_rejects_multiple_exact_matches(self):
        api = BiliApi()
        api._get = AsyncMock(
            return_value={
                "code": 0,
                "data": {
                    "result": [
                        {"mid": "123", "uname": "Target", "level": 6},
                        {"mid": "456", "uname": "target", "level": 2},
                    ]
                },
            }
        )
        try:
            result = await api.search_user_profile("target")
        finally:
            await api.close()

        self.assertEqual(result.state, "failed")
        self.assertIsNone(result.matched_target_uid)
        self.assertIn("搜索到多个同名B站账号", result.detail)
        self.assertIn("请检查昵称或填写正确的UID", result.detail)
        self.assertIn("请不要填写B站昵称或UID以外的内容", result.detail)


class BiliFollowTests(unittest.IsolatedAsyncioTestCase):
    async def test_common_follow_match_skips_full_following_scan(self):
        api = BiliApi("SESSDATA=value")
        api._get = AsyncMock(
            return_value={
                "code": 0,
                "data": {"total": 1, "list": [{"mid": 200000003}]},
            }
        )
        try:
            result = await api.check_follow_any_status(200000002, [200000003])
        finally:
            await api.close()

        self.assertEqual(result.state, "passed")
        self.assertEqual(result.matched_target_uid, 200000003)
        self.assertEqual(api._get.await_count, 1)
        self.assertIn("same/followings", api._get.await_args.args[0])

    async def test_common_follow_miss_is_failed_without_following_scan(self):
        api = BiliApi("SESSDATA=value")
        api._get = AsyncMock(
            return_value={"code": 0, "data": {"total": 0, "list": []}}
        )
        try:
            result = await api.check_follow_any_status(200000002, [200000003])
        finally:
            await api.close()

        self.assertEqual(result.state, "failed")
        self.assertIn("共同关注中未找到目标主播", result.detail)
        self.assertEqual(api._get.await_count, 1)

    async def test_unverified_common_miss_falls_back_to_full_followings(self):
        api = BiliApi("SESSDATA=value")
        api._get = AsyncMock(
            side_effect=[
                {"code": 0, "data": {"total": 0, "list": []}},
                {
                    "code": 0,
                    "data": {"total": 1, "list": [{"mid": 200000003}]},
                },
            ]
        )
        try:
            result = await api.check_follow_any_status(
                200000002,
                [200000003],
                common_negative_is_definitive=False,
            )
        finally:
            await api.close()

        self.assertEqual(result.state, "passed")
        self.assertIn("same/followings", api._get.await_args_list[0].args[0])
        self.assertIn("/followings", api._get.await_args_list[1].args[0])
        self.assertEqual(api._get.await_count, 2)

    async def test_common_inaccessible_falls_back_to_truncated_followings(self):
        first_page = [{"mid": uid} for uid in range(1, 51)]
        second_page = [{"mid": uid} for uid in range(51, 101)]
        api = BiliApi("SESSDATA=value")
        api._get = AsyncMock(
            side_effect=[
                {"code": 22115, "message": "用户已设置隐私"},
                {"code": 0, "data": {"total": 156, "list": first_page}},
                {"code": 0, "data": {"total": 156, "list": second_page}},
                {"code": 0, "data": {"total": 156, "list": []}},
            ]
        )
        try:
            result = await api.check_follow_any_status(200000002, [200000003])
        finally:
            await api.close()

        self.assertEqual(result.state, "inaccessible")
        self.assertIn("共同关注不可见或无法读取", result.detail)
        self.assertIn("关注列表仅能读取 100/156 项", result.detail)

    async def test_follow_target_found_in_visible_page(self):
        api = BiliApi()
        api._get = AsyncMock(
            return_value={
                "code": 0,
                "data": {
                    "total": 2,
                    "list": [{"mid": 111}, {"mid": "200000003"}],
                },
            }
        )
        try:
            result = await api.check_follow_status(200000002, 200000003)
        finally:
            await api.close()

        self.assertEqual(result.state, "passed")
        self.assertEqual(result.matched_target_uid, 200000003)

    async def test_truncated_short_page_is_inaccessible_not_failed(self):
        api = BiliApi()
        api._get = AsyncMock(
            side_effect=[
                {
                    "code": 0,
                    "data": {"total": 467, "list": [{"mid": 111}]},
                },
                {
                    "code": 0,
                    "data": {"total": 467, "list": []},
                },
            ]
        )
        try:
            result = await api.check_follow_status(200000002, 200000003)
        finally:
            await api.close()

        self.assertEqual(result.state, "inaccessible")
        self.assertIn("1/467", result.detail)
        self.assertEqual(api._get.await_count, 2)

    async def test_complete_list_without_target_is_failed(self):
        api = BiliApi()
        api._get = AsyncMock(
            return_value={
                "code": 0,
                "data": {"total": 2, "list": [{"mid": 111}, {"mid": 222}]},
            }
        )
        try:
            result = await api.check_follow_status(200000002, 200000003)
        finally:
            await api.close()

        self.assertEqual(result.state, "failed")
        self.assertEqual(api._get.await_count, 1)

    async def test_multi_target_fallback_scans_followings_only_once(self):
        api = BiliApi("SESSDATA=value")
        api._get = AsyncMock(
            side_effect=[
                {"code": 22115, "message": "共同关注不可用"},
                {"code": 0, "data": {"total": 0, "list": []}},
            ]
        )
        try:
            result = await api.check_follow_any_status(
                200000002, [200000003, 200000004]
            )
        finally:
            await api.close()

        self.assertEqual(result.state, "failed")
        self.assertEqual(api._get.await_count, 2)

    async def test_pagination_limit_is_inaccessible(self):
        api = BiliApi()
        api._get = AsyncMock(
            return_value={"code": 22007, "message": "访问超过5页"}
        )
        try:
            result = await api.check_follow_status(200000002, 200000003)
        finally:
            await api.close()

        self.assertEqual(result.state, "inaccessible")
        self.assertIn("22007", result.detail)


class BiliLoginFollowTests(unittest.IsolatedAsyncioTestCase):
    async def test_existing_target_follow_is_not_modified(self):
        api = BiliApi("SESSDATA=value; bili_jct=csrf-token")
        api._get = AsyncMock(
            return_value={"code": 0, "data": {"attribute": 2}}
        )
        api._post = AsyncMock()
        try:
            results = await api.ensure_following_targets([200000003, 200000003])
        finally:
            await api.close()

        self.assertEqual(list(results), [200000003])
        self.assertEqual(results[200000003].detail, "已关注")
        api._post.assert_not_awaited()

    async def test_missing_target_follow_is_added_with_csrf(self):
        api = BiliApi("SESSDATA=value; bili_jct=csrf-token")
        api._get = AsyncMock(
            return_value={"code": 0, "data": {"attribute": 0}}
        )
        api._post = AsyncMock(return_value={"code": 0, "message": "0"})
        try:
            results = await api.ensure_following_targets([200000003])
        finally:
            await api.close()

        self.assertEqual(results[200000003].state, "passed")
        self.assertEqual(results[200000003].detail, "已自动关注")
        payload = api._post.await_args.args[1]
        self.assertEqual(
            payload,
            {"fid": 200000003, "act": 1, "re_src": 11, "csrf": "csrf-token"},
        )

    async def test_missing_csrf_does_not_attempt_follow_request(self):
        api = BiliApi("SESSDATA=value")
        api._get = AsyncMock(
            return_value={"code": 0, "data": {"attribute": 0}}
        )
        api._post = AsyncMock()
        try:
            results = await api.ensure_following_targets([200000003])
        finally:
            await api.close()

        self.assertEqual(results[200000003].state, "inaccessible")
        self.assertIn("bili_jct", results[200000003].detail)
        api._post.assert_not_awaited()

    async def test_startup_cookie_triggers_target_follow_sync(self):
        sync = AsyncMock(return_value="目标主播关注检查完成")
        with patch.object(
            ConfigManager, "get_bili_cookie", return_value="SESSDATA=value"
        ), patch.object(
            manager_main, "_ensure_login_account_follows_targets", sync
        ):
            await manager_main.ensure_bili_cookie_on_startup()

        sync.assert_awaited_once_with("SESSDATA=value")

    def test_configured_target_uids_are_deduplicated(self):
        config = PluginConfig(
            groups={
                "1": GroupConfig(group_id="1", target_uids=[200000003, 123]),
                "2": GroupConfig(group_id="2", target_uids=[123, 456]),
            }
        )
        with patch.object(ConfigManager, "get_config", return_value=config):
            self.assertEqual(
                manager_main._configured_follow_target_uids(),
                [200000003, 123, 456],
            )


class BiliMedalTests(unittest.IsolatedAsyncioTestCase):
    async def test_closed_medal_wall_is_inaccessible_not_missing(self):
        api = BiliApi()
        api._get = AsyncMock(
            return_value={
                "code": 0,
                "data": {
                    "close_space_medal": 1,
                    "only_show_wearing": 1,
                    "count": 0,
                    "list": [],
                },
            }
        )
        try:
            result = await api.check_medal_any_status(200000001, [200000003], 1)
        finally:
            await api.close()

        self.assertEqual(result.state, "inaccessible")
        self.assertIn("无法获取完整粉丝牌列表", result.detail)

    async def test_complete_empty_medal_wall_means_no_medal(self):
        api = BiliApi()
        api._get = AsyncMock(
            return_value={
                "code": 0,
                "data": {
                    "close_space_medal": 0,
                    "only_show_wearing": 0,
                    "count": 0,
                    "list": [],
                },
            }
        )
        try:
            result = await api.check_medal_any_status(123, [200000003], 1)
        finally:
            await api.close()

        self.assertEqual(result.state, "failed")

    def test_inaccessible_medal_uses_unknown_pipeline_branch(self):
        config = GroupConfig(group_id="1", target_medal_uids=[200000003])
        statuses = _build_rule_condition_statuses(
            config,
            {"medal"},
            qq_level=None,
            bili_search_result=CheckResult("passed", matched_target_uid=123),
            bili_level=None,
            follow_result=CheckResult("not_required"),
            medal_result=CheckResult("inaccessible", "灯牌墙已关闭"),
            leave_record=None,
            conflict_qq=None,
            owned_other_bili_uids=[],
        )

        self.assertEqual(statuses["medal"].state, "unknown")
        self.assertIn("暂无法确认粉丝牌或等级", statuses["medal"].reason)
        self.assertIn("灯牌墙已关闭", statuses["medal"].reason)
        self.assertIn("请稍后重试或联系管理员", statuses["medal"].reason)


class LocalLoggingPolicyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        manager_main._reset_disk_alert_email_state()

    def test_removed_features_are_not_configurable(self):
        removed_fields = {
            "manual_review_enabled",
            "manual_review_target_type",
            "manual_review_target_id",
            "manual_review_timeout_seconds",
            "log_archive_enabled",
            "log_archive_webdav_url",
            "log_archive_username",
            "log_archive_password",
            "log_archive_directory",
            "log_archive_interval_minutes",
            "log_archive_min_age_minutes",
            "log_archive_keep_days",
        }
        self.assertTrue(removed_fields.isdisjoint(PluginConfig.model_fields))

    async def test_low_disk_space_notifies_superadmin(self):
        bot = SimpleNamespace(send_private_msg=AsyncMock())
        usage = SimpleNamespace(free=int(0.1 * 1024**3))
        sender = AsyncMock(return_value=True)
        with (
            patch("src.plugins.group_request_manager.main.shutil.disk_usage", return_value=usage),
            patch("src.plugins.group_request_manager.main._get_superadmin_qqs", return_value={"12345"}),
            patch("src.plugins.group_request_manager.main.email_notifier.send_notification", sender),
            patch("nonebot.get_bots", return_value={"bot": bot}),
        ):
            await _check_disk_space_once()
        bot.send_private_msg.assert_awaited_once()
        sender.assert_awaited_once()
        kwargs = bot.send_private_msg.await_args.kwargs
        self.assertEqual(kwargs["user_id"], 12345)
        self.assertIn("磁盘告警-严重", kwargs["message"])
        self.assertIn("磁盘告警", sender.await_args.args[0])

    async def test_disk_space_email_is_deduplicated_until_recovery(self):
        bot = SimpleNamespace(send_private_msg=AsyncMock())
        low_usage = SimpleNamespace(free=int(0.1 * 1024**3))
        ok_usage = SimpleNamespace(free=int(5 * 1024**3))
        sender = AsyncMock(return_value=True)
        with (
            patch(
                "src.plugins.group_request_manager.main.shutil.disk_usage",
                side_effect=[low_usage, low_usage, ok_usage, low_usage],
            ),
            patch("src.plugins.group_request_manager.main._get_superadmin_qqs", return_value={"12345"}),
            patch("src.plugins.group_request_manager.main.email_notifier.send_notification", sender),
            patch("nonebot.get_bots", return_value={"bot": bot}),
        ):
            await _check_disk_space_once()
            await _check_disk_space_once()
            await _check_disk_space_once()
            await _check_disk_space_once()
        self.assertEqual(sender.await_count, 2)

    async def test_failed_disk_space_email_retries(self):
        bot = SimpleNamespace(send_private_msg=AsyncMock())
        usage = SimpleNamespace(free=int(0.1 * 1024**3))
        sender = AsyncMock(return_value=False)
        with (
            patch("src.plugins.group_request_manager.main.shutil.disk_usage", return_value=usage),
            patch("src.plugins.group_request_manager.main._get_superadmin_qqs", return_value={"12345"}),
            patch("src.plugins.group_request_manager.main.email_notifier.send_notification", sender),
            patch("nonebot.get_bots", return_value={"bot": bot}),
        ):
            await _check_disk_space_once()
            await _check_disk_space_once()
        self.assertEqual(sender.await_count, 2)

    async def test_disk_space_check_failure_sends_email(self):
        sender = AsyncMock(return_value=True)
        with (
            patch(
                "src.plugins.group_request_manager.main.shutil.disk_usage",
                side_effect=OSError("disk unavailable"),
            ),
            patch("src.plugins.group_request_manager.main.email_notifier.send_notification", sender),
        ):
            await _check_disk_space_once()
        sender.assert_awaited_once()
        self.assertIn("检查失败", sender.await_args.args[0])
        self.assertIn("无法获取", sender.await_args.args[1])


    async def test_disk_monitor_loop_survives_exceptions(self):
        """_disk_space_monitor should keep running even if _check_disk_space_once raises."""
        real_sleep = asyncio.sleep
        call_count = 0

        async def flaky_check():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("unexpected failure")

        async def fast_sleep(seconds):
            # Use real sleep with tiny delay to yield control
            await real_sleep(min(seconds, 0.01))

        with (
            patch.object(
                manager_main, "_check_disk_space_once", side_effect=flaky_check
            ),
            patch.object(manager_main.asyncio, "sleep", side_effect=fast_sleep),
        ):
            task = asyncio.ensure_future(_disk_space_monitor())
            try:
                await real_sleep(0.15)
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self.assertGreaterEqual(call_count, 2)



class WebUICustomCssTests(unittest.TestCase):
    token = "test-token-with-at-least-32-characters"

    def test_relative_custom_css_path_uses_working_directory(self):
        self.assertEqual(
            _resolve_custom_css_path("theme.css"),
            (Path.cwd() / "theme.css").resolve(),
        )

    def test_pipeline_file_follows_group_config_path(self):
        config = PluginConfig(
            groups={
                "1": GroupConfig(group_id="1", review_pipeline_file="configs/review_pipeline.yaml")
            }
        )
        self.assertEqual(
            _resolve_pipeline_file(config),
            (Path.cwd() / "configs/review_pipeline.yaml").resolve(),
        )

    def test_custom_css_is_served_with_token_authentication(self):
        with tempfile.TemporaryDirectory() as directory:
            css_path = Path(directory) / "theme.css"
            css_text = ":root { --accent: #123456; }"
            css_path.write_text(css_text, encoding="utf-8")
            config = PluginConfig(
                webui_enabled=True,
                webui_host="127.0.0.1",
                webui_port=0,
                webui_token=self.token,
                webui_custom_css_file=str(css_path),
            )
            server = start_webui(config)
            self.assertIsNotNone(server)
            assert server is not None
            port = server.server_address[1]
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            try:
                connection.request("GET", "/custom.css")
                unauthorized = connection.getresponse()
                self.assertEqual(unauthorized.status, 401)
                unauthorized.read()

                connection.request(
                    "POST",
                    "/api/login",
                    body=json.dumps({"token": self.token}),
                    headers={"Content-Type": "application/json"},
                )
                login = connection.getresponse()
                self.assertEqual(login.status, 200)
                # 顺带校验登录响应是合法 JSON
                json.loads(login.read())
                cookie = login.getheader("Set-Cookie")
                self.assertIn("HttpOnly", cookie)
                self.assertIn("SameSite=Strict", cookie)
                session_cookie = cookie.split(";", 1)[0]

                connection.request("GET", "/custom.css", headers={"Cookie": session_cookie})
                response = connection.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(response.read().decode("utf-8"), css_text)
                self.assertEqual(response.getheader("Content-Type"), "text/css; charset=utf-8")

                connection.request(
                    "PUT",
                    "/api/pipeline",
                    body=json.dumps({"pipeline": {"version": 2, "groups": []}}),
                    headers={"Content-Type": "application/json", "Cookie": session_cookie},
                )
                self.assertEqual(connection.getresponse().status, 403)
                connection.close()

                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
                connection.request("GET", f"/api/session?token={self.token}")
                query_token = connection.getresponse()
                self.assertEqual(query_token.status, 401)
                query_token.read()

                connection.request("GET", "/")
                root = connection.getresponse()
                root.read()
                self.assertEqual(root.status, 200)
                self.assertIn("nonce-", root.getheader("Content-Security-Policy"))
                self.assertEqual(root.getheader("X-Frame-Options"), "DENY")
            finally:
                connection.close()
                server.shutdown()
                server.server_close()

    def test_login_rate_limit_and_public_bind_guard(self):
        insecure_public = PluginConfig(
            webui_host="0.0.0.0",
            webui_port=0,
            webui_token=self.token,
        )
        self.assertIsNone(start_webui(insecure_public))

        config = PluginConfig(
            webui_host="127.0.0.1",
            webui_port=0,
            webui_token=self.token,
            webui_login_max_attempts=2,
        )
        server = start_webui(config)
        self.assertIsNotNone(server)
        assert server is not None
        connection = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
        try:
            for expected in (401, 401, 429):
                connection.request(
                    "POST",
                    "/api/login",
                    body=json.dumps({"token": "wrong-token"}),
                    headers={"Content-Type": "application/json"},
                )
                response = connection.getresponse()
                self.assertEqual(response.status, expected)
                response.read()
        finally:
            connection.close()
            server.shutdown()
            server.server_close()

    def test_https_proxy_requirement_sets_secure_host_cookie(self):
        config = PluginConfig(
            webui_host="127.0.0.1",
            webui_port=0,
            webui_token=self.token,
            webui_require_https=True,
            webui_trust_proxy_headers=True,
        )
        server = start_webui(config)
        self.assertIsNotNone(server)
        assert server is not None
        connection = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
        try:
            connection.request("GET", "/")
            insecure = connection.getresponse()
            self.assertEqual(insecure.status, 426)
            insecure.read()

            proxy_headers = {"X-Forwarded-Proto": "https"}
            connection.request("GET", "/", headers=proxy_headers)
            secure_root = connection.getresponse()
            self.assertEqual(secure_root.status, 200)
            self.assertEqual(secure_root.getheader("Strict-Transport-Security"), "max-age=31536000")
            secure_root.read()

            connection.request(
                "POST",
                "/api/login",
                body=json.dumps({"token": self.token}),
                headers={"Content-Type": "application/json", **proxy_headers},
            )
            login = connection.getresponse()
            self.assertEqual(login.status, 200)
            cookie = login.getheader("Set-Cookie")
            login.read()
            self.assertTrue(cookie.startswith("__Host-gatekeeper_session="))
            self.assertIn("Secure", cookie)
            self.assertIn("HttpOnly", cookie)
        finally:
            connection.close()
            server.shutdown()
            server.server_close()


class CommandCaseVariantsTests(unittest.TestCase):
    """命令名大小写不敏感变体生成测试。"""

    def test_pure_chinese_no_variants(self):
        self.assertEqual(_command_case_variants("设置群"), {"设置群"})

    def test_two_english_letters_four_variants(self):
        result = _command_case_variants("检查qq")
        self.assertEqual(len(result), 4)
        self.assertIn("检查qq", result)
        self.assertIn("检查qQ", result)
        self.assertIn("检查Qq", result)
        self.assertIn("检查QQ", result)

    def test_mixed_chinese_english(self):
        result = _command_case_variants("检查bili")
        self.assertEqual(len(result), 16)  # 2^4 = 16
        self.assertIn("检查bili", result)
        self.assertIn("检查BILI", result)
        self.assertIn("检查Bili", result)

    def test_pure_english(self):
        result = _command_case_variants("help")
        self.assertEqual(len(result), 16)  # 2^4 = 16
        self.assertIn("help", result)
        self.assertIn("HELP", result)
        self.assertIn("Help", result)
        self.assertIn("hElp", result)

    def test_cookie_variants(self):
        result = _command_case_variants("设置cookie")
        self.assertEqual(len(result), 64)  # 2^6 = 64
        self.assertIn("设置cookie", result)
        self.assertIn("设置COOKIE", result)
        self.assertIn("设置Cookie", result)

    def test_single_letter_two_variants(self):
        result = _command_case_variants("a")
        self.assertEqual(result, {"a", "A"})

    def test_no_letters_single_variant(self):
        self.assertEqual(_command_case_variants("123"), {"123"})
        self.assertEqual(_command_case_variants(""), {""})


if __name__ == "__main__":
    unittest.main()
