"""Authoritative SQLite configuration with a one-time legacy import.

Dotenv/JSON/YAML precedence is compiled only when config_head is empty. Later
restarts load the immutable SQL revision, never silently reapply old files.
"""
from __future__ import annotations

import hashlib
import base64
import json
import logging
import os
from pathlib import Path
import threading
import time
from typing import Any

import yaml

from app.database import legacy as database
from app import legacy_config as legacy_paths
from app.database.repository import ConfigRepository, ConfigVersionConflict
from app.legacy_config import (  # noqa: F401 - compatibility reexports
    ACTION_ALIASES, BOOL_TRUE_VALUES, DEFAULT_STAGE_BRANCH_FLOWS, REVIEW_CONDITIONS,
    STAGE_ACTIONS, STAGE_FLOW_ACTIONS, STAGE_MODES, STAGE_SIDE_EFFECT_ACTIONS,
    GroupConfig, LegacyConfigLoader, PluginConfig, ReviewStageConfig,
    load_group_configs_from_yaml_file, load_review_stages_from_yaml_file,
    parse_action_list, parse_bool, parse_condition_list, parse_custom_reject_uids,
    parse_named_stage_target, parse_reject_reasons, parse_str_list, parse_uid_list,
    resolve_config_path,
)
from app.settings import get_settings

REDACTED = "[redacted]"
logger = logging.getLogger(__name__)


def _secret_key(key: str) -> bool:
    normalized = key.lower()
    if normalized in {"credential_generation", "refresh_enabled", "token_expires_at"}:
        return False
    return normalized in {"cookie", "authorization", "bili_refresh_pending", "cookiecloud_uuid", "cookiecloud_key"} or normalized.endswith("_cookie") or any(
        part in normalized for part in ("token", "password", "secret", "private_key")
    )


def redact_secrets(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): REDACTED if _secret_key(str(key)) and item else redact_secrets(item) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_secrets(item) for item in value]
    return value


def _preserve_redacted(value: Any, previous: Any) -> Any:
    if value == REDACTED:
        return previous if previous is not None else ""
    if isinstance(value, dict):
        prior = previous if isinstance(previous, dict) else {}
        return {key: _preserve_redacted(item, prior.get(key)) for key, item in value.items()}
    if isinstance(value, list):
        prior = previous if isinstance(previous, list) else []
        return [_preserve_redacted(item, prior[index] if index < len(prior) else None) for index, item in enumerate(value)]
    return value


def _validate_config(config: PluginConfig | dict[str, Any]) -> PluginConfig:
    from app.ownership import OwnershipService
    from app.patrol import PatrolService
    from src.gatekeeper.pipeline import get_review_stages
    candidate = PluginConfig.model_validate(config.model_dump() if isinstance(config, PluginConfig) else config)
    for group_id, group in candidate.groups.items():
        if str(group_id) != group.group_id:
            raise ValueError(f"群配置键 {group_id} 与 group_id {group.group_id} 不一致")
        if min(group.required_qq_level, group.required_bili_level, group.required_medal_level) < 0:
            raise ValueError("等级要求不能为负数")
        if group.review_stages:
            get_review_stages(group)
        ownership = OwnershipService._policy(group)
        if ownership:
            OwnershipService.public_base(ownership, candidate)
        PatrolService._policy(group)
    if candidate.notify_admins_on_abnormal_request not in {"all", "superusers_only", "none"}:
        raise ValueError("异常通知模式必须为 all / superusers_only / none")
    if not 1 <= candidate.webui_port <= 65535:
        raise ValueError("WebUI 端口必须位于 1..65535")
    return candidate


def _source_manifest(settings, config: PluginConfig, root: Path) -> dict[str, Any]:
    """Keep exact source text privately so the initial compilation is explainable."""
    environment = str(getattr(settings, "environment", "prod") or "prod")
    paths = {
        root / ".env", root / f".env.{environment}", root / "data/runtime/plugin_state.json",
        root / "data/runtime/group_overrides.json", root / "data/config/plugin_config.json",
    }
    paths.update(legacy_paths.SOURCE_PATHS)
    groups_file = getattr(settings, "group_config_file", "") or os.getenv("GROUP_CONFIG_FILE", "")
    if groups_file:
        paths.add(resolve_config_path(groups_file, root).resolve())
    for group in config.groups.values():
        if group.review_pipeline_file:
            paths.add(resolve_config_path(group.review_pipeline_file, root).resolve())
    # Also preserve superseded YAML sources named by runtime overrides/numbered groups.
    for name, value in vars(settings).items():
        if name.lower().endswith("review_pipeline_file") and value:
            paths.add(resolve_config_path(value, root).resolve())
    files = []
    for path in sorted(paths):
        if not path.is_file():
            continue
        raw = path.read_bytes()
        files.append({
            "path": str(path), "sha256": hashlib.sha256(raw).hexdigest(),
            "text": raw.decode("utf-8-sig"), "raw_base64": base64.b64encode(raw).decode("ascii"),
        })
    return {
        "version": 1, "imported_at": time.time(), "root": str(root), "files": files,
        "precedence": ["runtime_or_legacy_json", "group_yaml", "runtime_group_overrides", "GROUP_CONFIGS", "GROUP_ID", "GROUP_N", "pipeline_yaml"],
        "effective_settings": {
            str(key): value for key, value in vars(settings).items()
            if (str(key).lower().startswith(("group_", "bili_", "cookiecloud_", "smtp_", "notify_", "webui_"))
                or str(key).lower() in {"cookie_check_interval_seconds", "admin_qqs"})
            and isinstance(value, (str, int, float, bool, list, dict, type(None)))
        },
    }


def _private_source_backup(root: Path, manifest: dict[str, Any]) -> Path:
    folder = root / "data/backups" / f"config-import-{time.time_ns()}"
    folder.mkdir(parents=True, exist_ok=False)
    folder.chmod(0o700)
    path = folder / "legacy-sources.json"
    # Open mode 0600 from creation, not after writing secret bytes.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)
    path.chmod(0o600)
    return path


class ConfigManager:
    """Legacy synchronous API backed by append-only, CAS-protected SQL revisions."""
    _config: PluginConfig | None = None
    _version: int = 0
    _initialized: bool = False
    _root: Path | None = None
    _lock = threading.RLock()

    @classmethod
    def initialize(cls, settings=None, database_path: Path | str | None = None) -> PluginConfig:
        settings = settings or get_settings()
        root = Path(getattr(settings, "app_root", Path.cwd())).resolve()
        selected = database_path or getattr(settings, "database_path", "") or getattr(settings, "gatekeeper_database", "")
        path = Path(selected).expanduser() if selected else Path("data/records/gatekeeper.sqlite3")
        if not path.is_absolute():
            path = root / path
        with cls._lock:
            cls._root = root
            legacy_paths.CONFIG_ROOT = root
            database.configure_database(path, root=root)
            database.initialize_database()
            row = ConfigRepository.current()
            if row is None:
                config = LegacyConfigLoader.load(settings)
                # Infrastructure credentials previously lived only in driver config.
                for name, value in vars(settings).items():
                    if (name.startswith(("cookiecloud_", "smtp_", "notify_smtp_", "ownership_", "patrol_"))
                            or name in {"notify_from_addr", "notify_to_addr", "cookie_check_interval_seconds"}) and value not in (None, ""):
                        setattr(config, name, value)
                refresh = getattr(settings, "bili_refresh_token", "") or os.getenv("BILI_REFRESH_TOKEN", "")
                if refresh:
                    config.bili_refresh_token = str(refresh)
                if config.bili_cookie and not config.credential_generation:
                    config.credential_generation = 1
                config = _validate_config(config)
                manifest = _source_manifest(settings, config, root)
                manifest["backup"] = str(_private_source_backup(root, manifest))
                try:
                    ConfigRepository.append(config.model_dump(), expected_version=0, actor="legacy-import", manifest=manifest)
                except ConfigVersionConflict:
                    # Another process completed the first import. Its SQL revision wins.
                    pass
                row = ConfigRepository.current()
            assert row is not None
            cls._config = _validate_config(json.loads(row["config_json"]))
            cls._version = int(row["version"])
            cls._initialized = True
            logger.info("Loaded business configuration from SQLite revision %s; group enabled values: %s",
                        cls._version, {group_id: group.enabled for group_id, group in cls._config.groups.items()})
            return cls._config

    @classmethod
    def _refresh(cls) -> None:
        if not cls._initialized:
            cls.initialize()
        row = ConfigRepository.current()
        if row is None:
            raise RuntimeError("配置数据库没有当前版本")
        if int(row["version"]) != cls._version or cls._config is None:
            cls._config = _validate_config(json.loads(row["config_json"]))
            cls._version = int(row["version"])

    @classmethod
    def get_config(cls) -> PluginConfig:
        with cls._lock:
            cls._refresh()
            assert cls._config is not None
            return cls._config

    @classmethod
    def get_version(cls) -> int:
        with cls._lock:
            cls._refresh()
            return cls._version

    @classmethod
    def get_public_config(cls) -> dict[str, Any]:
        return redact_secrets(cls.get_config().model_dump())

    @classmethod
    def get_snapshot(cls, include_secrets: bool = False) -> dict[str, Any]:
        with cls._lock:
            config = cls.get_config().model_dump()
            return {"version": cls._version, "config": config if include_secrets else redact_secrets(config)}

    @classmethod
    def replace_config(cls, config: PluginConfig | dict[str, Any], expected_version: int, actor: str = "") -> int:
        with cls._lock:
            cls.get_config()
            row = ConfigRepository.current()
            assert row is not None
            current = json.loads(row["config_json"])
            if int(row["version"]) != int(expected_version):
                raise ConfigVersionConflict(f"配置版本冲突：当前版本 {row['version']}，提交版本 {expected_version}；请刷新后重试")
            raw = config.model_dump() if isinstance(config, PluginConfig) else dict(config)
            candidate = _validate_config(_preserve_redacted(raw, current))
            current_generation = int(current.get("credential_generation", 0))
            changed = candidate.bili_cookie != current.get("bili_cookie", "") or candidate.bili_refresh_token != current.get("bili_refresh_token", "")
            session_actor = actor in {"credential-service", "credentials-update", "cookie-update"}
            if changed or session_actor:
                candidate.credential_generation = current_generation + 1
            else:
                # Importing a historic or edited generation cannot reset or
                # advance it independently of an actual session operation.
                candidate.credential_generation = current_generation
            if actor not in {"credential-service", "credential-confirm"}:
                if changed or session_actor:
                    candidate.bili_refresh_pending = {}
                    if (candidate.bili_cookie != current.get("bili_cookie", "")
                            and candidate.bili_refresh_token == current.get("bili_refresh_token", "")
                            and not session_actor):
                        candidate.bili_refresh_token = ""
                else:
                    # Ordinary rule edits keep the live recovery intent; they
                    # cannot inject an old token-invalidation request.
                    candidate.bili_refresh_pending = current.get("bili_refresh_pending", {})
            version = ConfigRepository.append(candidate.model_dump(), expected_version=expected_version, actor=actor)
            cls._config, cls._version = candidate, version
            return version

    @classmethod
    def list_versions(cls, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        cls.get_config()
        return ConfigRepository.versions(limit, offset)

    @classmethod
    def restore_version(cls, version: int, expected_version: int, actor: str = "") -> int:
        with cls._lock:
            cls.get_config()
            current_row = ConfigRepository.current()
            assert current_row is not None
            if int(current_row["version"]) != int(expected_version):
                raise ConfigVersionConflict(f"配置版本冲突：当前版本 {current_row['version']}，提交版本 {expected_version}；请刷新后重试")
            current = json.loads(current_row["config_json"])
            row = ConfigRepository.get(version)
            if row is None:
                raise ValueError(f"配置版本不存在：{version}")
            restored = json.loads(row["config_json"])
            for field_name, default in (("bili_cookie", ""), ("bili_refresh_token", ""), ("credential_generation", 0), ("bili_refresh_pending", {})):
                restored[field_name] = current.get(field_name, default)
            candidate = _validate_config(restored)
            new_version = ConfigRepository.append(
                candidate.model_dump(), expected_version=expected_version, actor=actor,
                manifest=json.loads(row["manifest_json"]), restored_from=int(version),
            )
            cls._config, cls._version = candidate, new_version
            return new_version

    @classmethod
    def export_yaml(cls, include_secrets: bool = False) -> str:
        return yaml.safe_dump(
            {"version": 3, "config": cls.get_snapshot(include_secrets)["config"]},
            allow_unicode=True, sort_keys=False,
        )

    @classmethod
    def import_yaml(cls, text: str, expected_version: int, actor: str = "") -> int:
        try:
            payload = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise ValueError("配置 YAML 解析失败") from exc
        if not isinstance(payload, dict) or payload.get("version") != 3 or not isinstance(payload.get("config"), dict):
            raise ValueError("配置导入需要 version: 3 与 config 对象；旧配置仅在首次启动迁移")
        return cls.replace_config(payload["config"], expected_version, actor)

    @classmethod
    def save_config(cls) -> None:
        with cls._lock:
            candidate = cls.get_config().model_copy(deep=True)
            cls.replace_config(candidate, cls._version, "legacy-compatible-save")

    _save_config = save_config

    @classmethod
    def get_group_config(cls, group_id: str) -> GroupConfig | None:
        return cls.get_config().groups.get(str(group_id))

    @classmethod
    def set_group_config(cls, group_config: GroupConfig, updated_fields: list[str] | None = None) -> None:
        with cls._lock:
            candidate = cls.get_config().model_copy(deep=True)
            if updated_fields is not None:
                # Commands may hold an older group object. Apply only the fields
                # they actually edited to the latest SQL head, retaining others.
                current = candidate.groups.get(group_config.group_id) or GroupConfig(group_id=group_config.group_id)
                incoming = group_config.model_dump()
                for name in updated_fields:
                    name = "enabled" if name == "enable" else name
                    if name == "group_id" or name not in GroupConfig.model_fields:
                        raise ValueError(f"不支持的群配置更新字段：{name}")
                    setattr(current, name, incoming[name])
                candidate.groups[group_config.group_id] = current
            else:
                candidate.groups[group_config.group_id] = group_config.model_copy(deep=True)
            cls.replace_config(candidate, cls._version, "group-update")

    @classmethod
    def remove_group_config(cls, group_id: str, expected_version: int | None = None, actor: str = "") -> int:
        with cls._lock:
            candidate = cls.get_config().model_copy(deep=True)
            if expected_version is not None and int(expected_version) != cls._version:
                raise ConfigVersionConflict("配置版本冲突；请刷新后重试")
            candidate.groups.pop(str(group_id), None)
            return cls.replace_config(candidate, cls._version if expected_version is None else expected_version, actor or "group-delete")

    @classmethod
    def set_credentials(cls, cookie: str, refresh_token: str = "", expected_generation: int | None = None) -> dict[str, int]:
        with cls._lock:
            candidate = cls.get_config().model_copy(deep=True)
            if expected_generation is not None and candidate.credential_generation != int(expected_generation):
                raise ConfigVersionConflict("凭证已被其他操作更新；请重新读取后重试")
            candidate.bili_cookie = str(cookie).strip()
            candidate.bili_refresh_token = str(refresh_token).strip()
            candidate.bili_refresh_pending = {}
            candidate.credential_generation += 1
            version = cls.replace_config(candidate, cls._version, "credentials-update")
            return {"generation": candidate.credential_generation, "version": version}

    @classmethod
    def set_bili_cookie(cls, cookie: str) -> None:
        with cls._lock:
            candidate = cls.get_config().model_copy(deep=True)
            candidate.bili_cookie = str(cookie).strip()
            # Cookie-only imports may switch account. A refresh token belongs to
            # the previous session and must not be paired with an arbitrary cookie.
            candidate.bili_refresh_token = ""
            candidate.bili_refresh_pending = {}
            candidate.credential_generation += 1
            cls.replace_config(candidate, cls._version, "cookie-update")

    @classmethod
    def get_bili_cookie(cls) -> str:
        # Deliberately no environment fallback after the first SQL import.
        return cls.get_config().bili_cookie.strip()

    @classmethod
    def get_admin_qqs(cls) -> list[str]:
        return cls.get_config().admin_qqs

    @classmethod
    def get_notify_mode(cls) -> str:
        return cls.get_config().notify_admins_on_abnormal_request

    @classmethod
    def reset_state(cls) -> None:
        """Clear only process-local state; SQL revisions remain authoritative."""
        with cls._lock:
            cls._config, cls._version, cls._initialized, cls._root = None, 0, False, None
