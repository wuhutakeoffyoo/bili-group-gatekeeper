# V3 文档目录与功能核对

本目录说明截至 2026-11-11 核对的 V3 独立实现，QQ 接入使用 SnowLuma 并保留 NapCat 兼容。生产基线见[迁移记录](production-acceptance-20261026.md)，后续生产修复与边界见[申请漏收核查](snowluma-request-incident-20261030.md)；真实 QQ 审批状态不能由文档或单元测试推断。

## 按任务阅读

| 任务 | 文档 |
| --- | --- |
| 安装、启动、私聊命令 | [项目 README](../README.md) |
| 默认入口、模块边界、并发与限流 | [独立运行架构](architecture.md) |
| 配置优先级、首次旧配置迁移、SQL 版本、YAML 备份和回滚 | [配置与迁移](configuration.md) |
| T/F/I、AST、UID/昵称、pending | [审核规则](review-requirements.md) |
| 所有拒绝类型与可操作反馈 | [拒绝反馈](rejection-feedback.md) |
| 手动/扫码/刷新/CookieCloud | [凭证管理](credentials.md) |
| 管理界面、API、登录安全与本地恢复 | [管理控制台](admin-console.md) |
| 所有权挑战、H5、巡检及退群策略 | [所有权与巡检](ownership-and-patrol.md) |
| systemd、备份、升级和软件回退 | [部署与升级](deploy-systemd.md) |
| SnowLuma、后台运行、QQ 登录与 NapCat 迁移 | [SnowLuma 接入](snowluma-migration.md) |
| 唯一生产入口、适配器目录与回滚副本 | [生产运维](operations-layout.md) |
| 本次上线的只读实测、配置数据核对及未验收项 | [2026-10-26 生产验收](production-acceptance-20261026.md) |
| SnowLuma 申请类型缺陷、私有修复与真实补审 | [2026-10-30 漏收核查](snowluma-request-incident-20261030.md) |
| Cookie/磁盘邮件告警 | [邮件告警](cookie-email-alert.md) |
| 代码覆盖与外部验收限制 | [功能状态](development-roadmap.md) |
| V3 架构取舍 | [ADR 0003](adr/0003-standalone-runtime-sql-config.md) |
| 变更与开发交接 | [更新日志](../CHANGELOG.md)、[贡献指南](../CONTRIBUTING.md)、[交接文档](../AI_HANDOFF.md) |

## 当前实现对应

| 能力 | 默认代码 |
| --- | --- |
| 单一 ASGI 与 OneBot | `app/main.py`、`app/runtime.py`、`app/onebot/` |
| SnowLuma 契约、跨适配器去重与启动补审 | `app/onebot/snowluma.py`、`inspector.py`、`recovery.py`、`app/service.py` |
| 规则、表达式、数值与反馈 | `src/gatekeeper/`、`app/service.py` |
| SQL 配置、来源备份、CAS、v4 | `app/config.py`、`app/legacy_config.py`、`app/database/` |
| 局部 pending 与精确对账 | `app/gatekeeper/decisions.py`、`reconciler.py` |
| 限流、资料缓存与凭证 | `app/bilibili/` |
| 所有权与巡检 | `app/ownership.py`、`app/patrol.py` |
| 管理 API/UI 和 H5 | `app/web/` |
| SMTP 与磁盘通知 | `app/notifications/`、`app/runtime.py` |

## 历史资料

[ADR 0001](adr/0001-sqlite-record-storage.md) 和 [ADR 0002](adr/0002-framework-independent-core.md) 保留以前阶段的决策，当前被替代的部分已标注。历史需求与旧规划也不作为现有功能说明：

- [2026-06-21 规划原文](history/development-roadmap-20260621.md)
- [原始筛选需求](history/review-requirements-original.md)

完整原始蓝图的实施核对、验证证据及仍需真实账号验收的项目见 [V3 实施与验收](v3-implementation.md)。本地回归不能代替外部动作或生产切换的记录。
