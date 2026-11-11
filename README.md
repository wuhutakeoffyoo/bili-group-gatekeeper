# Bilibili QQ Group Gatekeeper

[![CI](https://github.com/wuhutakeoffyoo/bilibili-qq-group-gatekeeper/actions/workflows/ci.yml/badge.svg)](https://github.com/wuhutakeoffyoo/bilibili-qq-group-gatekeeper/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

通过 B站账号资料审核 QQ 入群申请的独立服务。**V3 默认使用 FastAPI + 独立 OneBot v11 WebSocket，支持 SnowLuma 与 NapCat；当前项目生产接入使用 SnowLuma，默认运行不依赖 NoneBot2。** 审核规则、凭证和配置历史统一保存在 SQLite v4。

本页说明仓库实现。生产基线见 [2026-10-26 迁移记录](docs/production-acceptance-20261026.md)，后续申请漏收修复及验收边界见 [2026-10-30 核查记录](docs/snowluma-request-incident-20261030.md)。升级前请备份完整 `data/`、实际环境文件和服务配置，见[部署与迁移](docs/deploy-systemd.md)。

[文档目录](docs/README.md) · [架构](docs/architecture.md) · [配置与迁移](docs/configuration.md) · [审核规则](docs/review-requirements.md) · [拒绝反馈](docs/rejection-feedback.md) · [更新日志](CHANGELOG.md)

## 功能

- **独立运行**：一个 ASGI 服务承载正向/反向 OneBot、管理控制台和可选 H5；接收回包与处理事件分开。
- **规则**：T/F/I 三值、八项条件、安全 AST 布尔/等级表达式、兼容 v2 `conditions + mode` 和阶段跳转。
- **严格输入分流**：明确 UID 标记/纯数字只走 UID 名片接口；含字符昵称只走搜索。清洗 UID，拦截可能误填的申请 QQ，不做双路兜底猜测。
- **清晰拒绝**：反馈实际事实、门槛/目标和修正方法；账号错误提示检查 UID/昵称，只填写两者；自定义说明不掩盖实际原因。
- **资料网关**：WBI、共享逐 HTTP 请求限流、短时资料缓存、在途合并、共同关注与受限 UP 视角回退；未知资料不伪装成未关注。
- **SQL 配置**：首次完整迁移旧有效配置，随后以 SQL 为准；版本 CAS、历史、导入导出和追加式回滚。
- **可靠审批**：先写 pending 再执行动作，未知只隔离相关 QQ/UID/请求；精确证据对账和管理员确认。
- **申请补审**：启动、OneBot 重连或 QQ 恢复在线后自检 SnowLuma 未处理入群申请，健康运行期间每分钟补扫，按当前群规则审核并持久化去重；保留 NapCat 兼容和迁移去重。
- **凭证**：手动 Cookie、CookieCloud、管理员扫码登录、原生刷新、指纹与状态监控；会话更新防止并发覆盖。
- **可选所有权与巡检**：短效挑战/H5、成员复查、提醒/移除及退群绑定策略。启用和真实外部能力限制见[专题说明](docs/ownership-and-patrol.md)。
- **管理与运维**：群策略、限制名单、审计、绑定、退群、pending、凭证健康、队列和本地日志/磁盘告警。

账号绑定不是所有权证明；粉丝牌不等于当前舰长；扫码或社区接口实现不等于已经在你的账号上验证可用。项目不承诺绕过隐私、永不风控、绝不封号或固定核验时延。

## 架构

```text
app/main.py + app/runtime.py        单一 FastAPI 生命周期
├── onebot/                         正向/反向 WS、echo、事件队列
├── service.py + commands.py        审核、模拟、权限、通知
├── config.py + database/           SQL v4 配置、历史、业务事务
├── gatekeeper/                     局部 pending 与精确对账
├── bilibili/                       资料、限流缓存、凭证
├── ownership.py + patrol.py        可选所有权与入群后巡检
└── web/                            管理 API/UI、H5
src/gatekeeper/                     纯 Python 模型、解析、AST、T/F/I、管道
app/nonebot_adapter.py              可选 NoneBot 事件/动作桥
src/plugins/                       归档旧实现与回归材料，不用于 v4 数据
```

旧版本只抽离业务核心；V3 将默认入口、生命周期和外部适配也迁入 `app/`。当前边界见[架构文档](docs/architecture.md)与[ADR 0003](docs/adr/0003-standalone-runtime-sql-config.md)。

## 安装与启动

需要受支持的 Python（以 `pyproject.toml` 为准）和 `uv`。先按 [SnowLuma 接入说明](docs/snowluma-migration.md)安装并配置 OneBot v11；已有 NapCat 部署可继续使用其兼容接入。

```bash
git clone https://github.com/wuhutakeoffyoo/bilibili-qq-group-gatekeeper.git
cd bilibili-qq-group-gatekeeper
uv sync --locked --no-dev
cp .env.example .env
cp .env.prod.example .env.prod
chmod 600 .env.prod
```

Windows 使用 `Copy-Item .env.example .env` 和 `Copy-Item .env.prod.example .env.prod`。编辑私有 `.env.prod`，填入真实地址、超级管理员和令牌。`.env` 的 `ENVIRONMENT=prod` 选择 `.env.prod`；进程环境拥有最高优先级。

最小基础设施示意：

```dotenv
SUPERUSERS=["123456789"]
ONEBOT_WS_URLS=["ws://127.0.0.1:3001/"]
ONEBOT_ACCESS_TOKEN=replace-with-your-private-token
HOST=127.0.0.1
PORT=1128
WEBUI_ENABLED=true
WEBUI_TOKEN=replace-with-a-long-random-private-token
```

`ONEBOT_WS_URLS` 填实际适配器的正向 WS 地址；SnowLuma 示例使用 `/`，保留自定义路径时须与其服务端 `path` 一致。使用反向 WS 时可置空列表，将 SnowLuma 或 NapCat 的客户端地址设为 `ws://应用地址:1128/onebot/v11/ws`，配相同令牌并携带账号 ID。外网访问需自行配置 HTTPS/WSS 反向代理及访问限制。

启动默认独立服务：

```bash
uv run --locked --no-dev python bot.py
# 等价入口：uv run --locked --no-dev python -m app.main
```

`GET /healthz` 仅反映进程启动及 OneBot 连接数量，不证明 B站凭证有效或审核动作成功。受支持部署为单进程、一个 Uvicorn worker；不要并行启动多个审核实例。

后台每 30 秒检查 QQ 在线状态；首次就绪及连接恢复后扫描 `get_group_system_msg(count=100)`，持续健康时也在上次成功扫描至少 60 秒后补扫，防止漏推送。根据实现身份解析 SnowLuma 数组或 NapCat 列表，仅处理已启用群；列表窗口有限，详见[审核恢复规则](docs/review-requirements.md#启动与重连补审)。SnowLuma v2.2.0 原版存在申请转换缺陷，新部署须先按[核查记录](docs/snowluma-request-incident-20261030.md)准备已修复的私有镜像，不能仅凭连接健康验收。

### 首次业务配置

首次可导入已有有效环境/JSON/群 YAML/管道，具体优先级和嵌套路径见[配置迁移](docs/configuration.md#首次导入的实际优先级)。没有旧配置时，通过控制台创建真实群和内嵌审核阶段，并提供 B站凭证；仓库占位群默认关闭。

SQL 当前修订建立后，修改旧 `groups.yaml`、`review_pipeline.yaml`、`plugin_state.json` 或 `GROUP_1_*` 不会更新业务规则。以后通过控制台/配置 API/授权命令保存，或导入标准 V3 YAML。默认导出会脱敏，不能用作跨机器凭证恢复文件。

WebUI、反向 WS 与 H5 共用一个 FastAPI 服务，不再另外启动旧 WebUI HTTP 线程。配置与令牌本地恢复见[管理控制台](docs/admin-console.md)，监听与部署见[部署文档](docs/deploy-systemd.md)。

生产目录统一入口、当前版本辨认、SnowLuma 运维、QQ 登录数据挂载及历史 NapCat 回滚副本见[生产运维](docs/operations-layout.md)。

QQ 接入推荐 SnowLuma；已有 NapCat 的配置、登录数据、群申请标识差异及切换验收见[SnowLuma 迁移](docs/snowluma-migration.md)。

SnowLuma 官方 Docker 在 Xvfb 虚拟显示环境中后台运行原生 QQ，noVNC 仅用于登录和排障，关闭浏览器不影响 bot。当前生产只读烟测与全量数据核对已通过；**重启后免登录仍未验收**，不能仅凭勾选“自动登录”或配置持久化卷判定恢复完成。

## 规则示例

```yaml
when: "bili_account && no_conflict && bili_level >= 2 && (follow || medal_level >= 5)"
on_true: [approve]
on_false: [reject]
on_unknown: [ignore, notify_admin]
```

等级比较读取原始数值；看不到资料为 I。裸 `medal` 使用群门槛；`medal >= 5` 与 `medal_level >= 5` 都比较完整目标集合的最高等级。关注/粉丝牌各有多个目标时满足任一位即可。完整语法、输入清洗、误填 QQ 与路由见[审核规则](docs/review-requirements.md)。

## 私聊管理命令

只响应配置的管理员私聊，群聊和未授权用户不回复。英文大小写不敏感；昵称含空格时加引号。

| 命令 | 权限与用途 |
| --- | --- |
| `/help`、`/帮助` | 管理员；查看用法 |
| `/查看配置 [群号]` | 管理员；查看当前配置 |
| `/检查qq <QQ号>` | 管理员；统计、绑定和最近记录 |
| `/检查bili <群号> <UID或昵称>` | 管理员；只读模拟，不发审批、不写申请/绑定/退群 |
| `/解除绑定 <QQ号>` | 管理员；解绑，保留历史审计 |
| `/设置群 <群号> [参数 值 ...]` | 超级管理员；将指定字段写入 SQL |
| `/设置cookie <Cookie>` | 超级管理员；替换凭证，清除旧刷新配对 |
| `/获取cookie` | 超级管理员；可选 CookieCloud 同步 |
| `/登录二维码` | 超级管理员；管理员凭证扫码登录 |
| `/移除退群记录 <QQ号>` | 超级管理员；删除该 QQ 各群退群记录 |
| `/待确认审批` | 超级管理员；查看未知动作 |
| `/确认审批 <审计ID> <applied\|failed>` | 超级管理员；核对真实结果后恢复记录 |

`/设置群` 参数：`enable`、`required_qq_level`、`required_bili_level`、`target_uids`、`target_medal_uids`、`required_medal_level`、`reject_reason_with_leave_time`、`reject_reasons`、`custom_reject_uids`。例如：

```text
/设置群 123456789 required_qq_level 4 required_bili_level 2
/设置群 123456789 reject_reasons '{"qq_level":"本群设有等级门槛"}'
/设置群 123456789 custom_reject_uids '{"12345":"请联系管理员核实"}'
```

`custom_reject_uids` 整项替换本群名单，传入 `'{}'` 可清空；各参数全部校验成功后才保存。内嵌管道、所有权和巡检等完整策略通过管理界面/版本化配置维护。只创建群号不会生成审核管道。`/检查bili` 未提供 QQ，QQ 等级、退群和绑定条件为 I，不代表真实申请会得到相同结果。

## 凭证与运行风险

手动粘贴、CookieCloud 和管理员扫码是不同凭证来源；刷新流程需要合法配对 token。默认公开快照、健康输出和导出隐藏秘密。详情见[凭证管理](docs/credentials.md)与[邮件告警](docs/cookie-email-alert.md)。

程序可能在启动或目标修改时尝试让 Bot 登录的 B站账号关注目标主播；这是外部写操作。通用只读烟测不应调用该操作、刷新确认、真实审批、评论删除或踢人。上线测试应分别说明模拟动作、只读接口和实际受控动作的证据。

## 开发与验证

```bash
uv sync --locked
uv run --locked ruff check .
uv run --locked python -m unittest discover -s tests -p 'test_v3*.py'
uv sync --locked --extra legacy
uv run --locked --extra legacy python -m unittest discover -s tests
```

`legacy` 额外依赖用于兼容桥与旧适配器回归；生产默认安装无需该 extra。默认包导入测试会拒绝任何 NoneBot 导入。测试范围、已知协议限制和尚未实测的外部能力见[功能状态](docs/development-roadmap.md)。不要把测试数量、fixture 或健康端口当作生产验收。

## 旧 NoneBot 环境

可选 `legacy` 安装 NoneBot 兼容依赖，由事件桥调用同一个 V3 Runtime/SQL 服务；不是默认启动的前置条件。`src/plugins/` 还保留旧回归资料，但原 schema3 程序不能读取 v4 库。不能让兼容入口与独立入口同时收同一账号事件。升级及软件回退遵循[迁移文档](docs/deploy-systemd.md)。

确需接入 NoneBot 环境时，显式启动共享服务的兼容桥：

```bash
uv sync --locked --extra legacy
uv run --locked --extra legacy python legacy_bot.py
```

该入口只加载 `app.nonebot_adapter` 进行事件/动作转换；不要额外加载归档的旧插件目录。默认 `bot.py` 仍独立运行。

## 许可证与参与

[MIT](LICENSE)。问题与建议请通过 [GitHub Issues](https://github.com/wuhutakeoffyoo/bilibili-qq-group-gatekeeper/issues)，开发约定见[贡献指南](CONTRIBUTING.md)。
