# Bilibili QQ Group Gatekeeper 交接

更新：2026-11-11。分支、CI、生产状态须接手时重新核对；历史验收不等于当前实测。

## 接手顺序

1. 检查 Git 状态和提交；阅读 [文档索引](docs/README.md)、[领域](CONTEXT.md)和[架构](docs/architecture.md)。
2. 对照 [V3 验收矩阵](docs/v3-implementation.md)与实际模块，避免将旧核心抽离版本当作当前版本。
3. 功能变更同步 [贡献清单](CONTRIBUTING.md)和更新日志；禁止提交凭证、SQLite、运行记录及私有备份。

## 当前边界

| 路径 | 责任 |
| --- | --- |
| `bot.py` / `app/main.py` | 默认独立 FastAPI，统一 OneBot 反向 WS、管理台和 H5，单 worker |
| `app/onebot/` | 正反向 WS、echo、有限事件队列、明确失败与未知结果、SnowLuma/NapCat 精确请求观测和启动补审 |
| `app/service.py` / `commands.py` | 入群、退群、模拟、权限和命令应用服务 |
| `src/gatekeeper/` | 纯模型、输入路由、条件、AST 三值逻辑、流程、格式化和身份锁 |
| `app/gatekeeper/` | 局部 pending、持久化对账与逐条确认 |
| `app/config.py` / `database/` | SQLite v4 配置版本、一次迁移、业务记录及扩展状态 |
| `app/bilibili/` | 单路 UID/昵称查询、限流缓存、凭证、只读归属证据与申请者隔离扫码 |
| `app/ownership.py` / `patrol.py` | 归属验证、一次授权及复查、巡检报告与受限动作 |
| `legacy_bot.py` / `app/nonebot_adapter.py` | 显式安装 legacy extra 后，转换事件/动作并使用同一新服务 |

`src/plugins/group_request_manager/` 是旧行为回归材料。不要用其旧配置/数据库模块打开 v4 数据库，默认入口也不得导入该包。

## 必须保持

- 清洗后的显式 UID 和纯数字只走 UID 接口；其他输入只走昵称搜索。裸数字恰等于申请QQ时本地提示误填，显式 UID 可消歧。输入失败明确要求检查，且不填写昵称/UID以外内容。
- T/F/I 为证据状态；`ignore` 是流程动作。分页缺失、隐私、网络错误不证明不存在。自定义拒绝说明不能遮盖真实事实、要求和下一步。
- 粉丝牌 F 的原因保留检查详情，同时附目标 UID 与最低等级；反馈和审计一致。不可读取的粉丝牌仍为 I。
- v2 数组/goto 兼容；v3 AST 只允许已知条件、安全布尔与数值比较。粉丝牌数值是完整目标集合最大等级；不完整列表不能据低等级作数值否定。
- 先持久化审批意图，再发 OneBot 动作。超时/断线/取消不等于失败；只占用相关 QQ/UID。只有精确仍未处理证据允许一次重发，成员存在或 checked=true 不能核销。
- 只读对账暂时不可用时按持久化预算退避；证据不充分则逐条人工确认，不能冻结所有用户。
- SnowLuma 使用原完整 `slreq` 动作标识，并以原 bot/群/申请序号/QQ 核对旧数字键与收件箱变体。旧 NapCat pending 不会跨适配器自动重发；邀请不能转成主动申请补审。
- 业务配置只在首次导入旧来源；后续 SQL 为准。修改必须版本检查；回滚追加修订并保留当前凭证。旧环境不得覆盖已更新 Cookie 或业务设置。
- 配置修订、迁移来源、动作载荷与数据库备份含秘密，使用私有权限；公开 API/默认导出隐藏凭证。私有动作记录可能保留专属反馈链接。
- 可选归属验证不绕过入群规则。申请者扫码凭证仅在内存验证后丢弃，禁止写入管理员凭证。原申请未处理状态不明确时禁止自动恢复。
- 巡检默认关闭，必须显式 `patrol.when`。使用本群已通过 UID、真实当前成员、最新角色/绑定/策略、宽限期及新证据；Unknown 不独立触发移出。其他群仍存在或审批未确认时保留绑定。

## 运维与验证

当前生产使用 SnowLuma v2.2.0，审核服务唯一入口为 `/opt/bili-group-gatekeeper/current`，适配器目录为 `/opt/snowluma`。2026-10-26 的在线、原有全部接入、补审与全量配置/数据核验见[生产记录](docs/production-acceptance-20261026.md)。旧 NapCat 容器停止且 `restart=no`。读取本机 `CURRENT-DEPLOYMENT.md` 确认实际发布，不能直接启动历史目录。

10 月 30 日已确认原版 SnowLuma 申请转换缺陷并使用固定摘要的私有运行文件/镜像修复，见[核查记录](docs/snowluma-request-incident-20261030.md)。不能把上游 #597 已关闭或旧扫描零候选理解为官方修复/无遗漏。健康连接期间也定期读取申请列表，持久化去重保持原样；未来更换适配器镜像前须验证该修复不会丢失。真实遗漏申请已恢复审核并写入忽略审计，尚未验证 SnowLuma 同意/拒绝新申请的外部回执。

重启免登录尚未验收；当前实例保持运行，临时登录隧道已关闭。QQ 的“自动登录”“记住密码”与持久化卷不能代替实际恢复验证，运行用户已恢复官方默认 UID/GID 1000，QQ 使用官方默认启动参数。这些调整尚未被证明解决登录保存问题，接手者不得写成已修复。后续维护先安排账号持有人可完成登录，再核对实际 QQ 身份、接口和补审日志；只读检查不需要停服务。

默认运行：`uv sync --locked --no-dev` 后 `uv run --locked --no-dev python bot.py`。
默认安装不得出现 NoneBot；可选适配使用 `uv run --locked --extra legacy python legacy_bot.py`。

```bash
uv sync --locked --extra legacy
uv run --locked --extra legacy python -m unittest discover -s tests
uv run --locked --extra legacy python -m compileall -q bot.py legacy_bot.py app src tests
uv run --locked --extra legacy ruff check .
git diff --check
```

CI 另在无 legacy 安装上运行全部 `test_v3_*.py`。本地协议测试、真实临时 SQLite、本地 WS/ASGI 与浏览器测试分别报告；均不能冒充真实新用户申请或永久上游兼容性验收。

部署应先冻结最新生产数据、备份完整来源、在副本导入并逐表逐列核对，再切换单实例。详细操作见[部署与回退](docs/deploy-systemd.md)。迁移后不能让旧程序直接打开 v4 数据库。
