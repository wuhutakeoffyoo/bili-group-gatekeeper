# V3 配置、历史与迁移

默认运行时使用 `app/config.py`。群规则、管理员配置、B站凭证及编译后的审核阶段保存在 `data/records/gatekeeper.sqlite3`，数据库架构为 v4。首次导入完成后，旧 YAML、JSON 和 `GROUP_*` 环境变量不再参与业务配置合并。

## 基础设施与业务配置

| 类型 | 生效来源 | 修改方法 |
| --- | --- | --- |
| 环境选择、OneBot 地址/令牌、超级管理员、ASGI 监听、日志 | `.env` → `.env.<ENVIRONMENT>` → 进程环境；后者优先 | 修改私有环境文件后重启 |
| 群开关、等级、目标 UID、限制名单、拒绝补充、审核阶段、所有权和巡检策略 | SQLite 当前配置修订 | 管理控制台、配置 API 或授权私聊命令 |
| B站 Cookie、refresh token、凭证 generation、刷新确认状态 | SQLite 当前配置修订 | 凭证管理功能或授权命令；公开快照隐藏秘密 |
| SMTP、CookieCloud、Cookie 检查周期 | 环境仅首次导入，后续读 SQL 当前修订 | 版本化配置编辑；秘密脱敏 |
| 审计、QQ/UID 绑定、退群记录 | SQLite 业务表 | 审核流程及相应管理功能 |
| pending 精确动作、所有权挑战、巡检元数据 | SQLite `runtime_state` | 对应服务以事务更新 |

基础设施文件仍需备份。数据库配置统一不意味着进程不再需要 OneBot 连接。WebUI 开关/令牌/安全字段首次可由环境初始化，之后属 SQL 且不允许浏览器修改；本地恢复见[管理台](admin-console.md)。

## 首次导入的实际优先级

只在 SQL 没有当前配置修订时执行；已有 SQL 修订则直接加载 SQL。

| 状态 | 业务配置优先级（从低到高） |
| --- | --- |
| 首次导入 | 运行时 JSON → 群 YAML → 群字段 overrides → `GROUP_CONFIGS` → `GROUP_ID`/`GROUP_*` → 连续编号 `GROUP_N_*` |
| 导入后及后续启动 | SQLite 当前修订；旧文件和群环境变量仅保留为迁移材料 |

全局环境字段与群字段分别合并，具体顺序如下。启动日志会汇总实际加载的 SQLite 修订号及各群 `enabled` 值；旧来源的原始内容和散列保存在首次导入清单及私有备份中。旧格式已退出日常运行时合并，仅保留首次导入兼容。

1. `data/runtime/plugin_state.json`；不存在时读 `data/config/plugin_config.json`。
2. 非空的全局环境设置：Cookie、管理员、通知模式和 WebUI 初始值等。
3. `GROUP_CONFIG_FILE` 指向的群 YAML；每群先应用 `defaults`，再应用自身字段，整群替换此前同群对象。
4. `data/runtime/group_overrides.json`，按字段覆盖。
5. 旧 `GROUP_CONFIGS` JSON，按群整项替换。
6. `GROUP_ID` 与对应 `GROUP_*` 字段。
7. 连续编号的 `GROUP_1_*`、`GROUP_2_*` 等；遇第一个缺失编号停止。
8. 按最终 `review_pipeline_file` 读取规则，将编译后的阶段和原始 `when` 内嵌到 SQL 快照。

群 YAML 中的相对 `review_pipeline_file` 以该 YAML 所在目录为基准；环境中的相对路径以应用根目录为基准。首次导入保存实际来源清单、文件散列和原始字节备份。源文件保留；主运行配置损坏会停止首次迁移，不能无声生成空配置。

例如，`GROUP_CONFIG_FILE=data/config/groups.local.yaml` 配合 `review_pipeline_file: rules/review.yaml`，首次读取的是 `data/config/rules/review.yaml`。导入成功后即使修改或移走该文件，当前 SQL 规则仍保持不变。

## 版本与并发编辑

- 每次替换、命令修改、导入、凭证更新都会追加修订。
- 保存必须携带读取时的版本号；版本冲突应刷新并重新编辑，不能强行覆盖。
- 回滚会生成一个新修订，并记录 `restored_from`；历史修订不被修改或删除。仅恢复业务配置，保留当前 B站 Cookie/token/generation/刷新待确认状态，避免复活历史会话。
- `/设置群` 只修改本次指定字段，保留其他群字段和凭证；实际写入 SQLite。
- `/设置群 <群号> custom_reject_uids '{"12345":"请联系管理员核实"}'` 整项替换指定群限制名单，`'{}'` 清空；UID 或 JSON 无效时整条命令不保存。
- 退群记录只保存已配置群的普通成员，忽略 Bot 自身离群。群审核临时关闭时仍保留成员退群记录，供重新开启后使用。
- `review_pipeline_file` 仅作为首次迁移来源说明，日常修改以 SQL 内嵌阶段为准。
- `ConfigManager.get_snapshot()` 返回同一次读取的版本及配置。内部需要原始凭证时必须显式请求 `include_secrets=True`；不能把两次独立的配置读取和版本读取配对保存。

## 标准 YAML 导入导出

SQL 配置备份格式是 `version: 3` 加 `config` 对象，区别于首次迁移使用的群 YAML 和 `version: 2` 管道 YAML。

```yaml
version: 3
config:
  groups:
    "123456789":
      group_id: "123456789"
      enabled: true
      target_uids: [12345]
      review_stages:
        - index: 1
          name: 基础审核
          when: "bili_account && no_conflict && bili_level >= 2 && follow"
          on_true: [approve]
          on_false: [reject]
          on_unknown: [ignore]
  admin_qqs: []
  notify_admins_on_abnormal_request: superusers_only
```

此例省略的全局字段会采用默认值。导入整份配置前，应先导出当前完整配置再编辑，避免用局部示例替换现有设置。导入会校验群号、等级、表达式和路由，不会执行 QQ 审批，也不删除业务记录。

默认导出将 Cookie、令牌、密码和刷新中间状态替换为 `[redacted]`。将该脱敏文件导回同一数据库时，占位符保留当前秘密。迁移到空数据库时，它不能恢复凭证，须单独提供。确需包含秘密的内部备份应显式导出并保存为仅服务账号可读的文件，不得提交 Git 或发到公开日志。

## Cookie 与会话更新

首次导入时，非空环境 `BILI_COOKIE` 覆盖旧运行时值；SQL 已建立后不再回退到旧环境 Cookie。因此手动更换 Cookie 后重启不会被旧 `BILI_COOKIE` 覆盖，主动清空 SQL Cookie 也不会从环境复活。

Cookie-only 手动或 CookieCloud 更新可能更换账号，会同时清除旧 refresh token 与旧刷新确认状态。显式 `set_credentials` 可保存 Cookie/token 配对并递增 generation；任何一般配置替换改变凭证时也必须推进 generation，不能导入旧刷新确认任务。普通业务编辑保留当前刷新恢复状态。原生刷新配对使用版本与 generation 检查防止覆盖并发修改。凭证行为见[凭证管理](credentials.md)。

## 备份、升级与回退

应保留完整 `data/`、全部实际环境文件、外置 CSS/首次迁移 YAML、systemd 单元及 drop-in、`EnvironmentFile=` 文件。在线数据库备份必须使用 SQLite 一致性备份接口；不能只复制主文件而遗漏 WAL。

v3 数据库升级到 v4 前会生成私有 SQLite 备份；首次业务配置导入另保存来源备份。数据库与秘密文件为 `0600`，备份目录为 `0700`（POSIX）。原有业务列和行不因 v4 配置迁移被重写。

升级时先在副本核对有效配置和业务行；切换前停止旧进程，用冻结后的最新数据再次复制并校验。v3 旧代码拒绝打开 v4 数据库。软件回退需专门核对 schema 兼容性，配置版本回滚只适用于当前 V3 程序，二者不是同一操作。详见[部署与升级](deploy-systemd.md)。
