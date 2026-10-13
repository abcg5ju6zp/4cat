# 按集成划分的作用域访问令牌（Scoped Access Tokens）— 产品需求文档

## Overview
- **Summary**: 将 4CAT 当前"每用户一个长期全权限令牌"的 API 认证模型，扩展为"每集成一个命名令牌系列（token series）"，每个系列可声明独立的操作范围（scopes）、可访问数据集边界（dataset boundary）和到期时间；支持同系列代际轮换（generation rotation）与可选重叠期；密钥仅在创建/轮换成功时显示一次，数据库只存哈希。
- **Purpose**: 研究团队把多个采集、处理、导出脚本接入 4CAT 时不再共享同一密钥；单个集成的轮换、停用或到期不会影响其他脚本；令牌权限在签发时被冻结，不随后续账户变化自动扩权。
- **Target Users**: 通过 4CAT Web API 运行自动化脚本的注册用户（需要 `privileges.can_create_api_token`）；管理该实例的管理员。

## Goals
- 一个用户可创建多个彼此独立的命名令牌，各自携带范围、数据集边界、到期时间。
- 签发能力不得超过该用户当时拥有的权限；请求仅按令牌保存的授权快照判定，不自动扩权。
- 同一系列内可轮换出新代际，用户可选短暂重叠期；旧代际在重叠结束、到期、显式撤销、账户停用或权限来源移除后失效。
- 撤销/轮换一个系列不影响同一用户的其他系列。
- 并发轮换、重复提交、签发响应丢失、前后端进程重启都收敛到同一持久代际，不多产生可用密钥。
- 列表完整展示名称、范围、数据集边界、到期、系列关系、最后使用时间与当前状态，且不泄露可认证值。
- 完全兼容现有遗留令牌、三种认证入口和"删除用户即撤销全部令牌"的语义。

## Non-Goals
- 不提供脚本化（JSON API）的令牌管理接口；管理操作仅在网页会话中完成。
- 不改造浏览器会话登录、Flask-Login 密码登录、注册/重置密码令牌（`register_token`）机制。
- 不对遗留 `access_tokens` 表做哈希迁移或语义变更（见"约束与决策"）。
- 不做 OAuth2、不做令牌级审计日志导出、不做每请求细粒度授权策略 DSL。
- 不改造 HTML 页面（非 API）的访问控制；作用域与边界仅约束 API 端点。

## Background & Context
- 现状表结构（[backend/database.sql](file:///Users/ding/Documents/swe/0910125/project-09/backend/database.sql) 第 149-155 行）：`access_tokens(name TEXT UNIQUE PRIMARY KEY, token TEXT, calls INTEGER, expires INTEGER)`，`name` 即用户名，每用户至多一个有效令牌，`token` 为明文。
- 现状签发：[api_tool.py](file:///Users/ding/Documents/swe/0910125/project-09/webtool/views/api_tool.py#L1396-L1442) 的 `GET /api/request-token/`，令牌有效期固定 365 天，返回完整明文令牌。
- 现状认证：[views_user.py](file:///Users/ding/Documents/swe/0910125/project-09/webtool/views/views_user.py#L45-L78) 的 `request_loader` 依次读取 `?access-token=`、`Authentication` 头、`Authorization` 头（后者历史误名，保留兼容），命中后按用户名装配完整实时权限的 `User`。
- 现状授权：API 端点通过 `@login_required` + `@setting_required("privileges.xxx")` 与视图内 `g.config.get(...)`、`User.can_access_dataset()` / `DataSet.is_accessible_by()` 判定；`is_admin` 来自用户 `tags` 中的 `admin`。
- 现状页面：[access-tokens.html](file:///Users/ding/Documents/swe/0910125/project-09/webtool/templates/access-tokens.html) 直接明文列出令牌；路由 `user.show_access_tokens`，导航项 "API Access"。
- 删除用户：[common/lib/user.py](file:///Users/ding/Documents/swe/0910125/project-09/common/lib/user.py#L531-L566) 的 `User.delete()` 已 `DELETE FROM access_tokens WHERE name=%s`。
- 迁移机制：VERSION 现为 1.57；schema 变更需在 `helper-scripts/migrate/` 增加 `migrate-{old}-{new}.py`、同步 `backend/database.sql` 并按 AGENTS.md 提升 VERSION；`tests/test_schema_consistency.py` 校验二者一致（迁移中 `CREATE TABLE` 的列自动被认可）。
- 无 CSRF 框架，表单 POST 为现有普遍风格；`setting_required` 失败返回登录管理器的 unauthorized。

## 约束与关键决策
- **遗留令牌（用户已确认）**：保留旧 `access_tokens` 表与旧端点完全原样工作（明文存储、365 天、实时权限、`calls` 计数）；新体系使用独立新表。列表页对遗留令牌改为打码展示（不再明文回显），并提供删除（撤销）按钮。存量明文属于旧机制既有数据，新代码不再产生任何明文持久化。
- **管理入口（用户已确认）**：仅网页会话（cookie 登录）可创建/轮换/撤销；任何令牌（无论新旧）都不能通过 API 创建或轮换令牌。
- **到期（用户已确认）**：创建时从预设（30/90/180/365 天）或自定义日期选择，默认 365 天；允许显式选择"永不过期"（`expires=0`）。到期属于系列，轮换不延长到期。
- **范围粒度（用户已确认）**：四个粗粒度 scope：`dataset:read`、`dataset:create`、`dataset:process`、`dataset:manage`（定义见 FR-4）。
- **认证值形态**：新令牌呈现形式为 `{token_id}.{secret}`（token_id 为系列公开随机标识，secret 为高熵随机十六进制）；库内仅存 `sha256(secret)` 与系列元数据。遗留令牌为 64 位十六进制无点号，据此分流。
- **权限来源**：4CAT 用户权限由全局/标签（tags）配置解析得到。签发时快照 = 该用户当时解析出的全部 `privileges.*` 值 + `is_admin` + 签发时标签集合。请求时：标签集合必须仍是当前用户标签的子集（权限来源未被移除），账户未停用；授权值只取快照。
- **全局配置变更**：管理员事后修改全局/标签权限不会改变旧令牌快照（这是"不自动扩权"的另一面）；管理员的止血手段是撤销该系列。此行为在页面文案中说明。
- **技术约束**：Python ≥ 3.11、Tab 缩进、不新增第三方依赖；Postgres 表与 JSONB；遵循既有视图/模板风格。

## Functional Requirements

### FR-1：命名令牌系列
- 用户在 Access tokens 页面可创建新令牌：必填名称（同一用户下唯一，1-100 字符）、一个或多个 scope、数据集边界模式、到期选择。
- 每个系列在库中具有随机 `token_id`（公开标识，非密钥），同一用户可拥有任意数量系列。

### FR-2：数据集边界
- 创建时三选一：
  - `all`：该用户按快照权限本可访问的全部数据集（默认）；
  - `owned`：仅限该用户以 `owner` 角色拥有的数据集；
  - `listed`：仅限创建时显式给出的数据集 key 白名单；每个 key 必须存在且创建时该用户至少可查看，否则整单拒绝。
- 所有解析数据集 key 的 API 端点都必须额外通过边界检查；`owned`/`listed` 不得因快照中保留了 `can_view_private_datasets`/管理员类权限而绕过。
- `listed` 白名单中的 key，请求时仍要求该用户当下对数据集有任意角色的访问权（所有者事后移除授权即不可再访问）。

### FR-3：签发时能力封顶
- 各 scope 的前置权限（签发时必须为真，否则禁止勾选/提交该 scope）：
  - `dataset:read`：无额外前置；
  - `dataset:create`：`privileges.can_create_dataset`；
  - `dataset:process`：`privileges.can_run_processors`；
  - `dataset:manage`：无单一前置（删除/共享以所有权判定，管理类端点再由快照中的具体权限/管理员身份封顶）。
- 服务端必须重新校验前置权限，不能只依赖前端禁用复选框。
- 白名单数据集必须在创建时通过"该用户当前可访问"校验。
- 特殊用户 `anonymous`/`autologin` 不得创建新令牌（沿用旧限制）。

### FR-4：操作范围（scopes）与端点映射
- 新令牌携带非空 scope 集合；映射集中维护、单点执行（API 蓝图 `before_request`）：
  - `dataset:read`：`version.json`、`status.json`、`available-processors`、`processor-options`、`check-processors`、`dataset/<key>/items`、`export-packed-dataset`、`toggle-dataset-favourite`；
  - `dataset:create`：`datasource-form`、`import-dataset`、`queue-query`、`check-query`、`check-search-queue`；
  - `dataset:process`：`queue-processor`、`convert-dataset`、`edit-dataset-label`、`nuke-query`、standalone `process/<processor>/`；
  - `dataset:manage`：`delete-dataset`、`add-dataset-owner`、`remove-dataset-owner`、`toggle-dataset-private`、`erase-credentials`、`remove-tag`。
- 失败关闭（fail-closed）：新令牌调用任何需要登录但不在其 scope 集合映射内的 API 端点一律 403；这同时意味着新令牌不能调用 `/api/request-token/`。
- scope 门在既有 `@login_required`/`@setting_required`/所有权检查之外叠加，永不放宽它们；快照权限不足时仍由既有机制拒绝。
- 不需要登录的公共端点（如 openapi 文档、`/api/datasources/`）不受 scope 影响。

### FR-5：授权快照
- 创建成功时持久化：当时解析的全部 `privileges.*` 键值、`is_admin` 布尔值、用户标签集合。
- 该令牌认证的请求，其 `privileges.*` 配置读取与 `is_admin` 判定只返回快照值；用户事后获得新标签/管理员身份不得使旧令牌扩权。
- 每次请求还必须实时满足：用户存在、未停用、快照标签集合 ⊆ 当前用户标签；任一不满足则拒绝认证（401/未认证语义，等同令牌无效）。
- 遗留令牌不使用快照，继续实时权限（原语义）。

### FR-6：密钥仅显示一次 + 仅哈希持久化
- 创建或轮换成功后，完整 `{token_id}.{secret}` 仅在该次 POST 的成功响应页面展示一次；页面提示关闭后无法找回。
- 数据库仅存 secret 的 SHA-256 哈希；任何列表、错误信息、日志、重复提交响应都不得回显可直接认证的值。
- 重复提交/响应丢失重试：返回同一既有代际的元数据并明确提示"密钥仅展示过一次"，绝不再生成或展示新密钥。

### FR-7：代际轮换与重叠期
- 轮换在同一系列内创建 `generation + 1`，系列的名称、scope、边界、到期不变。
- 用户可选择重叠期：0（默认）、1 小时、24 小时、7 天（上限常量 30 天）；新代际创建后立即可用。
- 旧代际在以下任一条件成立时必须拒绝：重叠期已过（当前代际始终不受重叠限制）、系列到期、该代际或系列被显式撤销、账户停用、快照标签来源缺失。
- 轮换不重置系列到期时间；到期后所有代际失效。

### FR-8：撤销隔离
- 撤销一个系列即作废其全部代际，不触碰该用户其他系列或遗留令牌。
- 列表为每个系列提供独立撤销操作；撤销后状态可见且不可用该系列任何代际认证。

### FR-9：并发与恢复收敛
- 创建与轮换表单携带一次性随机 nonce；服务端以持久化的 nonce 唯一约束 + 系列行级事务为收敛锚点。
- 同 nonce 的并发/重复请求（含前端重复提交、响应丢失后重试、前后端进程重启后重试）要么返回同一代际（不返回明文），要么创建恰好一个代际；任何情况下不得产生两个可用新密钥。
- 不同 nonce 的先后两次轮换属于两次合法轮换，各自产生新代际。

### FR-10：令牌列表
- Access tokens 页面展示每个系列：名称、scope 集合、数据集边界（含白名单 key 数量/内容）、到期时间（或"永不过期"）、系列当前代际与处于重叠期的旧代际（系列关系）、最后使用时间、当前状态。
- 状态至少包括：active（当前代际可用）、overlap（有旧代际处于重叠期）、expired、revoked。
- 同页以独立分区展示遗留令牌：打码值、calls、到期、状态，可删除（撤销）。
- 所有行不出现完整可认证值；新系列只展示 token_id。

### FR-11：兼容性
- 三种认证入口（`?access-token=` 查询参数、`Authentication` 头、`Authorization` 头）对新旧令牌均保持可用。
- `GET /api/request-token/`（会话登录、具备 `privileges.can_create_api_token`）行为对遗留令牌不变：存在有效遗留令牌则返回它，否则创建一个 365 天的新遗留令牌；`forward` 参数仍重定向回列表页。
- 遗留令牌继续按实时权限、原到期语义工作；不被新 scope/快照/边界逻辑约束。
- 删除用户时，其遗留令牌与所有新系列/代际一并删除（撤销）。

### FR-12：数据库与发布
- 新表与列同时出现在 `backend/database.sql`（全新安装）与版本迁移脚本（存量安装）；VERSION 提升且编号与迁移匹配；迁移可重复安全执行（`IF NOT EXISTS`/存在性检查）。

## Non-Functional Requirements
- **NFR-1 安全**：密钥比较基于哈希常量时间无关的等值查询（高熵随机值，SHA-256 足够）；SQL 全部参数化；管理端点要求会话登录且禁止令牌认证；不新增密钥泄露面（日志/错误/模板）。
- **NFR-2 可靠性**：认证热路径每次请求只增加少量索引查询；最后使用时间/调用计数更新不得显著拖慢请求或在失败时影响认证判定。
- **NFR-3 可测试性**：令牌生成/解析、代际有效性裁决、scope 映射、快照封顶等核心逻辑以不依赖数据库的纯函数实现，可单元测试。
- **NFR-4 风格一致性**：遵循现有 Flask/Jinja 结构、Tab 缩进、`error()` JSON 错误、现有模板与 CSS 类风格；不引入新依赖。

## Assumptions
- Postgres 是唯一支持的数据库，事务与行级锁、部分唯一索引、JSONB 可用。
- 前端 gunicorn 多 worker 与后端 daemon 可能并发访问同一数据库；收敛只依赖数据库约束/事务，不依赖进程内状态。
- 令牌认证仅用于 API；HTML 页面即使带令牌访问也不授予超出会话的内容（scope/边界只在 API 蓝图执行）。
- 管理员事后调整全局/标签权限不回溯影响快照令牌，系预期行为；需要立即止血时撤销系列。

## Acceptance Criteria

### AC-1：可创建多命名、多范围、带边界与到期的令牌
- **Type**: `rule`
- **Given**: 具备 `privileges.can_create_api_token` 的普通用户登录会话
- **When**: 在 Access tokens 页以名称"采集脚本A"、勾选 `dataset:create`、边界 `owned`、到期 90 天提交创建
- **Then**: 生成一个新系列，页面仅一次展示完整 `{token_id}.{secret}`，库中存在系列行（name=该用户、label、scopes 含 dataset:create、dataset_access=owned、expires≈now+90d）与第 1 代哈希行，且任何表中都无 secret 明文
- **Pass Condition**: 连续为同一用户创建两个不同名称的系列成功且各自得到不同令牌；数据库与页面检查符合上述全部断言
- **Evidence**: 新建后查询 `access_token_series`/`access_token_generations` 的行内容；响应 HTML 仅出现一次完整密钥；grep 库内数据无明文 secret

### AC-2：签发能力不超过当时权限
- **Type**: `rule`
- **Given**: 不具备 `privileges.can_run_processors` 的用户
- **When**: 尝试（直接构造 POST）创建含 `dataset:process` 的令牌
- **Then**: 服务端拒绝（400/403），不产生任何系列或代际
- **Pass Condition**: 绕过前端直接 POST 仍被拒；DB 无新行
- **Evidence**: 视图单元/手动 POST 响应码与 DB 计数

### AC-3：请求按授权快照裁决，事后提权不扩权
- **Type**: `rule`
- **Given**: 用户以非管理员身份创建含 `dataset:read` 的令牌 T
- **When**: 之后为该用户添加 `admin` 标签，再用 T 请求需要管理员/更高权限的 API
- **Then**: 请求不获得任何新增权限（按快照被拒绝），且其普通 read 请求仍正常
- **Pass Condition**: 加 admin 标签前后，T 对越权端点均返回 403，对允许端点返回成功
- **Evidence**: 带令牌请求的响应码对比；快照行内容与 `is_admin` 冻结值

### AC-4：权限来源移除或账户停用即拒绝
- **Type**: `rule`
- **Given**: 用户通过某标签获得权限并据此创建有效令牌 T
- **When**: 从该用户移除该标签，或将账户标记停用
- **Then**: T 的后续请求认证失败（401/403），即使系列未到期、未撤销
- **Pass Condition**: 移除标签/停用后用 T 请求返回未授权/禁止；恢复标签后？（标签恢复不自动恢复——按严格语义仍拒绝，因为来源曾被移除：实现以"快照标签必须为当前子集"判定，恢复子集后可再次通过；此以子集判定为准并在验收中固定）
- **Evidence**: 认证请求响应码与服务端裁决函数的输入输出

### AC-5：密钥只显示一次且持久化无明文
- **Type**: `rule`
- **Given**: 已成功创建令牌并看到一次明文
- **When**: 刷新页面、重复提交同一创建表单（同 nonce）、轮换响应丢失后用同 nonce 重试、重启前后端后再试
- **Then**: 任何地方不再出现该明文；同 nonce 重试返回同一系列/同一代际元数据且不生成新代际；列表只显示 token_id
- **Pass Condition**: 四种场景下明文仅出现一次、代际数不增加、nonce 对应的代际编号一致
- **Evidence**: 代际行数与 generation 编号；响应 HTML/JSON 中不含完整密钥；DB 中仅存哈希

### AC-6：轮换产生新代际，重叠期行为正确
- **Type**: `rule`
- **Given**: 有效系列，当前为第 1 代
- **When**: 以重叠期 1 小时轮换，立即用新代际和旧代际分别请求；重叠期过后再用旧代际请求
- **Then**: 新代际立即可用；旧代际在重叠期内可用，重叠期结束后被拒；第 1、2 代同属一个 token_id
- **Pass Condition**: 三个时间点的请求响应码符合预期；DB 中两代共存且旧代 overlap_until 正确
- **Evidence**: 认证请求结果与 generations 行

### AC-7：到期与撤销条件
- **Type**: `rule`
- **Given**: 即将到期的系列与一个正常系列
- **When**: 系列到期后用其任一代际请求；撤销正常系列后用其任一代际请求；再用同用户其他系列请求
- **Then**: 到期系列全部代际被拒；撤销系列全部代际被拒；其他系列不受影响
- **Pass Condition**: 6 个组合请求的响应码全部符合
- **Evidence**: 请求响应码与系列状态字段

### AC-8：并发/重复轮换收敛为单一有效代际
- **Type**: `rule`
- **Given**: 一个系列，多个客户端持同一 nonce 并发轮换；以及同 nonce 在进程重启后重放
- **Then**: 恰好产生一个新代际；其余请求拿到同一代际的元数据（无明文）；无额外可用密钥
- **Pass Condition**: 并发 N 次同 nonce 轮换后该系列只增加 1 代，且库中有效密钥哈希数量符合预期；不同 nonce 再轮换则正常增加 1 代
- **Evidence**: 并发脚本输出与 generations 行数/编号；唯一约束存在

### AC-9：三种认证入口与数据集边界
- **Type**: `rule`
- **Given**: 一个 `owned` 边界令牌与一个 `listed` 边界令牌（白名单仅含 key K1）
- **When**: 分别用查询参数、Authentication 头、Authorization 头携带令牌访问 K1 与无权数据集 K2 的 API
- **Then**: 三种入口都能认证；K1 在边界内可访问，K2 返回 403/404 拒绝
- **Pass Condition**: 3 入口 × 2 数据集的结果矩阵正确
- **Evidence**: HTTP 请求结果

### AC-10：列表信息完整且不泄密
- **Type**: `rule`
- **Given**: 用户拥有不同状态的多个系列和一个遗留令牌
- **When**: 打开 Access tokens 页面
- **Then**: 每个系列显示名称、scopes、边界、到期、token_id 与当前/重叠代际、最后使用时间、状态；遗留令牌分区显示打码值与到期；任何行不出现完整可认证值
- **Pass Condition**: 逐项核对页面字段；HTML 中不存在完整新 secret 或遗留 token 全文
- **Evidence**: 页面 HTML 检查

### AC-11：遗留令牌与 request-token 端点兼容
- **Type**: `rule`
- **Given**: 升级前已存在遗留令牌的用户，以及无遗留令牌的用户
- **When**: 升级后用遗留令牌经三种入口请求 API；登录会话调用 `/api/request-token/`（无/有有效遗留令牌两情形）
- **Then**: 遗留令牌行为与升级前一致（实时权限、原到期、calls 自增）；无有效遗留令牌时端点创建 365 天遗留令牌并返回明文一次，有时返回同一令牌；`forward` 仍重定向
- **Pass Condition**: 全部断言成立，遗留令牌不受 scope/快照/边界约束
- **Evidence**: 迁移前后行为对比、DB 与 HTTP 结果

### AC-12：删除用户撤销全部令牌
- **Type**: `rule`
- **Given**: 用户拥有遗留令牌与多个新系列
- **When**: 管理员删除该用户
- **Then**: 旧表行与该用户全部系列、代际被删除；这些值再用于认证均失败
- **Pass Condition**: 删除后三张表中无该用户数据，认证请求失败
- **Evidence**: DB 查询与认证请求结果

### AC-13：数据库发布物一致
- **Type**: `rule`
- **Given**: 全新数据库与一个 1.57 存量数据库
- **When**: 分别用 database.sql 初始化、对存量执行 1.57→1.58 迁移
- **Then**: 两者都具备新表/索引且不破坏旧表；迁移脚本重复执行不报错；`pytest tests/test_schema_consistency.py` 通过
- **Pass Condition**: 两种初始化路径结构一致；幂等迁移成功；schema 一致性测试通过
- **Evidence**: 迁移脚本输出、psql 结构、pytest 结果

### AC-14：核心逻辑单元测试与代码质量
- **Type**: `rule`
- **Given**: 新增纯逻辑（令牌生成/解析、代际裁决、scope 映射、签发封顶、nonce 收敛）
- **When**: 运行 `pytest` 与 `ruff check .`
- **Then**: 新增/既有测试全部通过，ruff 无新增告警
- **Pass Condition**: 命令退出码 0
- **Evidence**: pytest/ruff 输出

### AC-15：管理界面可用性与风格一致性
- **Type**: `rubric`
- **Dimension**: 界面是否清晰表达"每集成一个令牌、权限可被冻结/缩小、密钥只出现一次"，并与 4CAT 现有视觉/交互风格一致
- **Scale**: 1-5
- **Anchors**: 1 = 表单/列表混乱，用户无法理解 scope/边界/轮换后果或密钥泄露在页面；3 = 功能齐全但文案或布局粗糙、风格游离；5 = 信息架构清晰，创建/轮换/撤销/一次性密钥揭示顺畅，样式复用现有组件，风险提示到位
- **Pass Threshold**: >= 4
- **Evidence**: Access tokens 页面人工走查（创建、轮换重叠、撤销、遗留分区、一次性密钥揭示）

### AC-16：实现与代码库架构的契合度
- **Type**: `rubric`
- **Dimension**: 实现是否最小侵入、复用既有配置/用户/数据集抽象、职责分层清晰
- **Scale**: 1-5
- **Anchors**: 1 = 大改既有认证/配置链路、重复实现权限解析；3 = 可用但耦合生硬；5 = 新逻辑集中于单一辅助模块与数据库层，视图只做薄接入，快照通过 ConfigWrapper 子类无缝接入，遗留路径零改动
- **Pass Threshold**: >= 4
- **Evidence**: 代码走查（模块边界、改动文件清单与行数、与 ConfigWrapper/User/DataSet 的集成方式）

## Open Questions
- 无（四个关键决策已与用户确认：遗留令牌保留原表原样；仅网页管理；到期预设且允许永不过期；四个粗粒度 scope）。
