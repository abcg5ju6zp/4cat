# 按集成划分的作用域访问令牌 — 实现计划

## 独立审查后修复（第二轮）
- M1：rotate_series 改为"系列行 FOR UPDATE + 独立语句取号"的真事务（显式 cursor、commit/rollback/close），修复不同 nonce 并发轮换时 CTE 旧快照导致的 UniqueViolation/连接毒化；30 轮×2 线程并发实测 60 代严格递增、零错误、连接可用。
- M2：补齐 check-query（403）、check-processors（边界外 skip）、erase-credentials（403）三处边界；新增静态测试 `test_every_user_keyed_dataset_site_enforces_boundary` 锁死全部 16 个按 key 解析站点。
- M3：listed 边界改 `is_accessible_by(role=None)`，接受 viewer；补 viewer 用例。
- 附带：snapshot 改为枚举 config_definition 全量 privileges（修复默认特权未入库导致快照缺失的真实缺陷）；usage 更新失败先 rollback；create 异类 nonce 冲突转 409；revoke/legacy-delete 补 setting_required；模板过期系列可撤销、去除未用变量。
- 复测：33/33 E2E（含 viewer、三新边界点）、172 pytest、ruff 通过。

## 完成证据汇总（实现阶段）
- Task 1：`pytest tests/test_schema_consistency.py` 3 passed；迁移在本地 Postgres 17 连续执行两次无错；旧 access_tokens 定义未改。
- Task 2：纯逻辑 25 项单测通过；E2E 49 项检查通过（含 UniqueViolation 子类捕获、nonce 收敛）。
- Task 3：E2E 覆盖三入口（Authentication/Authorization/query）、停用、来源标签移除/恢复、快照提权冻结。
- Task 4：单测 `test_all_login_required_api_endpoints_have_scope_mapping` 静态保证映射完整；E2E 验证 403 矩阵与 owned/listed 边界。
- Task 5：E2E 覆盖创建/重放收敛/轮换/零重叠/撤销/遗留兼容/用户删除；非法输入 flash+302 无落库（UI 脚本验证重复名称）。
- Task 6：36/36 UI 渲染检查（空态/active/overlap/expired/revoked/legacy/一次性揭示/no-store）；浏览器代理因与本机端口网络隔离未能运行。
- Task 7：E2E 用户删除后三表无残留、令牌全部失效。
- Task 8：tests/test_access_tokens.py，25 测试。
- Task 9：全量 `pytest` 171 passed；`ruff check .` All checks passed；模板 Jinja 语法解析通过。

## Task 1: 数据库表结构、迁移脚本与版本提升
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 在 `backend/database.sql` 增加 `access_token_series` 与 `access_token_generations` 两表（含外键级联、JSONB、部分唯一索引），保留旧 `access_tokens` 表不动。
  - 新增 `helper-scripts/migrate/migrate-1.57-1.58.py`：幂等创建两表与索引；不触碰旧表与存量数据。
  - `VERSION` 第一行提升为 `1.58`（AGENTS.md 授权）。
  - 表草案：
    - `access_token_series(token_id TEXT PK, name TEXT NOT NULL, label TEXT NOT NULL, scopes JSONB DEFAULT '[]', dataset_access TEXT DEFAULT 'all', dataset_keys JSONB DEFAULT '[]', auth_snapshot JSONB DEFAULT '{}', source_tags JSONB DEFAULT '[]', expires INTEGER DEFAULT 0, revoked BOOLEAN DEFAULT FALSE, timestamp_created INTEGER DEFAULT 0, timestamp_rotated INTEGER DEFAULT 0, UNIQUE(name, label))`，索引 `(name)`。
    - `access_token_generations(token_id TEXT FK ON DELETE CASCADE, generation INTEGER, token_hash TEXT NOT NULL, request_nonce TEXT DEFAULT '', timestamp_created INTEGER DEFAULT 0, overlap_until INTEGER DEFAULT 0, revoked BOOLEAN DEFAULT FALSE, last_used INTEGER DEFAULT 0, calls INTEGER DEFAULT 0, PK(token_id, generation))`；`UNIQUE(token_hash)`；部分唯一索引 `(request_nonce) WHERE request_nonce <> ''`。
- **Acceptance Criteria Addressed**: AC-13
- **Test Requirements**:
  - `rule` TR-1.1: `pytest tests/test_schema_consistency.py` 通过（新表列在迁移 CREATE TABLE 中出现且 database.sql 同步）。
  - `rule` TR-1.2: 迁移脚本在已有两表的库上重复执行不报错（语句均带 IF NOT EXISTS/存在性检查）。
  - `rule` TR-1.3: 旧 `access_tokens` 表定义与存量行不被修改（SQL diff 仅新增对象）。
- **Notes**: 迁移脚本可独立连库执行，参照 migrate-1.55-1.56.py 的样板。

## Task 2: 令牌核心库（纯逻辑 + 数据访问 + 快照配置）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 新建 `webtool/lib/access_tokens.py`，集中承载：
    - 常量：`SCOPES`（id/label/说明/前置权限/端点名集合）、`DATASET_ACCESS_MODES`、到期预设、重叠预设与上限（30 天）、token_id 与 secret 长度。
    - `generate_presented_token()` → `(token_id, secret, presented)`；`hash_secret(secret)`；`parse_presented_token(value)`（区分新令牌与 64 位十六进制遗留令牌）。
    - `build_snapshot(user, config)`：解析当时全部 `privileges.*`、`is_admin`、标签集合。
    - 纯函数 `evaluate_generation(series, generation, user_row, now)`：裁决代际是否可用（系列/代际撤销、到期、当前代际或 overlap_until、停用、source_tags ⊆ 当前 tags），返回 `(bool, reason)`，不碰数据库，便于单测。
    - DB 操作：`create_series(...)`、`rotate_series(...)`（事务：系列行 FOR UPDATE → nonce 命中则收敛返回既有代际；否则插入新代际并把上一代 overlap_until 置为 now+overlap）、`revoke_series(...)`、`get_series_for_user(...)`、`list_series(...)`（含代际聚合与状态推导）、`authenticate_presented_token(db, presented, now)`（连接查询 + 裁决 + 成功时更新 calls/last_used）。
    - nonce 冲突（IntegrityError/唯一索引）一律重查既有代际并以 `secret=None` 收敛，绝不二造。
    - `TokenConfigWrapper(ConfigWrapper)`：`privileges.*` 键返回快照值，其余委托实时配置。
    - `TokenUser(User)`：冻结 `is_admin` 为快照值；携带 `api_token` 上下文（token_id、generation、scopes、boundary）。
    - `token_allows_endpoint(token_ctx, endpoint)` 与 `assert_token_dataset_access(token_ctx, user, dataset)`（all/owned/listed 三模式；listed 还要求当下任意角色可访问）。
  - Tab 缩进、无新依赖。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-4, AC-6, AC-8
- **Test Requirements**:
  - `rule` TR-2.1: `parse_presented_token` 正确解析新形态并把 64 位无点号十六进制识别为遗留形态。
  - `rule` TR-2.2: `evaluate_generation` 对撤销/到期/非当前且超重叠/停用/标签缺失/当前代际/重叠期内等输入组合给出全部正确裁决。
  - `rule` TR-2.3: 签发校验对缺少前置权限的 scope 返回失败且不落库；白名单 key 不可访问时失败。
  - `rule` TR-2.4: TokenConfigWrapper 对快照内 `privileges.*` 返回快照值（含管理员提权场景），对非快照键返回实时值。
  - `rule` TR-2.5: 同 nonce 的 create/rotate 在唯一约束冲突路径下返回同一代际且不产生新行（用 mock DB 或伪 DB 驱动）。
- **Notes**: 纯裁决函数不得 import flask request，保持可测。

## Task 3: 认证装配（request_loader 与每请求配置）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - 改造 `webtool/views/views_user.py` 的 `load_user_from_request`：三种入口顺序不变；先按新形态认证（新表 + 裁决 + TokenUser + TokenConfigWrapper + 上下文），失败再走遗留 SQL（原查询、原 calls 自增、原 User）；新路径也记录 calls/last_used。
  - 改造 `webtool/__init__.py` 每请求 g.config 装配：若 `current_user.api_token` 存在则使用 TokenConfigWrapper，否则维持 ConfigWrapper，确保不被后续 rewrap 覆盖快照。
  - 新令牌认证失败（过期/撤销/停用/来源缺失）返回 None，表现为未认证。
- **Acceptance Criteria Addressed**: AC-3, AC-4, AC-9, AC-11
- **Test Requirements**:
  - `rule` TR-3.1: 三种入口携带同一有效新令牌均认证成功并在 g 上得到快照配置与令牌上下文。
  - `rule` TR-3.2: 过期/撤销/停用/标签移除后的新令牌在三种入口下均为未认证。
  - `rule` TR-3.3: 遗留令牌经三入口行为与改造前一致（同一 SQL 条件、calls +1、实时权限 User）。
  - `rule` TR-3.4: 请求处理期间 `g.config.get("privileges.*")` 对令牌用户返回快照值；事后加 admin 标签不改变该请求与后续请求的冻结 is_admin。

## Task 4: API 范围门与数据集边界执行
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - 在 `api_tool.py` 与 `api_standalone.py` 注册共享的 `before_request`：若当前身份是新令牌（有 api_token 上下文），按 SCOPES 端点名映射校验；未映射但需要登录的端点失败关闭返回 403；公开端点放行。
  - 在所有解析数据集 key 的端点中，DataSet 解析成功后调用 `assert_token_dataset_access`（约：available-processors、processor-options、edit-label、convert、nuke、delete、add/remove-owner、toggle-favourite、toggle-private、queue-processor、export-packed、items），越界返回 403。
  - `/api/request-token/` 对新令牌身份返回 403（失败关闭的映射自然覆盖，另加显式保护）。
  - 不改动端点既有装饰器与所有权逻辑。
- **Acceptance Criteria Addressed**: AC-2（调用面）, AC-9
- **Test Requirements**:
  - `rule` TR-4.1: 仅持 `dataset:read` 的令牌调用 create/process/manage 映射端点全部 403；调用 read 端点成功。
  - `rule` TR-4.2: owned/listed 边界令牌访问边界外数据集返回 403，边界内成功；listed 的 key 被移除所有者身份后再访问被拒。
  - `rule` TR-4.3: 遗留令牌与会话用户访问这些端点不受 scope/边界逻辑影响。
  - `rule` TR-4.4: 每个 `@login_required` 的 API 端点名都在映射表或公开清单中有确定归属（以代码扫描/测试断言，避免漏配导致失败关闭误伤或漏网）。

## Task 5: 令牌管理视图（创建/轮换/撤销，仅会话）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - 在 `webtool/views/views_user.py` 增加 POST 路由（均 `@login_required` + 拒绝令牌身份 + `privileges.can_create_api_token`）：
    - 创建：名称（同用户唯一）、scopes（服务端重校前置权限）、边界模式与白名单 key（存在且当前可访问）、到期（预设/自定义/永不过期，默认 365 天）、表单 nonce；成功后以同页渲染方式一次性展示 presented token。
    - 轮换：仅系列所有者；重叠期（0/1h/24h/7d，默认 0，超 30 天拒绝）；nonce 收敛；成功一次性展示新 presented token。
    - 撤销：仅系列所有者；系列与全部代际置 revoked。
    - 遗留令牌删除：删除旧表本用户行。
  - 更新 `show_access_tokens`：聚合新系列（名称/scope/边界/到期/代际关系/最后使用/状态）+ 遗留行（打码）。
  - 错误输入使用 flash + 重定向或 JSON `error()`，与现有风格一致；任何响应/日志不含完整 secret。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-5, AC-6, AC-7, AC-8, AC-11（forward 不变）
- **Test Requirements**:
  - `rule` TR-5.1: 创建成功路径：系列/代际落库正确，响应 HTML 含且仅含一次完整 presented token，刷新与同 nonce 重放不再出现明文且不增代际。
  - `rule` TR-5.2: 轮换：代际号递增、旧代 overlap_until 正确、同 nonce 并发/重放只产生一个代际；不同 nonce 产生下一代。
  - `rule` TR-5.3: 撤销仅影响目标系列；非所有者操作返回 403/404；令牌身份调用管理路由一律拒绝。
  - `rule` TR-5.4: 各类非法输入（重名、未知 scope、越权 scope、非法到期/重叠、不可访问白名单 key）被拒且无落库。
  - `rule` TR-5.5: `/api/request-token/` 对遗留令牌用户行为不变（无则创建 365 天并返回明文，有有效则返回同一值，forward 重定向）。

## Task 6: Access tokens 页面重写
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 5
- **Description**:
  - 重写 `webtool/templates/access-tokens.html`：创建表单（名称、scope 复选框+说明，按当前权限禁用不可用项，边界单选 + 白名单 key 文本域，到期下拉+自定义日期）、系列表格（要求的全部列与状态徽标、当前/重叠代际）、每系列轮换表单（重叠选择 + nonce）、撤销按钮、一次性密钥揭示面板（含复制按钮与"只显示一次"警示）、遗留令牌分区（打码、到期、删除按钮）、三种认证用法说明保留。
  - 复用现有 CSS 类与页面结构，不引入前端依赖。
- **Acceptance Criteria Addressed**: AC-10, AC-15
- **Test Requirements**:
  - `rule` TR-6.1: 页面包含每个系列全部要求字段与操作控件；任何渲染路径的 HTML 中不出现完整新 secret（揭示面板除外的一次性渲染）或遗留 token 全文。
  - `rule` TR-6.2: 创建/轮换/撤销/遗留删除在浏览器中走通（手工或浏览器代理验证），表单均带 nonce。
  - `rubric` TR-6.3: 可用性与风格一致性；scale 1-5；anchors 见 AC-15；threshold >= 4；证据为页面走查截图/描述。

## Task 7: 删除用户级联与遗留清理接线
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 1
- **Description**:
  - `common/lib/user.py` 的 `User.delete()`：保留旧表删除，并在同一事务中删除 `access_token_series` 中该用户全部行（代际由外键级联）。
  - 全仓 grep 确认无其他对旧令牌表/`request_token` 的孤立引用。
- **Acceptance Criteria Addressed**: AC-12
- **Test Requirements**:
  - `rule` TR-7.1: 删除用户后旧表与两新表均无该用户数据；其遗留与新令牌再认证失败。

## Task 8: 单元测试
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - 新增 `tests/test_access_tokens.py`：以伪行字典/mock DB 覆盖 TR-2.1–2.5，并增加端点映射归属测试（TR-4.4 所需的端点名清单断言）、快照不扩权、nonce 收敛。
  - 不依赖真实 Postgres；遵循 tests/ 既有 mock 风格。
- **Acceptance Criteria Addressed**: AC-14
- **Test Requirements**:
  - `rule` TR-8.1: 新测试文件全部通过且覆盖每个核心纯函数的主要分支。
  - `rule` TR-8.2: 故意制造越权/过期/撤销输入时测试能失败（断言有效，不 vacuous）。

## Task 9: 全量校验与收尾
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 4, Task 5, Task 6, Task 7, Task 8
- **Description**:
  - 运行 `pytest`（全量）与 `ruff check .`；修复全部失败与告警。
  - 端到端手工/浏览器代理走查：创建→认证（三入口、scope、边界）→轮换重叠→到期/撤销→删除用户。
  - 自查无密钥泄露面（模板、flash、日志）。
- **Acceptance Criteria Addressed**: AC-13, AC-14, AC-16
- **Test Requirements**:
  - `rule` TR-9.1: `pytest` 退出码 0。
  - `rule` TR-9.2: `ruff check .` 退出码 0。
  - `rubric` TR-9.3: 架构契合度；scale 1-5；anchors 见 AC-16；threshold >= 4；证据为改动文件清单与集成方式走查。
