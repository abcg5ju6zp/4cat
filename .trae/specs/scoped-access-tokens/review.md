# 独立审查记录 — 按集成划分作用域访问令牌

## 第一轮（实现完成后）
- 结果：**FAIL**（无 Critical，3 Major + 5 Minor）
- Major：
  - M1：轮换 CTE 中 FOR UPDATE 行锁与 MAX 子查询同语句快照，不同 nonce 并发轮换必抛 UniqueViolation 并毒化共享连接（已在真实 Postgres 复现）。
  - M2：check-query、check-processors、erase-credentials 三处数据集解析点漏了边界检查。
  - M3：listed 边界只认 owner，viewer 永远 403，违反 FR-2。
- 另在修复验证中发现：build_snapshot 用 get_all() 只取到库中已持久化的特权，默认特权未进快照（默认权限用户的令牌在 setting_required 端点失效）。

## 第二轮（修复复核）
- 逐项复核结论：M1/M2/M3 与附带项全部 PASS；并发实证（2 连接×2 线程×30 轮）60 代严格递增、零错误、连接存活；172 pytest、ruff 全绿。
- 新提 F1：scope/边界仅在两个 API 蓝图生效，令牌可驱动 views_dataset 的交互式/下载路由（不超过用户自身访问面，但削弱"边界"承诺）。
- 裁决：虽属已批准假设（"scope/边界只在 API 蓝图执行"），仍采用更强的纵深防御——新增 app 级 before_request `enforce_api_only_token`：新令牌在非 API 蓝图一律 403，遗留令牌与会话用户行为不变。F1 关闭。
- Minor 处置：F3（重叠选项改为常量驱动）、F4（create 非完整性异常先 rollback 再抛）已修；F2（静态扫描为启发式）接受为后续 AST 化改进；F5（管理员 break-glass）记录为后续增强。

## Checkpoints

- [x] CP-1 数据库发布物（AC-13）：database.sql 与迁移逐字一致；迁移本地 PG17 连跑两次幂等；schema 一致性测试通过。
- [x] CP-2 密钥保密（AC-5/AC-10）：库内仅 SHA-256；成功响应仅出现一次完整令牌且 no-store；列表仅 token_id；遗留打码；E2E 断言。
- [x] CP-3 快照与不扩权（AC-3/AC-4）：17 个 privileges 全量快照；admin 提权后公开切换仍 403；停用/来源标签移除即拒，恢复子集即放行。
- [x] CP-4 轮换/重叠/撤销/收敛（AC-6/7/8）：重叠窗口、零重叠、同 nonce 重放收敛、不同 nonce 并发不碰撞（实证）、撤销隔离。
- [x] CP-5 scope 与边界（AC-2/9）：29 个 API 端点映射完整并有静态测试；16 个按 key 解析站点全部接边界；viewer 角色用例；三入口矩阵。
- [x] CP-6 兼容性（AC-11/12）：遗留 request-token、calls、实时权限、HTML 路由行为不变；删除用户三表级联。
- [x] CP-7 质量门（AC-14）：172 pytest passed（新增 26）、ruff All checks passed。
- [x] CP-8 架构契合度（AC-16）：common 纯逻辑 + web 薄封装；遗留路径零改动。评分 4/5（边界手工接入点以静态测试锁定，未做到框架级统一钩子，记为后续改进）。
- [x] CP-9 界面（AC-15）：36 项渲染状态检查 + 35 项端到端交互检查；浏览器沙箱无法访问宿主端口，以 Flask test client 渲染证据替代。评分 4/5。

## 最终结论：PASS
- 证据：tests/test_access_tokens.py（26）、全量 pytest 172 passed、ruff 0；真实 Postgres E2E 35/35；并发轮换 60/60；迁移幂等。
- 已知残余（非阻断）：F2 静态扫描启发式；F5 缺管理员侧批量撤销界面（停用账户仍可即时止血）；m4 类非 API 面已通过 app 级门控消除。
