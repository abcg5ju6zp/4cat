# 委托抓取请求持久化与接管恢复 实施计划

## 需求理解（来自用户描述）

处理器通过 `DelegatedRequestHandler`（`backend/lib/proxied_requests.py`）把大量抓取请求委托给代理池；目前批次、队列、future 全部只存在于进程内存。需要做到：

1. 每个处理器拥有的委托批次有**稳定身份**和**输入顺序**。
2. 每个请求**持久记录**：领取者、外部尝试（attempt）、响应摘要、结果状态、是否已交付。
3. 接管者只重试**没有可确认结果**的请求；恢复迭代时**跳过已交付项**并保持处理器可观察的顺序。
4. 代理健康/冷却继续独立于请求所有权：代理失败只影响相应尝试；可换代理的请求继续；单项外部错误不丢弃批次内已成功结果。
5. 处理器取消、作业终止、批次过期：停止未领取请求；标记运行中尝试的处理方式；忽略迟到响应。
6. 重启、重复 flush、多个后台并发领取：不能重复交付，旧 owner 不能覆盖接管结果。
7. 无代理直连、localhost 回退、正常小批次处理保持兼容。

## Repository Research（现状结论）

- **委托器**：`DelegatedRequestHandler`（[proxied_requests.py](file:///Users/ding/Documents/swe/0910125/project-08/backend/lib/proxied_requests.py)），每个后台一个实例（`WorkerManager.__init__`，[manager.py#L48](file:///Users/ding/Documents/swe/0910125/project-08/backend/lib/manager.py#L48)），被多个处理器 worker 线程共享；所有公开方法已被同一把 `RLock` 串行化。
- 内存状态：`self.queue = {queue_name: [url_metadata, ...]}`，状态机 `QUEUED(0) → STARTED(1) → WAITING_FOR_YIELD(2)`；`proxied` 持有 future/代理/结果；`halted` 集合；`get_results(preserve_order)` 按队列顺序交付。
- **处理器侧入口**（[processor.py#L454-L535](file:///Users/ding/Documents/swe/0910125/project-08/backend/lib/processor.py#L454-L535)）：`iterate_proxied_requests()`（50 条一批，0.05s top-up）、`push_proxied_request(url, position)`、`flush_proxied_requests()`（= `halt_and_wait`，取消未发请求、取消 future、排空并删除队列）；队列名 `{type}-{dataset.key}`。
- 使用方仅 4 个处理器：[download_images.py](file:///Users/ding/Documents/swe/0910125/project-08/processors/visualisation/download_images.py)（stream hook 落盘、position=0 动态追加 imgur 图片 URL）、[download_videos.py](file:///Users/ding/Documents/swe/0910125/project-08/processors/visualisation/download_videos.py#L638)、[download_tiktok_video.py](file:///Users/ding/Documents/swe/0910125/project-08/processors/visualisation/download_tiktok_video.py#L256)、[url_titles.py](file:///Users/ding/Documents/swe/0910125/project-08/processors/metrics/url_titles.py#L149)。kwargs 含 `headers/timeout/stream/verify/allow_redirects`（可序列化）与 `hooks={"response": callable}`（**不可序列化**，如绑定方法 `self.stream_url`、静态方法 `URLFetcher.stream_url`）。
- **生命周期**：关停时 manager 对 worker 发 `INTERRUPT_RETRY`；`BasicWorker.run` 释放作业（10s 后可再领取）并调用 `abort()`；`BasicProcessor.abort()` → `clean_up_on_error()` → `flush_proxied_requests()`（[processor.py#L422-L452](file:///Users/ding/Documents/swe/0910125/project-08/backend/lib/processor.py#L422-L452)）。崩溃走 `Job.park()`，仅重启后释放（[job.py#L164-L183](file:///Users/ding/Documents/swe/0910125/project-08/common/lib/job.py#L164-L183)）。手动取消为 `INTERRUPT_CANCEL`，作业 finish。
- **代理故障处理现状**：`ProxyError` 先 probe，探测失败只把**该代理**标记不健康、该请求回到 QUEUED 换代理重试；超时/连接错误等封装为 `FailedProxiedRequest` 交给处理器决定，不影响其他请求。健康/冷却逻辑在 `SophisticatedFuturesProxy`，与请求无耦合。
- **持久化设施**：Postgres；`Database` 封装支持 `insert(safe/constraints/RETURNING)`、`upsert`、`update`（返回影响行数，可做 CAS）、`execute_values`；可用 `FOR UPDATE SKIP LOCKED`。建表在 [backend/database.sql](file:///Users/ding/Documents/swe/0910125/project-08/backend/database.sql)，既有安装靠迁移脚本；当前 `VERSION=1.57`，最新迁移为 1.55→1.56（1.56→1.57 无脚本），故本次新增 **migrate-1.57-1.58.py + VERSION→1.58**。
- 作业表 `jobs.claim()` 本身是原子 CAS，同一作业同一时刻只有一个后台运行；但接管窗口内多后台/重复 flush 仍需批次与请求两级 fencing。
- 测试 `tests/test_modules.py` 不实例化 manager/handler；handler 目前构造参数为 `(log, config)`。

### 关键设计决策（需评审确认的假设）

1. **响应体无法跨进程复活**：`requests.Response`（尤其 stream hook 已落盘/已置 `_content=False`）不能从数据库重建。因此终态成功但尚未交付的请求，重启后以轻量 **`RecoveredProxiedRequest`**（携带持久化的响应摘要：status_code/reason/final url/白名单 headers/所用代理/自定义标记位）交付；终态失败以 `FailedProxiedRequest` 配合按类名尽力还原的异常交付。"只重试没有可确认结果的请求"由此成立：**有终态记录就不重发**，代价是恢复对象不含响应体（hook 产物如文件是否可用取决于处理器自身清理策略，现状 `clean_up_on_error` 会删除部分产物，本次不改动该行为）。
2. **批次身份**含 `jobs.id`：`{processor_type}-{dataset_key}-job{job_id}`。同一作业重试（release/park 后重启）复用同一批次；作业取消后重新排队产生新作业 → 新批次，旧批次自然过期。
3. **批内 URL 去重语义**：同一批次内同一 URL 最多交付一次（当前 4 个调用方均自行对 URL 去重，符合现状）；恢复时处理器重新提交的 URL 命中"已交付"即静默跳过，命中未完成行则合并 kwargs（主要是 hook 等不可序列化项）而不新建请求；仅在原批次中由运行时动态插入（imgur push）的行直接从库中恢复。
4. **持久化仅对处理器绑定队列启用**；匿名队列（默认 `"_"`）与 DB 不可用时完全维持现状的纯内存行为；任何持久化写入失败都降级为内存行为并记日志，不中断抓取。
5. hook 可序列化策略：可解析为"模块路径+限定名"的 callable（函数/静态方法/`self.方法`）存为引用，接管时由处理器提供的绑定器还原；不可解析的 hook 不持久化（仅当次进程有效），记录调试日志。

## 数据模型（新增 3 张表）

```sql
delegated_batches
  key TEXT PK                     -- {type}-{dataset_key}-job{job_id}
  processor_type TEXT, dataset_key TEXT, job_id BIGINT
  status TEXT                     -- active | completed | cancelled | expired
  owner_id TEXT                   -- 后台实例 id（hostname-pid-uuid8）
  owner_generation INTEGER        -- 每次接管 +1 的 fencing 代号
  lease_expires DOUBLE PRECISION   -- 批次 owner 租约（epoch 秒）
  next_seq DOUBLE PRECISION        -- 追加序号分配器（步长 1024）
  expires_at DOUBLE PRECISION      -- 批次过期时刻（按最近活动滚动）
  created_at, updated_at DOUBLE PRECISION

delegated_requests
  id BIGSERIAL PK
  batch_key TEXT FK ON DELETE CASCADE
  seq DOUBLE PRECISION             -- 批内顺序（NUMERIC；插入 position 取邻接中点，逼近时整批 renorm）
  url TEXT
  kwargs JSONB                     -- 可 JSON 化的请求参数（hook 存引用）
  status TEXT                      -- queued | claimed | running | succeeded | failed | cancelled | expired
  claimer TEXT                     -- 当前尝试的领取者（后台实例 id）
  attempt_no INTEGER               -- 当前/最后尝试序号
  lease_expires DOUBLE PRECISION    -- 请求尝试租约
  result_kind TEXT                 -- response | error（可确认结果的种类）
  response_summary JSONB           -- 响应摘要/错误摘要
  delivered BOOLEAN, delivered_at DOUBLE PRECISION
  created_at, updated_at
  UNIQUE(batch_key, seq); INDEX(batch_key, delivered, seq); INDEX(status, lease_expires)

delegated_attempts
  id BIGSERIAL PK
  request_id BIGINT FK ON DELETE CASCADE
  attempt_no INTEGER
  proxy TEXT                       -- 已脱敏（remove_proxy_credentials），localhost 记 __localhost__
  outcome TEXT                     -- succeeded | request_error | proxy_error | cancelled | abandoned | expired
  summary JSONB                    -- 错误类/消息、HTTP 状态等
  started_at, finished_at
  UNIQUE(request_id, attempt_no)
```

## Files and Modules

- [backend/database.sql](file:///Users/ding/Documents/swe/0910125/project-08/backend/database.sql)：追加 3 张表 DDL（CREATE TABLE IF NOT EXISTS + 索引）。
- `helper-scripts/migrate/migrate-1.57-1.58.py`（新建）：幂等执行同一 DDL；风格参照 `migrate-1.55-1.56.py`（自建连接、print 进度）。
- [VERSION](file:///Users/ding/Documents/swe/0910125/project-08/VERSION)：1.57 → 1.58（随迁移协议要求）。
- [common/lib/config_definition.py](file:///Users/ding/Documents/swe/0910125/project-08/common/lib/config_definition.py)：新增 `proxies.durable-delegation`（默认 True）、`proxies.owner-lease`（300s）、`proxies.request-lease`（300s，大于现有最大 timeout 70s）、`proxies.batch-expiry`（604800s）、`proxies.record-retention`（2592000s）。
- [backend/lib/proxied_requests.py](file:///Users/ding/Documents/swe/0910125/project-08/backend/lib/proxied_requests.py)：核心改动（见下）。
- [backend/lib/manager.py](file:///Users/ding/Documents/swe/0910125/project-08/backend/lib/manager.py)：构造 handler 时传入 db 配置；`delegate()` 每秒循环中调用 `proxy_delegator.heartbeat()`（续租 + 惰性过期/清理）。
- [backend/lib/processor.py](file:///Users/ding/Documents/swe/0910125/project-08/backend/lib/processor.py)：队列名含 job id；迭代开始时绑定批次（传入 hook 绑定器）；`abort()` 区分 RETRY（**detach**：放弃 owner、运行中尝试标 abandoned、不删批次）与 CANCEL（**cancel**）；成功完成时 `complete_batch()`。
- `tests/test_delegated_requests.py`（新建）：纯逻辑单测（kwargs 序列化/hook 引用、seq 中点与 renorm、响应/错误摘要、状态机决策函数）；不依赖 Postgres。

## proxied_requests.py 实施步骤（依赖序）

1. **基础设施**：新增 `RecoveredProxiedRequest`、`RecoveredRequestError`、`DelegatedRequestConflict`、状态常量；后台实例 id（hostname-pid-uuid8）；专用 DB 连接**懒建立**（config 的 DB_* 凭据，appname `4cat-delegator`；失败则该实例降级内存模式）。
2. **绑定与批次身份** `bind_batch(queue_name, processor_type, dataset_key, job_id, hook_binder=None)`：upsert 批次行 → 原子接管
   `UPDATE ... SET owner_id=me, owner_generation=owner_generation+1, lease_expires=... WHERE key=%s AND status='active' AND (owner_id IS NULL OR lease_expires<=now) AND owner_generation=%s RETURNING ...`；CAS 失败（被存活他者持有）抛 `DelegatedRequestConflict`（处理器按可重试错误处理）。
3. **接管核对（同一事务）**：把本批 `claimed/running` 且（领取者非本实例或租约过期）的请求重置为 `queued/claimer=NULL`，并为其最后尝试插 `abandoned`；随后按 seq 装载所有 `delivered=FALSE` 且非终态取消的行进内存队列：终态未交付 → 构造恢复对象置 WAITING；queued → 常规等待；重建 url→行映射用于再提交去重。
4. **入队持久化**：`add_urls` 对绑定队列——已交付 URL → 生成静默跳过占位（永不 yield）；命中未完成行 → 合并 kwargs 不建行；新 URL 批量 insert 并分配 seq（追加：`UPDATE batches SET next_seq=next_seq+1024*n RETURNING`，倒推每条 seq；position 插入：邻接行中点，精度不足时整批 renorm 为 1024 倍数）。kwargs 经 `_serializable_kwargs()` 处理：脱敏代理信息、callable hook → 引用标记，其他不可 JSON 化对象丢弃并 debug 日志。
5. **领取持久化**：`manage_requests` 中真正发出 HTTP 前，把本 tick 新启动的请求**批量** UPDATE 为 `status='running', claimer=me, attempt_no=attempt_no+1, lease=now+request_lease`；该 UPDATE 以 `status IN ('queued')` 为条件，0 行说明被他者接管 → 不发请求（对应 future 也不建）。
6. **结果与尝试记录（fenced write）**：完成时按现状区分成功 / ProxyError+probe 失败（该尝试 outcome=`proxy_error`，请求回 queued、清 claimer、可换代理）/ 其他异常（`request_error`，终态 `failed` + `FailedProxiedRequest`）；终态写为
   `UPDATE delegated_requests SET status, result_kind, response_summary, updated_at WHERE id=%s AND claimer=me AND attempt_no=%s AND status IN ('claimed','running')`；
   **0 行即迟到响应/旧 owner → 忽略**（代理侧冷却、健康标记等纯本进程逻辑照旧执行，与所有权解耦）。成功时同事务 insert attempts 行；代理失败也插 attempts 行但请求不终结——单项失败不影响批内已成功结果（沿用现有逐项状态机）。
7. **交付去重**：`get_results` 在 yield 前执行
   `UPDATE delegated_requests SET delivered=TRUE, delivered_at=now WHERE id = ANY(%s) AND delivered=FALSE AND status IN ('succeeded','failed') RETURNING id`；
   仅对翻转成功的行 yield；重复 flush / 并发消费者因此恰好一次交付。内存队列顺序即 seq 顺序，`preserve_order` 语义不变。
8. **取消 / detach / 完成 / 过期**：
   - `cancel_batch(queue_name)`（= halt_and_wait 语义）：批次 `cancelled`；未领取请求 → `cancelled`（停止）；本实例运行中的尝试先 `request.cancel()` 再标请求 `cancelled`、attempt `cancelled`；非本实例的运行中尝试靠 generation/状态围栏使迟到写入落空；排空内存队列（忽略迟到结果）。
   - `detach_batch(queue_name)`（RETRY 路径新增）：批次 owner 置空、generation+1；未发请求保持 queued；运行中尝试标 `abandoned`、请求回 queued（不取消批次、不删行）；本进程置 halted 标记且拒绝再 add；尽量 cancel future，迟到结果被围栏拒绝。
   - `complete_batch(queue_name)`：处理器正常结束时调用；active → completed 并释放 owner（行保留至 retention）。
   - 惰性过期（heartbeat 中每 60s 一次）：超过 `batch-expiry` 的 active 批次 → `expired`，未领取请求 `expired`、运行中尝试 `expired`；超过 `record-retention` 的终态批次级联删除。
9. **heartbeat()**：manager 线程每秒调用；本实例持有的 active 批次与 running 请求在租约过半时批量续租；顺带做节流的过期/清理。后台实例死亡后，租约到期即允许他者接管。
10. **兼容性兜底**：未绑定队列维持全部现状代码路径；localhost/直连同样落持久层（proxy 记 `__localhost__`）但行为不变；DB 异常逐操作捕获 → 该队列降级内存模式；`halted` 行为保留。

## processor.py 改动

- `_proxy_queue_name()` → `f"{self.type}-{self.dataset.key}-job{self.job.data['id']}"`（dataset 不可用时回退旧形式，仅内存模式）。
- `iterate_proxied_requests()` 首次调用时 `bind_batch(..., hook_binder=self._bind_delegator_hook)`；hook 绑定器把引用还原为：匹配本处理器类的方法 → 绑定到 `self`，否则按模块路径导入。
- `abort()`：`INTERRUPT_RETRY` → 用 `detach` 取代 `flush`（随后照常 release）；`INTERRUPT_CANCEL` → 维持 cancel 语义（`flush_proxied_requests`）。`clean_up_on_error()`（崩溃/park 路径）同样改为 detach，使重启后接管而非整批重跑。
- `work()` 成功分支（`after_process()` 之后）安全调用 `complete_batch()`（异常仅记日志）。
- 处理器显式调用的 `flush_proxied_requests()`（提前拿够结果、错误退出等）语义不变 = cancel 剩余请求。

## Dependencies and Considerations

- 不新增第三方依赖；仅用 psycopg2/标准库。
- DB 写入均经现有 `Database` 封装与 handler 独占连接（所有调用点本就持有 `self.lock`，避免跨线程共用连接）。
- 迁移 DDL 幂等；新表不影响既有流程；Docker 启动自动执行迁移。
- hook 引用为尽力方案：不可解析 callable 仅当次有效；恢复后这类未开始请求会在无 hook 情况下执行（记 warning），处理器可据 `RecoveredProxiedRequest.recovered` 自行判别。
- 既有处理器在重试时会重建/截断自身输出文件（现状行为，本次不改）；"跳过已交付"是委托层契约，输出幂等性属处理器责任，在 PR 说明与代码注释中标注。
- `download_videos` 用 `next(task_iter)` 与交付配对；重启恢复时跳过已交付项会造成其配对错位——属上述处理器侧契约范畴，默认开关开启但可通过 `proxies.durable-delegation=False` 完全回退旧行为。
- 响应摘要 headers 采用白名单（content-type、content-length、location、content-disposition），避免持久化无关/敏感数据；代理 URL 一律脱敏。

## Validation

1. `pytest`（含新增 `tests/test_delegated_requests.py`：序列化、seq/renorm、围栏条件与状态转换的纯函数测试）全绿。
2. `ruff check .` 干净；Python 文件保持 tab 缩进。
3. 人工走查 SQL 路径：绑定/接管 CAS、fenced result write、交付 UPDATE 的恰好一次语义；迁移脚本在空库与已有库上幂等（CREATE IF NOT EXISTS）。
4. 兼容性自查：4 个处理器调用点签名不变；无代理配置（`__localhost__`）与空代理警告路径不变；`flush_proxied_requests` 提前终止语义不变。

## Risks

- **恢复对象无响应体**：以摘要对象交付，明确 `recovered` 标记；不满足需求时可改为"成功但未交付也重试"（破坏"只重试无确认结果"），故维持现方案并在评审中突出。
- **每请求额外 DB 开销**：入队/交付批量写、领取每 tick 一次批量 UPDATE、终态逐条（每 tick 通常 0–少数）；必要时后续可再批量化。
- **多后台并发接管竞争**：批次级 CAS + 请求级 claimer/attempt 围栏 + SKIP LOCKED 思路；冲突方抛 `DelegatedRequestConflict`，作业稍后重试。
- **租约时长权衡**：lease 300s + manager 秒级心跳；worker 线程长时间阻塞不影响心跳（心跳在 manager 线程）；后台硬死亡后最坏 300s 接管。
- **版本号**：按仓库迁移协议 bump VERSION 至 1.58 并新增迁移；`AGENTS.md` 中"1.53"为陈旧描述，不改动该文档。
