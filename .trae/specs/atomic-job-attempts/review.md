# Review R2（独立复审，全新上下文）
- **Result**: `pass`
- **Date**: 2026-10-13
- **范围**: 仅针对 R1 驳回项（F1-F4、A1、A2）的修复做独立验证，并按 AC-1..AC-19 全量复核；未只信实现者自证。

## Checks Performed
- 通读 spec.md、tasks.md（I-1..I-4 均 completed）、R1 review.md；逐行阅读 13 个改动文件与 2 个新模块（attempt.py、result_validation.py）。
- 全仓重新枚举 `self.job.finish/release/park`：processors/ 4 处（3 文件）、datasources/ 8 处（3 文件）、backend/ 43 处（13 文件），逐一在新代理/条件化语义下判定。
- 静态+动态双路核查：`_AttemptBoundJob` defer 窗口、`Attempt.finalize` 条件 UPDATE、gate 事务（commit=False）、bind/release 顺序、注解窗口、attach 改名、recurring 重置、proxy `__setattr__/__getattr__`。
- 编写**临时取证脚本**（28 项断言，运行后已删除，不留仓库），独立复现：F2 陈旧内存态（本地仍 running、DB 已 committed）下 finalize 必败且 SQL 含 `status NOT IN`；F1 代理 defer/真实/败者/外带 attempt_id/属性转发；A1 `.gexf` 改名候选端到端发布；F4 无 attempt 旧行为；F3 source 未就绪时 release 先于 bind 且行不打标。
- 复核全部 10 个测试文件中断言的实质（同义反复排查）。
- 重跑 `.venv/bin/python -m pytest -q` → **219 passed**；`.venv/bin/python -m ruff check .` → **All checks passed!**。

## Evidence

### F1（Critical）修复成立
- 代理仅对处理器安装：[processor.py:183-186](file:///Users/ding/Documents/swe/0910125/project-07/backend/lib/processor.py#L183-L186)；`BasicHTTPScraper`/`FourchanImageDownloader`/`ThreadScraper4chan` 均直接继承 `BasicWorker`（[scraper.py:13](file:///Users/ding/Documents/swe/0910125/project-07/backend/lib/scraper.py#L13)、[download_images.py:9](file:///Users/ding/Documents/swe/0910125/project-07/datasources/fourchan/scrapers/download_images.py#L9)、[scrape_threads.py:34](file:///Users/ding/Documents/swe/0910125/project-07/datasources/fourchan/scrapers/scrape_threads.py#L34)），`is_4cat_processor()=False`，永不包代理，worker 尾部保持与 HEAD 完全一致的无条件 finish——R1 所担心的误伤面不存在。
- defer 窗口用 try/finally **精确覆盖 `process()+after_process()`**：[processor.py:395-405](file:///Users/ding/Documents/swe/0910125/project-07/backend/lib/processor.py#L395-L405)；窗口内 `finish()` 只置 `_job_finish_requested`，不删 jobs 行（[processor.py:66-76](file:///Users/ding/Documents/swe/0910125/project-07/backend/lib/processor.py#L66-L76)）；发布由 gate 事务内条件删除收口。
- 四个既有处理器调用点逐一判定：
  - [search_tumblr.py:444](file:///Users/ding/Documents/swe/0910125/project-07/datasources/tumblr/search_tumblr.py#L444)：位于 `Search.process()` 调用链内（[search.py:52-98](file:///Users/ding/Documents/swe/0910125/project-07/backend/lib/search.py#L52-L98)），窗口内 defer，随后 gate 正常发布。
  - [url_titles.py:198](file:///Users/ding/Documents/swe/0910125/project-07/processors/metrics/url_titles.py#L198)：defer + A2 行数修复，CSV 行数=num_rows，闸门通过。
  - [download_tiktok_video.py:242](file:///Users/ding/Documents/swe/0910125/project-07/processors/visualisation/download_tiktok_video.py#L242)（EMPTY，0 行无候选→纯 DB 发布，AC-9 恢复）与 [:334](file:///Users/ding/Documents/swe/0910125/project-07/processors/visualisation/download_tiktok_video.py#L334)（finish(total_processed)，writerow 次数==total_processed）。
  - [audio_to_text.py:308-309](file:///Users/ding/Documents/swe/0910125/project-07/processors/machine_learning/audio_to_text.py#L308-L309)：`release(delay=60)` 后立即抛 `ProcessorInterruptedException`；代理对 release/park **不 defer**，即时执行条件 release；异常经 `WorkerInterruptedException`→`abort()`，因 `interrupted=False` 两个分支都不进（与代码注释设计一致），作业带 attempt_id 留在 60s 等待态，再领取时由 reconcile 判定 superseded 后安全重跑。
- 异常/abort/真实模式：窗口外 `finish()` 走 `self._job.finish(attempt_id=当前 attempt)`，0 行返回 False（取证脚本证实败者可感知）；调用方显式传入的外来 attempt_id 被代理剥离；`finish_with_error/warning` 走 DataSet.finish 延迟登记（[dataset.py:1162-1172](file:///Users/ding/Documents/swe/0910125/project-07/common/lib/dataset.py#L1162-L1172)）；preset 改调 `finish_job()`（[preset.py:51](file:///Users/ding/Documents/swe/0910125/project-07/backend/lib/preset.py#L51)），其 gate 对无 pending 的 preset 行只去标记、缓冲子作业同事务插入。
- 代理 `__setattr__` 仅转发 `is_finished/is_claimed/is_parked/data`；全仓对 job 的属性赋值只有 gate 一处（processor.py:731），无副作用面。

### F2（High）修复成立
- [attempt.py:422-427](file:///Users/ding/Documents/swe/0910125/project-07/common/lib/attempt.py#L422-L427)：终态写为 `UPDATE ... WHERE attempt_id=%s AND status NOT IN %s`；0 行时回读 DB 刷新本地态并返回 False，committed 不可翻转。**独立取证**：构造本地仍为 running、条件 UPDATE 返回 0、DB 行为 committed 的迟到 worker，`finalize(FAILED)` 返回 False、本地态刷新为 committed、SQL 确实含 `status NOT IN`（新增回归用例自身对象已终态，未直接压到该陈旧本地态分支，故以脚本补足）。
- gate（[processor.py:656-727](file:///Users/ding/Documents/swe/0910125/project-07/backend/lib/processor.py#L656-L727)）：`pg_advisory_xact_lock`→事务内复核 `jobs.attempt_id`→条件 delete（一次性）/条件 reset（recurring：timestamp_claimed=0,attempts=0,attempt_id=''）→终态数据集→去标记→flush 子作业→finalize(COMMITTED, commit=False)→单次 commit；败者抛 `LostArbitrationException` 并 rollback，post-commit 动作（邮件/copy_to）零执行（test_cancel_winning_inside_transaction + test_late_worker 有效断言）。
- commit=False 链路与 Database 语义一致：每游标独立但连接级事务共享（[database.py:103-126,412-424](file:///Users/ding/Documents/swe/0910125/project-07/common/lib/database.py#L103-L126)），rowcount 执行后即可用；测试以 MagicMock 模拟、真实代码路径无中途 commit。
- worker 尾部/中断/崩溃三路径全部处理器条件化（[worker.py:136-167,220-226](file:///Users/ding/Documents/swe/0910125/project-07/backend/lib/worker.py#L136-L167)）；轻量作业保持无条件收口且仅由轻量路径 finalize。全仓再无其他终态写：唯一的 `transition(status=PUBLISHING)` 发生在属主单线程内、且 phase 单调。

### F3（Medium）修复成立
- 结构：source 未就绪 `release(delay=30)` 早退（[processor.py:311-316](file:///Users/ding/Documents/swe/0910125/project-07/backend/lib/processor.py#L311-L316)）在 `bind_attempt`（[:332-333](file:///Users/ding/Documents/swe/0910125/project-07/backend/lib/processor.py#L332-L333)）之前；orphan/数据集已删两分支同样不 bind。
- 控制流取证：模拟未完成 source，release 触发、bind 零调用、datasets 行无打标 UPDATE——F3 的“子行永久隐藏”消除，等待期间结果页可见。
- resume：adopt 后 `bind_attempt(quarantine=False)`，已发布文件不会被二次封存；reconcile 的 adopt（validated/files_published 不重跑 process，有 AssertionError 间谍测试）/supersede（processing 全新隔离区）在新顺序下仍成立；gate 对已发布文件按 manifest sha256 复核，子作业 ON CONFLICT 幂等不重复。

### F4（Medium）修复成立
- 窗口 SQL（[processor.py:922-926](file:///Users/ding/Documents/swe/0910125/project-07/backend/lib/processor.py#L922-L926)）：`from_dataset=%s AND timestamp>=timestamp_created AND timestamp<=heartbeat_at+600`；下界=领取时刻，覆盖本次处理全部写入（含 resume 采纳旧尝试的原始时刻），心跳由 bound 数据集每次 update_status 刷新（[dataset.py:1927-1933](file:///Users/ding/Documents/swe/0910125/project-07/common/lib/dataset.py#L1927-L1933)）。
- 不删新尝试注解的时序论证：cleanup 与 job release/finish 在同一线程同步执行（abort/异常路径），DELETE 只影响已存在行；唯一“边 release 边继续”的处理器点（audio_to_text）release 后立即 abort 清理，新作业 60s 后才可领取，清理早已结束；非处理器 worker 不调用 clean_up_on_error。
- 无 attempt 时保留旧行为（无条件 db.delete，取证确认）；注解按 (field_id,dataset,item_id) upsert，极端漏删只会保守残留、不会重复。

### A1 / A2
- A1：gate 逻辑候选路径取 `planned.result_file`（[processor.py:543-548](file:///Users/ding/Documents/swe/0910125/project-07/backend/lib/processor.py#L543-L548)），终态 UPDATE 同步改 result_file（[:703-705](file:///Users/ding/Documents/swe/0910125/project-07/backend/lib/processor.py#L703-L705)）；attach 后缀变更（.csv→.gexf）端到端取证：候选按新名找到、按新名发布、DB 改名、旧名不上架。
- A2：[url_titles.py:146,173-185,194,197](file:///Users/ding/Documents/swe/0910125/project-07/processors/metrics/url_titles.py#L146-L197) 新增 `rows_written`，每次 writerow +1，finish/finish_with_warning 均用之，与 CSV 数据行严格一致。

### 新回归测试有效性
6 用例中 5 个为强断言：有状态 StatefulDB 的 F1 全链路（行存活→条件删除→发布路径字节一致→committed）、真实模式条件 finish、recurring 条件 reset、注解窗口 SQL 参数、F3 源码顺序；`test_finalize_never_flips_a_committed_attempt` 偏弱（对象本地态已终态，走的是内存短路而非 SQL 守卫），但同一守卫已由本复审脚本对陈旧本地态独立证实，不构成阻断。

## Checkpoint Results
- **CP-R1（rule，AC-1..AC-17）：pass**
  - AC-1 ✓ claim 同 UPDATE 盖戳/败者不改写（test_job_claim_*）；AC-2 ✓ 签名 6 类篡改参数化；AC-3/4/5 ✓ 路径重定向/封存/日志不重定向（test_dataset_attempts）；AC-6 ✓ 子作业缓冲、get_children `attempt_id=''` 过滤、discard 新建行删除/预置行仅去标；AC-7 ✓ 坏 CSV/NDJSON/ZIP/计数参数化；AC-8 ✓ 检查点+事务+故障回滚；AC-9 ✓ 0 行纯 DB 发布 + tiktok EMPTY 路径恢复；AC-10 ✓ 对账 process() 零调用间谍；AC-11 ✓ processing 崩溃 supersede 全新隔离区；AC-12 ✓ cancel 条件链；AC-13 ✓ 迟到 worker 预检/事务双裁决零副作用；AC-14 ✓ GC 状态×年龄×引用矩阵且不触发布路径；AC-15 ✓ attempt_status_payload 已接入两个 check API；AC-16 ✓ 一次性删除/周期重置/下载基名/folder/attach/preset/filter standalone（copy() 末尾 copy.finish 使 standalone 进入 pending，gate 不漏发）/copy_to/邮件时序；AC-17 ✓ database.sql 与幂等迁移列/表一致，schema 测试在 219 全绿内，VERSION=1.58。
- **CP-U1（AC-18 低侵入 rubric）：5 分** — 处理器/数据源仅 1 文件 9 行改动（url_titles 行数正确性，属修复必需而非垫片）；test_modules 全量加载 118 workers/99 processors/29 datasources 通过；隔离/发布/对账全部框架透明接管。
- **CP-U2（AC-19 测试门禁 rubric）：4 分（达阈值）** — 219 passed、ruff clean；状态机各分支、崩溃注入、仲裁、GC 矩阵、签名均有单测；扣分点：无真实 Postgres 并发（TR-9.2 的 20 次交错以条件写+咨询锁推理替代）、preset attach/recurring 端到端为组件级拼装而非完整 work() 驱动。

## Findings（无 actionable/阻断项）
1. `observation`；[processor.py:290-299](file:///Users/ding/Documents/swe/0910125/project-07/backend/lib/processor.py#L290-L299) preset 找不到非 preset 父级的“不应发生”分支 `job.finish();return` 未 finalize attempt，遗留 RUNNING 历史行；与同文件 DataSetException/orphan 分支不一致。若同 key 作业重入会被 reconcile supersede 自愈，影响仅限可追溯历史行（同 R1-A5 类），建议顺手补一个 finalize(CANCELLED)。
2. `observation`；建议把 `test_finalize_never_flips_a_committed_attempt` 强化为“本地态非终态+SQL 0 行”变体，直接锁定条件 UPDATE 守卫（本复审已临时取证，行为正确）。
3. `observation`；files_published 阶段对账时附属 folder 仅列名不做摘要复核（manifest 设计本就只列文件）；结合 bind quarantine 顺序，目标冲突实际不可达，风险可忽略。
4. R1 的 A3-A7 按 tasks.md 记录未采纳/延后，理由与 spec 边界一致（A3 非 AC 要求；A4 心跳为 FR-1 所需；A5 历史行保留；A6 单后端无并发运行时；A7 对账自愈），无异议。

## 结论
F1-F4 与 A1/A2 修复均有代码与独立证据支撑，未发现新引入的正确性问题；全部 checkpoint 达通过标准。**Result: pass**，可交付。建议后续可选处理上述 3 条 observation，不阻断合入。
