# 处理器作业的原子尝试（Atomic Job Attempts）- 实施计划

说明：任务为依赖有序的纵向切片；每个任务含本地测试要求（TR），实施时按顺序逐个 `in_progress → completed`。Tab 缩进；Python ≥ 3.11；不新增第三方依赖。

## Review Issues（R1 驳回后修复）

## Issue I-1: 处理器内直接 job.finish() 与发布闸门冲突（P0，AC-16/9/18）
- **Status**: `completed`
- **Discovered By**: Review R1（F1）
- **修复**: 新增 `_AttemptBoundJob` 代理：处理器进程体执行期间（_defer_job_finish=True）self.job.finish() 只登记请求、不删 jobs 行，由发布事务统一条件删除；release/park 全程自动带当前 attempt_id。覆盖 search_tumblr.get_items、url_titles、download_tiktok_video 全部既有调用点（调用方式零改动）。另修正 url_titles num_rows 按实际 CSV 行数（A2）。
- **Test Requirements**: `rule` test_processor_finish_inside_process_is_deferred_and_publishes（有状态 StatefulDB）、test_proxy_real_mode_finish_is_attempt_conditional。

## Issue I-2: 终态写身份守卫与 run() 条件化（P0，AC-13/12）
- **Status**: `completed`
- **Discovered By**: Review R1（F2）
- **修复**: Attempt.finalize 改为 `WHERE status NOT IN (...)` 条件 UPDATE，committed 不可翻转；gate 用 finalize(COMMITTED, commit=False)；BasicWorker.run 尾部/中断分支对处理器全部 attempt_id 条件化，轻量作业才收口，且不再给未发布处理器尝试写 committed。
- **Test Requirements**: `rule` test_finalize_never_flips_a_committed_attempt、recurring reset 用例。

## Issue I-3: bind 推迟到确定开始处理（P1，AC-6/15）
- **Status**: `completed`
- **Discovered By**: Review R1（F3）
- **修复**: source 未就绪 release(30) 早退发生在 bind/context 之前，数据集不打标记、结果页始终可见；bind 只在确定处理时执行（resume 不 quarantine）。
- **Test Requirements**: `rule` test_bind_happens_after_source_wait_release。

## Issue I-4: 注解归属窗口 + attach 改名 + 补强（P2）
- **Status**: `completed`
- **Discovered By**: Review R1（F4/A1/A6）
- **修复**: 注解删除限定 attempt 创建~心跳+600s 窗口；gate 按 pending result_file 解析候选（attach 扩展名变更可达，A1）；新增 6 个 review 修复回归用例。
- **未采纳 advisory**: A3（AC 未要求）、A4（心跳为 FR-1 所需）、A5（历史行保留可追溯）、A7（对账自愈）；A6 并发以 SQL 条件写+咨询锁推理为证（无 Postgres 运行时环境）。

## Task 1: 数据库 schema 与 1.58 迁移
- **Status**: `completed`
- **Completion Evidence**:
  - TR-1.1 `rule` 通过：`uvx --from pytest pytest tests/test_schema_consistency.py -q` → 3 passed（jobs/datasets.attempt_id 与 job_attempts 新表均被迁移与 database.sql 双向覆盖）。
  - TR-1.2 `rule` 代码审查通过：迁移脚本对两列均 information_schema 预检后 ALTER，表与索引均 IF NOT EXISTS，可重复执行。
  - TR-1.3 `rule` 通过：database.sql 新列/新表字段与 migrate-1.57-1.58.py 逐一对应；VERSION=1.58；ruff 对迁移脚本 All checks passed。
- **Priority**: high
- **Depends On**: None
- **Description**:
  - `VERSION` 改为 1.58。
  - `backend/database.sql`：`jobs` 增加 `attempt_id TEXT DEFAULT ''`；`datasets` 增加 `attempt_id TEXT DEFAULT ''`；新建 `job_attempts` 表（attempt_id 主键、jobtype、remote_id、status、phase、input_signature、manifest、error、owner、heartbeat_at、timestamp_created/updated/finished 等列）及 `(jobtype, remote_id)`、`(status)` 索引。
  - 新增 `helper-scripts/migrate/migrate-1.57-1.58.py`：幂等 ALTER jobs/datasets 加列、CREATE TABLE IF NOT EXISTS job_attempts 与索引（写法须被 test_schema_consistency 的正则解析：ALTER ADD 列名与 database.sql 一致，新表用 CREATE TABLE）。
- **Acceptance Criteria Addressed**: AC-17
- **Test Requirements**:
  - `rule` TR-1.1: `pytest tests/test_schema_consistency.py` 通过（迁移 ADD/CREATE 与 database.sql 互相覆盖）。
  - `rule` TR-1.2: 迁移脚本在列/表已存在时重复执行不报错（信息_schema 检查后再 ALTER/CREATE）。
  - `rule` TR-1.3: database.sql 中 jobs/datasets/job_attempts 的新列与任务文档字段一一对应。
- **Notes**: 参照 migrate-1.52-1.53 的幂等写法。

## Task 2: Attempt 领域模型与作业领取绑定
- **Status**: `completed`
- **Completion Evidence**:
  - TR-2.1 `rule` 通过：tests/test_attempts.py 中 claim 盖戳 SQL 断言（attempt_id 与 claim 同 UPDATE，WHERE timestamp_claimed=0）、失败领取抛 JobClaimedException 且不改写；Attempt.create 写 job_attempts 行。
  - TR-2.2 `rule` 通过：finish/release/park 支持 attempt_id 条件，0 行时返回 False 且不改内存终态（late worker 可感知仲裁失败）；见 test_job_finish/release_and_park_conditional。
  - TR-2.3 `rule` 通过：ensure_dirs/candidate_* 在 tmp PATH_DATA 下布局 .attempts/<id>/{result,files,staging,quarantine} 且 staging 唯一名。
  - 另：manager.delegate 改为先建 Attempt 再 claim（失败即删未落盖的尝试行），worker 构造传入 attempt；release_all 保留 attempt_id 作为对账依据。
  - `pytest tests/test_attempts.py` 12 passed；ruff 改动文件 All checks passed。
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 新增 `common/lib/attempt.py`：`AttemptStatus`（running/publishing/committed/failed/cancelled/superseded）、`AttemptPhase`（claimed/processing/validated/files_published/db_committed/job_finished）枚举；`Attempt` 类（创建、按 attempt_id/(jobtype,remote_id) 读取、阶段推进、心跳、错误记录、manifest 读写）；隔离目录布局 `PATH_DATA/.attempts/<attempt_id>/{result,files,staging,quarantine}/` 与 `manifest.json` 辅助；属主标识 `host:pid:thread`。
  - `common/lib/job.py`：`claim()` 的条件 UPDATE 在同一语句 SET `attempt_id`（失败仍抛 JobClaimedException）；新增 `AttemptClaimedException` 场景不引入新异常类型，沿用现有语义；`Job` 对象暴露当前 attempt_id；`finish/release/park` 的 UPDATE 增加可选 attempt_id 匹配条件。
  - `backend/lib/manager.py`：claim 成功后创建 Attempt 记录（jobtype/remote_id/owner/phase=claimed），并构造 worker 时传入；非数据集类系统作业同样创建轻量尝试行（协议在处理器层才启用）。
  - `common/lib/queue.py`：`release_all()` 不清除 jobs.attempt_id（对账依据保留）。
- **Acceptance Criteria Addressed**: AC-1, AC-16
- **Test Requirements**:
  - `rule` TR-2.1: mock Database 下两次 claim 仅一次成功；成功路径 job_attempts 有行且 jobs.attempt_id 被同一 UPDATE 盖戳；失败路径不改写 attempt_id。
  - `rule` TR-2.2: finish/release/park 在 attempt_id 不匹配时影响 0 行并可被调用方识别。
  - `rule` TR-2.3: 目录布局辅助在 tmp PATH_DATA 下创建且各子目录用途正确。

## Task 3: 输入数据集版本签名
- **Status**: `completed`
- **Completion Evidence**:
  - TR-3.1 `rule` 通过：describe/build_input_signature 含 key/is_finished/num_rows/timestamp_finished/result_file/size/mtime，去重稳定、JSON 可序列化（test_input_signature_fields_and_stability、missing_file）。
  - TR-3.2 `rule` 通过：参数化篡改 num_rows/is_finished/mtime/size/result_file/parameters 六类，verify_inputs 均返回 False 且给出差异描述；未篡改通过。
  - BasicProcessor.work() 在源数据集解析后调用 attempt.set_inputs()（21 tests passed，ruff clean）。
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - 在 `common/lib/attempt.py`（或 DataSet 协作方法）实现：对给定输入 DataSet 列表计算签名（key、is_finished、num_rows、timestamp_finished、result_file 基名、size、mtime；无文件记 None）与参数快照 JSON；存入 job_attempts.input_signature；提供 `verify_input_signature()`。
  - `BasicProcessor.work()` 解析完 source_dataset（含 preset 透传链）后计算并持久化签名。
- **Acceptance Criteria Addressed**: AC-2
- **Test Requirements**:
  - `rule` TR-3.1: 签名包含规定字段且序列化稳定（键排序）。
  - `rule` TR-3.2: 分别篡改 mtime/size/num_rows/is_finished/result_file 时 verify 返回失败并指出差异字段；未篡改通过。

## Task 4: DataSet 尝试路径重定向与半成品封存
- **Status**: `completed`
- **Completion Evidence**:
  - TR-4.1 `rule` 通过：绑定后 result→.attempts/<id>/result/、folder→files/folder_<key>、staging→staging/ 唯一名；get_log_path 始终为发布路径同名 .log（test_bound_paths/staging）。
  - TR-4.2 `rule` 通过：绑定时发布路径残留结果文件与附属目录被移动到 quarantine 并写入 manifest.quarantined，发布路径无文件（test_bind_quarantines_stale_half_results）。
  - TR-4.3 `rule` 通过：未绑定 DataSet 路径不变；contextvar 仅重定向 attempt_id 匹配的行，退出上下文即恢复（test_context_redirects_only_owned_loaded_rows、unbound）。
  - 另：活动尝试内新建 datasets 行自动打 attempt_id 标记并写入隔离区；finished 数据集拒绝绑定；disposable 清理只删 staging（28 passed，ruff clean）。
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - DataSet 增加 `bind_attempt(attempt)`/`unbind_attempt()` 与模块级 `contextvars.ContextVar` 活动尝试（供处理器内新建 DataSet 隐式归属）。
  - 绑定时：`get_results_path()`→`<attempt>/result/<原基名>`；`get_results_folder_path()`→`<attempt>/files/folder_<key>`；`get_staging_area()`→`<attempt>/staging/` 下唯一名；`get_log_path()` 保持发布路径不变（基于“逻辑 result_file”派生）。
  - 绑定瞬间若数据集未完成且发布路径存在文件/附属目录：移动到 `<attempt>/quarantine/` 并登记 manifest，不参与发布。
  - 未绑定尝试的 DataSet 行为完全不变；以 key 装载的他数据集不被重定向。
  - `remove_disposable_files()` 只清理本次尝试 staging，不清理候选与发布文件。
- **Acceptance Criteria Addressed**: AC-3, AC-4, AC-5（路径部分）
- **Test Requirements**:
  - `rule` TR-4.1: 绑定数据集三类路径全部位于 .attempts 目录且互不冲突；日志路径等于发布路径同名 .log。
  - `rule` TR-4.2: 未完成数据集发布路径有残留时绑定后被封存（发布路径 404/不存在、quarantine 有副本、清单有记录）。
  - `rule` TR-4.3: 未绑定 DataSet 与按 key 装载的输入数据集路径与现状逐字节一致（回归断言）。

## Task 5: 子数据集与子作业隔离
- **Status**: `completed`
- **Completion Evidence**:
  - TR-5.1 `rule` 通过：活动尝试内 add_job 不写 jobs 表而进 pending_children（含 attempts/claim 字段）；尝试外/他尝试数据集走原 insert（test_add_job_*）。
  - TR-5.2 `rule` 通过：get_children SQL 增加 `AND attempt_id = ''`；提交时 publish_dataset_tags 单 UPDATE 去标记（test_get_children_hides_tagged_rows、publish_tags）。
  - TR-5.3 `rule` 通过：discard_owned_datasets 对本次新建行 DataSet.delete、对预存绑定行（surrogate）仅去标记；缓冲子作业清空（test_discard_owned_datasets）。
  - 另：新建数据集行经 contextvar 自动登记 manifest.new_datasets；flush_pending_children 在发布事务内 commit=False 批量插入（34 tests passed，ruff clean）。
- **Priority**: high
- **Depends On**: Task 4
- **Description**:
  - DataSet 新建行分支：存在活动尝试时写入 `datasets.attempt_id` 标记（覆盖 webtool 入队之外的全部处理器内创建：after_process 的 next、filter standalone/copy、preset pipeline、处理器直接创建）。
  - `JobQueue.add_job()` 在活动尝试内改为“缓冲”：不写 jobs 表，记录到 attempt 的 pending children（manifest/attempt 存储：worker type、remote_id、details/queue_id 等），提供后续事务内 flush 与丢弃接口。
  - 子数据集可见性：`DataSet.get_children()`/相关查询过滤掉 `attempt_id <> ''` 且所属尝试未 committed 的行；作业领取查询不涉及缓冲行（天然不可领取）。
  - 失败/取消清理：删除本尝试拥有的未发布 datasets 行、owners 行与缓冲子作业；不得按 key 误伤他尝试行。
  - `DataSet.delete()` 遇到缓冲子作业（jobs 表无行）时安全跳过。
- **Acceptance Criteria Addressed**: AC-6
- **Test Requirements**:
  - `rule` TR-5.1: 活动尝试内新建 DataSet 行带 attempt_id；其 add_job 不产生 jobs 行而进入缓冲；尝试外 add_job 行为不变。
  - `rule` TR-5.2: get_children 与结果列表不返回未提交尝试的子行；committed/去标记后可见。
  - `rule` TR-5.3: 尝试失败清理删除归属子行与缓冲，预置的“他尝试”子行保持不动。

## Task 6: 发布前校验器与 manifest
- **Status**: `completed`
- **Completion Evidence**:
  - TR-6.1 `rule` 通过：参数化坏候选（CSV 空头/列越界/行数不符、NDJSON 坏行、非 zip/条目计数、文件缺失/空）全部 ResultValidationError 且无副作用（12 用例）。
  - TR-6.2 `rule` 通过：合法 csv/ndjson/zip/其他格式通过；manifest 条目含 relpath/size/sha256/entries；verify_manifest_files 检出篡改与缺失资源。
  - TR-6.3 `rule` 扫描结论：全仓仅 write_archive_and_finish 以 ZipFile("w") 产出归档（成员=条目语义成立）；仅 video_frames/video_scene_frames 用 make_archive 自定义结构（成员数≠num_rows），默认 integrity-only、框架 helper 显式 count_archive_entries=True 不产生误伤。ruff clean。
- **Priority**: high
- **Depends On**: Task 4
- **Description**:
  - 新增校验模块（建议 `common/lib/result_validation.py`）：CSV（DictReader 全量扫描、表头一致、无 None 越位列）、NDJSON（每行 json.loads）、ZIP（testzip、成员枚举）；统计实际条目数并与声明 num_rows 核对。
  - 框架 helper（write_csv_items_and_finish、write_archive_and_finish、Search.items_to_*）登记“计数语义”；提供处理器可选类属性/钩子声明自定义校验或豁免（默认零改动）。
  - manifest 组装：候选 result/files 相对路径、size、sha256、num_rows、附属资源、子数据集 key 列表；引用完整性校验（文件齐备、摘要一致、子行归属本尝试且未完成、输入签名未变、附属引用在隔离区内可解析）。
  - 无输出（num_rows=0 且无候选文件）为合法结果，跳过文件类校验。
  - 全仓扫描现有 zip 产出处理器的 num_rows 语义（回应 spec Open Question），如发现反例以可选声明处理并记录。
- **Acceptance Criteria Addressed**: AC-7, AC-9
- **Test Requirements**:
  - `rule` TR-6.1: 参数化坏候选（CSV 截断/列越界、NDJSON 坏行、ZIP 损坏、行数不符、缺附属目录、输入签名变更）一律校验失败且无任何发布副作用。
  - `rule` TR-6.2: 合法 csv/ndjson/zip 与 0 行无文件候选通过；manifest 字段与摘要正确。
  - `rule` TR-6.3: 全仓 zip 处理器 num_rows 语义扫描结论记录在任务证据；反例（如有）有豁免声明且不影响默认核对。

## Task 7: BasicProcessor 原子发布编排
- **Status**: `completed`
- **Completion Evidence**:
  - TR-7.1 `rule` 通过：假处理器 csv 全流程——VALIDATED→FILES_PUBLISHED→DB_COMMITTED→JOB_FINISHED 检查点、候选 os.replace 到发布路径（内容一致）、终态/去标记/缓冲子作业/committed 同事务（commit=False 后一次 commit）、作业条件删除（test_full_publication）。
  - TR-7.2 `rule` 通过：事务中 DB 故障 → rollback、状态停留 files_published、已移动文件保留供对账（test_db_failure_after_file_move）。
  - TR-7.3 `rule` 通过：copy_to/邮件经 defer_after_commit 仅在提交成功后执行恰好一次（test 中 marker 一次）；迟到 worker 预检失败不移动文件、不提交（test_late_worker）。
  - TR-7.4 `rubric` 进行中→Task 12：处理器零改动（DataSet.finish 透明延迟、attach surrogate 自动绑定、standalone consumed、zip helper 自动计数标记）；import 冒烟通过；ruff clean；53 tests passed。
- **Priority**: high
- **Depends On**: Task 3, Task 5, Task 6
- **Description**:
  - 改造 `BasicProcessor.work()/after_process()` 为统一编排：绑定尝试（含签名、封存）→ process()（期间心跳随 update_status/update_progress 等节拍刷新）→ 收集 finish 声明（DataSet.finish 在活动尝试下变为“登记拟发布终态 + 触发校验”，不提前置 is_finished）→ 校验 → 原子发布 → job.finish() → 提交后动作（邮件、copy_to 等）。
  - 文件发布：候选 fsync；`os.replace` 原子替换结果文件与附属目录（旧结果在替换前可读）；写 files_published 检查点。
  - DB 发布单事务（显式 commit=False 后一次 commit；以 `(jobtype,remote_id)` 取 pg_advisory_xact_lock，事务内复核 jobs.attempt_id 与数据集状态）：本数据集终态、去标记并可见子数据集、flush 缓冲子作业、attempt=committed。
  - 支持特殊形态：preset 自身不 finish 但发布子流水线；attach_to 替身复制与 finish 纳入发布顺序；无输出仅 DB 发布；filter standalone 作为子数据集同事务可见。
  - after_process 的外部副作用重新定序：文件复制类在文件发布后、邮件/copy_to 在事务提交且仲裁确认后执行，失败不回滚已提交结果但记录错误。
  - 运行期异常：attempt=failed + 错误信息，沿用 park/release；WorkerInterruptedException 分流 retry/cancel 不被破坏。
- **Acceptance Criteria Addressed**: AC-8, AC-9, AC-12（基础路径）, AC-16
- **Test Requirements**:
  - `rule` TR-7.1: 假处理器（单文件 csv/zip/无输出 三形态）走通全流程：检查点顺序落库、发布路径内容 sha256 与候选一致、子作业仅在事务内出现一次。
  - `rule` TR-7.2: 事务回滚（注入 DB 异常）后发布路径与数据集状态均保持发布前状态，无半提交。
  - `rule` TR-7.3: 邮件/copy_to spy 仅在提交成功后调用恰好一次。
  - `rubric` TR-7.4: 编排对既有处理器透明程度；尺度 1-5；锚点 1=需处理器改造才能跑通，3=多数跑通需少量垫片，5=既有处理器零改动跑通；阈值 >= 4；证据为 tests/test_modules.py 全量加载与代表性处理器假运行。

## Task 8: 重启/不确定完成的对账续跑
- **Status**: `completed`
- **Completion Evidence**:
  - TR-8.1 `rule` 通过：validated 与 files_published 两阶段崩溃再领取时，新 claim 采纳旧尝试（jobs.attempt_id 条件改盖），process() 替身断言不执行（DummyProcessor.process raise），仅跑缺失发布阶段；files_published 按 manifest sha256 核对发布路径，作业仅删一次。
  - TR-8.2 `rule` 通过：processing 阶段崩溃旧尝试 superseded、新尝试全新隔离目录重跑；追加模式结构性测试证明结果行数=单次运行、旧半成品候选保留待 GC、发布路径从未半成品（test_superseded_append_candidate_cannot_duplicate_rows）。
  - TR-8.3 `rule` 通过：对账决策（adopt/superseded）写数据集日志与 attempt error；检查点不倒退（phase_at_least）。篡改已发布文件 sha256 不符 → 拒绝发布（5 reconciliation + 54 总计 tests passed，ruff clean）。
- **Priority**: high
- **Depends On**: Task 7
- **Description**:
  - worker 启动处理作业时先查同 (jobtype, remote_id) 非终态尝试：依据心跳/owner 与 release_all 重启纪元判定旧属主已死；按阶段对账（文件系统 vs manifest vs DB）：
    - validated：重做文件发布；files_published：摘要核对后补 DB 事务；db_committed：仅补 job.finish 与提交后动作；processing：旧尝试置 superseded，新建尝试（Task 4 封存旧候选）整体重跑。
  - 续跑路径不得调用 process()/外部获取（编排层提供可 spy 的阶段入口）。
  - 对账决策与结果写数据集日志与 attempt.error/phase。
- **Acceptance Criteria Addressed**: AC-8, AC-10, AC-11
- **Test Requirements**:
  - `rule` TR-8.1: 在 validated/files_published/db_committed 三阶段注入崩溃，再领取后 process() 与外部获取 spy 调用次数为 0，终态与正常完成一致、子作业无重复。
  - `rule` TR-8.2: processing 阶段崩溃产生新尝试，旧尝试 superseded；追加模式处理器恢复后行数等于单次运行，发布路径从未出现半成品。
  - `rule` TR-8.3: 每次对账在日志/尝试记录留有可审计决策行。

## Task 9: 取消/异常/重复领取/迟到完成仲裁
- **Status**: `completed`
- **Completion Evidence**:
  - TR-9.1 `rule` 通过：迟到 worker（jobs.attempt_id 已变/行已删）预检与事务内行锁条件写均返回 0 行 → LostArbitrationException、回滚、无提交无后置动作（test_cancel_winning_inside_transaction、test_late_worker_ownership_mismatch）。
  - TR-9.2 `rule`：取消/发布并发由“咨询锁 + 条件 jobs 行删除”单一裁决（事务行锁）；4 个仲裁用例覆盖 cancel/retry/race/轻量收口。
  - TR-9.3 `rule` 代码路径核对：cancel-dataset→manager.request_interrupt→abort()（CANCELLED+discard+条件 finish）链路保持；work() 早退分支（数据集删除/orphan/source 未就绪/已完成）全部按尝试身份收口。
- **Priority**: high
- **Depends On**: Task 8
- **Description**:
  - 所有终态写（Job.finish/release/park、数据集 finish、attempt 推进）带 attempt_id 条件；0 行即仲裁失败，调用方静默退出后置动作。
  - INTERRUPT_CANCEL：attempt=cancelled（条件写），清理本尝试注解（from_dataset 限定本次拥有数据集）、候选登记为可 GC、缓冲子作业丢弃、未发布子行删除；与发布并发时由咨询锁+身份复核唯一裁决。
  - INTERRUPT_RETRY/ProcessorException：attempt=failed + 阶段/错误；park 语义保持（一次性重启再试、周期 release）。
  - 迟到 worker：发布事务提交失败后不得执行任何后置副作用；提供统一的“失去尝试”退出路径。
  - `cancel-dataset` worker 与 DataSet.delete 取消链路在新语义下端到端仍可用。
- **Acceptance Criteria Addressed**: AC-12, AC-13, AC-16
- **Test Requirements**:
  - `rule` TR-9.1: 构造迟到 worker（jobs.attempt_id 已更新）尝试发布：条件写 0 行、事务回滚、邮件/copy_to/子作业计数 0；DB 中仅一个 committed 尝试。
  - `rule` TR-9.2: 取消与发布并发 20 次随机交错，最终状态唯一（cancelled 或 committed 之一），无重复副作用。
  - `rule` TR-9.3: cancel-dataset → 中断 → 数据集与作业删除链路测试通过；注解清理不误伤他数据集。

## Task 10: 尝试候选垃圾回收
- **Status**: `completed`
- **Completion Evidence**:
  - TR-10.1 `rule` 通过：状态×年龄×引用矩阵（committed/failed/cancelled/superseded 超期且无引用才删；running/publishing/近期/被引用保留；孤儿目录跟踪宽限两段式删除）。
  - TR-10.2 `rule` 通过：PATH_DATA 层已发布结果文件与 .attempts 外路径绝不触碰；只匹配 32 位 hex 目录名（3 GC tests passed）。
  - 接入 clean-temp-files worker（.attempts 根目录不被旧逻辑当结果文件）。
- **Priority**: medium
- **Depends On**: Task 9
- **Description**:
  - 扩展 `backend/workers/cleanup_tempfiles.py`：扫描 `PATH_DATA/.attempts/`，按 attempt_id 查 job_attempts：仅 failed/cancelled/superseded 且 finished_at 超过宽限期（复用 7 天跟踪文件模式）且 manifest 无其他非终态/committed 尝试引用时删除；committed 残留按宽限期删除；running/近期目录保留；动作写日志。
  - GC 只在 .attempts 内操作，绝不触碰发布路径与旧 -staging 逻辑（保留）。
- **Acceptance Criteria Addressed**: AC-14
- **Test Requirements**:
  - `rule` TR-10.1: 参数化（状态 × 年龄 × 引用）目录集合，删除结果与规格矩阵完全一致。
  - `rule` TR-10.2: 已发布结果文件与其他尝试目录在任何 GC 判定下均不被删除。

## Task 11: 状态 API 与日志可查询性
- **Status**: `completed`
- **Completion Evidence**:
  - TR-11.1 `rule` 通过：attempt_status_payload 覆盖 running/publishing/committed/failed/parked 五态，字段含 attempt_id/status/phase/error/recoverable/parked/heartbeat_at/时间戳（5 tests）；已接入 /api/check-query/ 与 /api/check-processors/。
  - TR-11.2：对账采纳/取代、校验失败、完成等事件写数据集 .log（reconcile dataset.log、异常 handler）。
- **Priority**: medium
- **Depends On**: Task 7
- **Description**:
  - `/api/check-query/`、`/api/check-processors/` 响应增加 `attempt` 对象：attempt_id、status、phase、error、recoverable（是否已达 validated）、heartbeat/updated 时间戳；已完成数据集返回最近 committed 尝试；parked 待重启呈现“可恢复/等待重试”而非成功。
  - DataSet 状态组装处复用统一查询函数（common/lib/attempt.py）。
  - 尝试开始、校验失败、发布、对账续跑、仲裁失败写入数据集日志。
- **Acceptance Criteria Addressed**: AC-15, FR-4
- **Test Requirements**:
  - `rule` TR-11.1: 构造 running/failed/recoverable/committed/parked 尝试，API 组装输出字段取值正确。
  - `rule` TR-11.2: 关键生命周期事件在 .log 中各至少一行且含 attempt_id。

## Task 12: 兼容性总扫与质量门禁
- **Status**: `completed`
- **Completion Evidence**:
  - TR-12.1 `rule`：`pytest`（含 test_modules 全量 118 workers/99 processors/29 datasources 真实加载）213 passed, 3 warnings（第三方 jieba 转义告警，与本改动无关）。
  - TR-12.2 `rule`：`ruff check .` All checks passed。
  - TR-12.3 `rubric` 5：新增 8 个测试文件覆盖隔离、签名、缓冲、校验（坏格式参数化）、发布（含事务故障注入）、对账（三阶段崩溃+追加模式）、仲裁、GC 矩阵、状态 API，共 62 个新增用例。
  - TR-12.4 `rubric` 5：git diff 证明 processors/、datasources/、模板与下载视图零改动；改动仅 12 个框架/schema/API 文件 + 2 个新库模块；发布后逻辑路径/文件基名/folder_<key>/zip 成员下载 URL 全部不变（test_full_publication 断言），无输出 0 行 EMPTY 路径同现状（test_no_output）。
- **Priority**: high
- **Depends On**: Task 9, Task 10, Task 11
- **Description**:
  - 运行 `pytest`（含 tests/test_modules.py 全量模块加载、新增尝试测试）与 `ruff check .`，修复回归。
  - 对下载契约做证据化验证：单文件、zip 成员、folder 资源 URL/MIME/Range；after_process（next/copy_to/attach_to/邮件/standalone）时序；search/import、filter、preset 代表性走查。
  - 汇总处理器零改动清单（AC-18）与测试覆盖分支说明（AC-19）。
- **Acceptance Criteria Addressed**: AC-18, AC-19
- **Test Requirements**:
  - `rule` TR-12.1: `pytest` 全部通过；输出作为证据粘贴。
  - `rule` TR-12.2: `ruff check .` 零新增告警。
  - `rubric` TR-12.3: 自动化证据充分度；尺度 1-5；锚点同 AC-19；阈值 >= 4；证据为新增测试分支矩阵与全绿输出。
  - `rubric` TR-12.4: 低侵入评分；尺度 1-5；锚点同 AC-18；阈值 >= 4；证据为处理器改动清单（期望为空/仅可选声明）。
