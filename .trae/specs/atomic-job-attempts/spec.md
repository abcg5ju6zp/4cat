# 处理器作业的原子尝试（Atomic Job Attempts）- 产品需求文档

## Overview
- **Summary**: 为 4CAT 后端作业（尤其是处理器/检索作业）引入“尝试（attempt）”机制：每次领取作业生成稳定尝试身份并绑定输入数据集版本；结果文件、附属资源、待创建子数据集先全部写入该尝试的隔离区域；经格式、条目数与引用完整性校验后，以一次原子发布替换旧结果；崩溃/不确定完成时按尝试身份对账、只补缺阶段；取消、异常、重启、重复领取与迟到完成全部按尝试身份仲裁；清理只移除确定无引用的未发布候选。
- **Purpose**: 消除“文件已在最终路径、数据库/作业状态未提交”的半结果窗口，避免重试追加重复行或覆盖另一次运行，使失败可恢复、状态可查询、清理可证明安全。
- **Target Users**: 运行检索与处理器的研究者（前端结果页/API 使用者）、4CAT 实例管理员（排障与清理）、以及依赖稳定结果文件的下游处理器。

## Goals
- 每次作业领取拥有稳定、可持久化、可仲裁的尝试身份，并记录输入数据集版本签名。
- 尝试运行期间，一切产物（结果文件、附属资源文件夹、staging 区域、运行中创建的子数据集）对发布路径与队列不可见。
- 发布前执行格式、条目数、引用完整性校验；校验失败不得发布，旧状态/旧结果不受影响。
- 发布以“文件原子替换 + 单数据库事务”完成，各阶段有持久检查点。
- 落盘、数据库提交、作业完成响应不确定时，依据原尝试对账实际状态并只续跑缺失阶段；已确认完成的外部获取/处理绝不重复执行。
- 取消、处理异常、worker 重启、重复领取、迟到完成一律按尝试身份仲裁。
- 垃圾清理只删除终态、超过宽限期且无引用的候选；绝不触碰已发布结果或后来尝试的产物。
- 错误状态与可恢复进度可通过现有状态 API 查询，并写入数据集日志。
- 无输出处理器、普通单文件结果、现有日志保留方式、成功作业的下载方式全部保持兼容。

## Non-Goals
- 不做任意处理器内部的细粒度断点续跑（如外部 API 分页获取中途的逐条续传）：在候选通过校验之前发生的崩溃，以“隔离区内整体安全重跑 process()”为恢复手段；用户已确认采用**框架级安全重跑**方案。框架保证重跑不产生重复行、不覆盖他次结果。
- 不改造前端下载路由形态、数据集 URL、结果目录的总体布局（仅在其下新增内部尝试目录）。
- 不支持跨文件系统的结果存储与发布（尝试目录位于 `PATH_DATA` 之内以保证 `os.replace` 原子性）。
- 不改变单后端守护进程 + Postgres 的部署假设，不实现多后端实例间的分布式锁（沿用现有单后端模型）。
- 不重写注解（annotations）的数据模型；处理器对父数据集写入的注解沿用现有“失败按 from_dataset 删除”的清理方式，但清理必须限定为本次尝试创建的行。
- 不新增管理员工人状态页展示（用户选择仅扩展现有状态 API）。

## Background & Context
- 作业领取：[Job.claim()](file:///Users/ding/Documents/swe/0910125/project-07/common/lib/job.py#L92-L116) 以 `timestamp_claimed=0` 条件 UPDATE 保证单领取；崩溃后 [Job.park()](file:///Users/ding/Documents/swe/0910125/project-07/common/lib/job.py#L164-L183) 置 -1，重启时 [JobQueue.release_all()](file:///Users/ding/Documents/swe/0910125/project-07/common/lib/queue.py#L200-L206) 复位。`jobs` 行无尝试身份，完成即删除（一次性作业）。
- 处理链路：[BasicProcessor.work()](file:///Users/ding/Documents/swe/0910125/project-07/backend/lib/processor.py#L116-L265) 装载输出数据集与父数据集后调用 `process()`；处理器普遍**直接写最终路径** [DataSet.get_results_path()](file:///Users/ding/Documents/swe/0910125/project-07/common/lib/dataset.py#L248-L261)（63 个处理器文件、82 处引用），[DataSet.finish()](file:///Users/ding/Documents/swe/0910125/project-07/common/lib/dataset.py#L993-L1023) 才提交 `is_finished/num_rows`；`after_process()` 还会创建 `next` 链子数据集、复制 `copy_to`、挂载 `attach_to` 替身、filter 再建 standalone 副本。文件落盘、DB 提交、作业删除分属多个时刻且无事务包裹。
- 追加写风险真实存在：如 [google_vision_api.py:157](file:///Users/ding/Documents/swe/0910125/project-07/processors/machine_learning/google_vision_api.py#L157) 以 `"a"` 模式写最终路径；崩溃重试即追加重复行。
- 数据库封装 [common/lib/database.py](file:///Users/ding/Documents/swe/0910125/project-07/common/lib/database.py#L103-L126) 每条语句默认独立提交；多表原子发布需显式 `commit=False` + 单次提交（必要时配合 Postgres 事务级咨询锁）。
- 下载按数据集完成状态间接把关（[check_dataset_finished()](file:///Users/ding/Documents/swe/0910125/project-07/common/lib/dataset.py#L230-L246)），但直接下载 URL 对存在的文件不校验完成状态，因此未完成数据集路径上的半成品文件可能被取到。
- 日志路径由结果路径派生（`.log` 后缀），staging 目录名为 `<结果文件名>-staging`；周期清理由 [cleanup_tempfiles.py](file:///Users/ding/Documents/swe/0910125/project-07/backend/workers/cleanup_tempfiles.py) 负责，带 7 天跟踪宽限。
- 现状无迁移时 VERSION 为 1.57；schema 变更须同时更新 [backend/database.sql](file:///Users/ding/Documents/swe/0910125/project-07/backend/database.sql) 并新增 `helper-scripts/migrate/migrate-1.57-1.58.py`，且受 [tests/test_schema_consistency.py](file:///Users/ding/Documents/swe/0910125/project-07/tests/test_schema_consistency.py) 正则校验约束（迁移脚本中 CREATE TABLE 覆盖新表全部列）。

## Functional Requirements

### FR-1 尝试身份与领取绑定
- 每次作业领取成功时创建一条持久化尝试记录，含全局唯一 `attempt_id`、`jobtype`、`remote_id`、属主标识（主机/PID/线程）、创建时间与心跳时间；领取所用的条件 UPDATE 同时把 `attempt_id` 盖到 `jobs` 行，领取失败（并发/重复领取）不得改写已有尝试身份。
- 一次性作业完成删除作业行后，尝试记录保留为历史；周期作业重置后再次领取生成新尝试。

### FR-2 输入数据集版本绑定
- 对处理器作业，尝试开始时为每个被读取的输入数据集（父/源数据集，含 preset 透传场景）记录版本签名：`key`、`is_finished`、`num_rows`、`timestamp_finished`、`result_file` 基名、文件大小与 mtime（无文件时记缺省），并记录作业参数快照。
- 发布前重新计算签名并比对；输入在处理期间被改动（换文件、行数/完成状态变化）时拒绝发布，尝试置失败并给出可查询的完整性错误。

### FR-3 隔离写入区域
- 每个尝试在 `PATH_DATA/.attempts/<attempt_id>/` 下拥有隔离目录（与结果目录同一文件系统）：候选结果文件、候选附属资源目录（对应 `folder_<key>`）、staging 区域、候选清单（manifest）。
- 尝试绑定期间，本次作业拥有的数据集上 `get_results_path()`、`get_results_folder_path()`、`get_staging_area()` 一律解析到隔离区域；以 key 装载的**其他**数据集（只读输入、merge/export 等引用）路径保持发布路径不变。
- 尝试开始时，若未完成数据集在发布路径上存在历史半成品文件，必须先将其移入本尝试隔离区封存（隔离且不参与发布），使发布路径在成功发布前不暴露任何本次或历史半成品。
- 尝试目录命名与内容不与现有 `<结果名>-staging` 机制冲突；旧 staging 目录继续被现有清理逻辑识别。

### FR-4 日志兼容
- 数据集日志仍写在发布路径同目录、同名 `.log` 文件；`clear_log()`/`log()` 的调用时机与行为保持现状（每次运行开始清空重建）。尝试的状态迁移额外追加到该日志。
- 尝试失败、重跑、发布等事件必须可从日志追溯（对应 4CAT 的 traceability 原则）。

### FR-5 子数据集隔离
- 尝试运行中创建的数据集行（`next` 链、filter 的 standalone、preset 挂载、处理器直接 `DataSet(...)` 创建者）必须标记为该尝试所有，且在发布前：(a) 不可被作业队列领取执行；(b) 不出现在“可运行子结果”的完成视图中；(c) 其结果文件位于同一尝试隔离区。
- 子数据集的入队（jobs 行插入）延迟到发布事务内；尝试失败/取消时，其拥有且未被外部引用的子数据集行与其候选一并清除。
- 尝试不得删除或更改其他尝试或已发布数据集拥有的子数据集。

### FR-6 发布前校验闸门
- 发布前对候选执行校验，并将结果（含大小、sha256、条目数、附属资源清单、子数据集 key 列表）写入 manifest：
  - 格式：CSV 可被解析、表头一致且无列数越界（`None` 键）；NDJSON 每行可解析为 JSON；ZIP 通过完整性测试（`testzip()`）且成员可列出。
  - 条目数：CSV 数据行数、NDJSON 行数与声明的 `num_rows` 一致；框架辅助函数（`write_archive_and_finish`、`items_to_*`）产出的归档成员数与声明一致；其他自定义产物至少通过格式完整性校验，计数语义允许处理器显式声明豁免/自定义核对。
  - 引用完整性：输入版本签名未变；manifest 列出的候选文件全部存在且摘要一致；附属资源目录存在；子数据集 key 对应行存在、归本尝试所有且均未完成；候选内部引用（归档/HTML 对 `folder_<key>` 资源的引用）可在隔离区内解析。
- 校验失败：不得移动/覆盖任何发布路径文件，数据集保持未完成，尝试置 `failed` 并记录错误，作业沿用现有 park/release 崩溃策略以支持稍后重试（新尝试）。
- 无输出处理器（`num_rows=0`、不产文件，EMPTY/部分 ERROR/WARNING 语义）免予候选文件校验，按纯数据库发布处理。

### FR-7 原子发布与阶段检查点
- 尝试状态机至少含阶段：`claimed → processing → validated → files_published → db_committed → job_finished`；每次阶段推进持久化到尝试记录（阶段、时间戳、manifest、错误）。
- 文件发布：候选 fsync 后用 `os.replace` 原子替换发布路径上的结果文件与附属目录；替换前一刻旧结果仍完整可读（用于已有旧结果的重跑场景）。
- 数据库发布：单事务完成——本数据集终态（`is_finished/num_rows/status_type/timestamp_finished/result_file`）、子数据集去标记并可见、子作业行插入、尝试置 `committed`；事务以 `(jobtype, remote_id)` 串行化（如 Postgres 咨询锁），并在事务内复核当前尝试身份仍是本尝试。
- 作业终结：事务提交成功后再 `job.finish()`；任一终态写库都必须是带尝试身份条件的条件写，影响行数为 0 即表示仲裁失败。

### FR-8 对账与只补缺
- 领取作业时若发现同一 `(jobtype, remote_id)` 存在非终态尝试（重启后、重复领取、不确定完成），先对账文件系统与数据库的实际状态：
  - 已 `validated` 未发布：从文件发布续跑；
  - 已 `files_published` 未提交：用 manifest 摘要核对发布路径文件，相符则直接进入数据库事务，否则重做文件替换；
  - 已 `db_committed` 作业未终结：只补 `job.finish()` 与发布后动作；
  - 停在 `processing`（候选未验证、不可证明完整）：将旧尝试置 `superseded`，开启新尝试整体重跑。
- 对账续跑路径不得再次调用 `process()` 或重新执行外部获取；以测试替身/spy 可证明处理函数零次调用。
- 新尝试与被取代尝试目录互不重叠；被取代候选在 GC 前保留但永不发布。

### FR-9 取消、异常、重启、迟到完成的仲裁
- **取消**（INTERRUPT_CANCEL）：尝试置 `cancelled`，清理本次尝试创建的注解/候选/未发布子数据集，作业按现有语义 finish；若与发布并发，以 FR-7 的事务内身份复核裁决，败者放弃全部后置动作。
- **处理异常**：尝试置 `failed` 并记录错误与阶段，沿用现有 park（一次性作业重启再试、周期作业 release）策略；半成品不出隔离区。
- **worker 重启**：`release_all()` 后首个领取者负责 FR-8 对账；心跳/属主信息仅用于辅助判断，不作为正确性唯一依据。
- **重复领取**：领取条件 UPDATE 与尝试盖章同一语句，保证同一时刻只有一个当前尝试。
- **迟到完成**：失去身份的旧 worker 继续执行时，其条件写影响 0 行、发布事务提交失败；它必须在不再产生副作用（不发邮件、不复制 `copy_to`、不入队子作业）的情况下退出；其候选成为无引用候选交由 GC。

### FR-10 安全垃圾清理
- 扩展 `clean-temp-files` worker（或同文件内新增尝试 GC 逻辑）：仅当尝试处于 `failed/cancelled/superseded` 终态、终态时间超过宽限期（复用现有 7 天跟踪模式）、且 manifest 资源不被任何非终态/已提交尝试引用时，才删除其隔离目录。
- `committed` 尝试的残留空目录/清单按宽限期清理；已发布路径文件永不被该机制删除；清理动作写日志并可审计。
- 处理器运行结束时的即时清理（`remove_disposable_files`/for_cleanup）只清理本次尝试的 staging，不删除候选与已发布文件。

### FR-11 状态与错误可查询
- 扩展 `/api/check-query/` 与 `/api/check-processors/` 返回：当前/最近尝试的 `attempt_id`、状态（running/publishing/committed/failed/cancelled/superseded）、阶段、错误消息、是否可无损恢复（是否已到 `validated` 检查点）、时间戳。
- 数据集状态/日志在失败时呈现尝试错误（现有 `status`/日志通道，不新增页面）。
- 作业被 park 等待重启时，状态 API 仍能返回“可恢复/等待重试”而不是伪装成功或空白。

### FR-12 兼容性
- 无输出处理器：0 行、无文件时终态与下载行为同现状（EMPTY/WARNING/ERROR 语义不变）。
- 普通单文件结果：成功后发布路径基名、URL、MIME、Range/条件请求行为不变；zip 及其附属 folder 资源的下载与内联访问不变。
- 成功作业：一次性作业行删除、周期作业重置、`after_process` 的 `next`/`copy_to`/`attach_to`/邮件/standalone 等外部可观察行为保持时序与结果一致。
- 检索（search/import）作业、filter、preset 无需修改处理器代码即可获得同等保证。

## Non-Functional Requirements
- **NFR-1 低侵入**：现有约 200+ 处理器/数据源原则上零改动；隔离与发布通过 `DataSet`/`BasicProcessor`/`Job` 框架层透明接管；确需处理器配合的仅限可选项（自定义校验声明）。
- **NFR-2 性能**：隔离区与发布目录同文件系统，发布仅一次 rename + 一次事务；常规处理器无可感知的额外 I/O（不强制全量哈希，仅在校验/检查点时计算一次摘要）。
- **NFR-3 可测试**：新逻辑可在无 Postgres/无网络的单元测试中用 mock 覆盖（对齐 `tests/test_modules.py` 的 mock 风格），核心状态机与对账逻辑不依赖真实 worker 线程。
- **NFR-4 可观测**：关键迁移、尝试开始、校验失败、发布、对账续跑、仲裁失败、GC 均有日志。
- **NFR-5 代码规范**：Python 缩进使用 Tab；Python ≥ 3.11；通过 `ruff check .`（遵循 ruff.toml 的规则集）。

## Constraints
- **Technical**: Postgres（可用事务、咨询锁、JSON/JSONB）、POSIX 文件语义（同 FS `os.replace` 原子）、Flask 前端不改下载契约；DB 语句默认 autocommit，原子发布需显式事务。
- **Business**: 不破坏进行中/历史数据集；迁移可在 Docker 启动时自动执行且幂等；不引入新的第三方依赖。
- **Dependencies**: `backend/database.sql`、`helper-scripts/migrate/migrate-1.57-1.58.py`、`VERSION`、`common/lib/{job,queue,dataset,database}.py`、`backend/lib/{worker,processor,manager}.py`、`backend/workers/cleanup_tempfiles.py`、`webtool/views/api_tool.py`。

## Assumptions
- 同一数据集同一时刻只有一个后端守护进程操作其作业（现有架构即如此）；跨实例并发不在范围内。
- `PATH_DATA` 为本地/同一文件系统卷；结果文件与 `.attempts` 目录不跨挂载点。
- 处理器写入的外部副作用（向第三方 API 发请求、写 `copy_to` 外部路径、发邮件）在“处理期崩溃整体重跑”模型下可能重复外部请求——这是现状已存在的行为；框架只保证框架管理的产物与发布幂等。发布后动作（邮件、copy_to）在仲裁确认提交后才执行，不重复。
- 子数据集在运行期创建的情况集中在框架辅助路径（`after_process`、`copy()`、preset）；极少数处理器直接创建 DataSet 的场景通过 DataSet 构造层统一打标记覆盖。

## Acceptance Criteria

### AC-1: 领取即绑定稳定尝试身份
- **Type**: `rule`
- **Given**: 队中存在可领取作业
- **When**: 某 worker 领取成功
- **Then**: `job_attempts` 新增一行（唯一 attempt_id、jobtype、remote_id、属主、时间戳），且同一条件 UPDATE 把 attempt_id 盖到 jobs 行；并发领取抛 JobClaimedException 且不改写 attempt_id
- **Pass Condition**: 单元测试中两次领取仅一次成功；尝试行与 jobs.attempt_id 一致；失败者查询不到自己的尝试写入
- **Evidence**: `tests/` 新增尝试/作业测试输出；数据库检查 SQL 结果

### AC-2: 尝试绑定输入数据集版本签名
- **Type**: `rule`
- **Given**: 带父数据集的处理器作业
- **When**: 尝试开始
- **Then**: 尝试记录包含每个输入数据集的签名（key/is_finished/num_rows/timestamp_finished/result_file/size/mtime）与参数快照；发布前若任一签名变化，发布被拒并记录完整性错误
- **Pass Condition**: 测试分别改动 mtime/大小/num_rows/完成状态后发布均失败且未触碰发布路径；未改动时正常通过
- **Evidence**: 单元测试断言尝试记录字段与失败错误类型

### AC-3: 产物在发布前全部位于隔离区
- **Type**: `rule`
- **Given**: 处理器正在运行
- **When**: 通过 `get_results_path()/get_results_folder_path()/get_staging_area()` 写文件
- **Then**: 全部落于 `PATH_DATA/.attempts/<attempt_id>/`；发布路径上不存在本次候选；以 key 装载的只读输入数据集仍解析发布路径
- **Pass Condition**: 测试中断于处理中段，断言发布路径无新文件、隔离区文件齐全、输入数据集路径未被重定向
- **Evidence**: 单元测试文件系统断言（tmp PATH_DATA）

### AC-4: 历史半成品在新尝试开始时被隔离封存
- **Type**: `rule`
- **Given**: 未完成数据集的发布路径上残留半成品（含升级前遗留）
- **When**: 新尝试绑定该数据集
- **Then**: 残留文件被移入尝试隔离区封存，发布路径不再暴露它；该封存文件不会被当作候选发布
- **Pass Condition**: 测试中发布路径文件消失、隔离区存在封存副本、成功发布内容仅来自本次候选
- **Evidence**: 单元测试断言

### AC-5: 日志路径与保留方式兼容
- **Type**: `rule`
- **Given**: 任意处理器尝试
- **When**: 运行、失败、重跑
- **Then**: 日志仍为发布路径同名 `.log`；`clear_log()` 时机不变；尝试关键事件（开始/校验失败/发布/对账/仲裁）追加到该日志
- **Pass Condition**: 测试断言 `get_log_path()` 不被重定向，且事件行可在日志中找到
- **Evidence**: 单元测试断言

### AC-6: 子数据集在发布前隔离、失败时清除
- **Type**: `rule`
- **Given**: 运行中创建子数据集（next/standalone/preset/直接创建）
- **When**: 发布尚未发生
- **Then**: 子数据集行标记尝试归属、其作业不可被领取、完成视图不可见、其文件在隔离区；尝试失败/取消时这些行、作业、候选被清除；其他尝试/已发布子数据集不被触碰
- **Pass Condition**: 测试断言发布前队列为子作业返回不可领取；失败后子行与候选不存在；预置的“他尝试”子行完好
- **Evidence**: 单元测试断言

### AC-7: 校验闸门拒绝坏候选
- **Type**: `rule`
- **Given**: 候选存在格式损坏/条目数不符/引用缺失/输入签名变化
- **When**: 进入发布前校验
- **Then**: 发布中止，发布路径与旧结果不变，数据集未完成，尝试 failed 且错误可查询
- **Pass Condition**: 参数化测试覆盖坏 CSV（列越界/截断）、坏 NDJSON、坏 ZIP、行数不符、缺失附属目录，全部不产生发布副作用
- **Evidence**: 单元测试参数化用例结果

### AC-8: 校验通过后原子发布且阶段可对账
- **Type**: `rule`
- **Given**: 候选通过校验
- **When**: 发布执行
- **Then**: 阶段依次推进 validated→files_published→db_committed→job_finished；文件经 os.replace 替换（旧文件在替换前可读）；数据集终态、子数据集可见、子作业、尝试 committed 在单事务内提交；之后作业行删除/重置
- **Pass Condition**: 测试断言每个检查点落库；发布路径内容与候选摘要一致；作业终态正确；模拟各阶段崩溃后状态自洽
- **Evidence**: 单元测试 + 检查点记录

### AC-9: 无输出处理器兼容
- **Type**: `rule`
- **Given**: 处理器产出 0 行且不创建结果文件
- **When**: 完成
- **Then**: 不要求候选文件，发布只提交数据库终态（EMPTY/既定 ERROR/WARNING 语义），下载行为与现状一致
- **Pass Condition**: 测试以 0 行处理器走完全流程，终态与无文件下载行为同现状
- **Evidence**: 单元测试断言

### AC-10: 不确定完成时只补缺、不重处理
- **Type**: `rule`
- **Given**: 尝试分别崩在 validated 后、files_published 后、db_committed 后
- **When**: 作业被再次领取
- **Then**: 对账后仅执行缺失阶段；`process()` 与外部获取替身调用次数为 0；最终结果与正常完成一致且无重复子作业
- **Pass Condition**: spy 统计 process/外部获取调用为 0；子作业无重复行；终态一致
- **Evidence**: 单元测试 spy 计数与 DB 断言

### AC-11: 处理期崩溃产生新尝试且不追加重复行
- **Type**: `rule`
- **Given**: 候选未验证时 worker 死亡（含追加模式处理器）
- **When**: 重启后重新领取
- **Then**: 旧尝试 superseded，新尝试在全新隔离目录重跑；发布结果行数恰为一次运行的行数；发布路径从未出现半成品
- **Pass Condition**: 以追加模式处理器模拟崩溃-恢复，结果行数等于单次运行；旧候选目录仍在但无引用
- **Evidence**: 单元测试行数与文件断言

### AC-12: 取消按尝试身份仲裁
- **Type**: `rule`
- **Given**: 作业运行中收到 INTERRUPT_CANCEL，或取消与发布并发
- **When**: 取消处理执行
- **Then**: 尝试置 cancelled（或发布事务因身份复核拒绝取消/拒绝发布二者居一且状态自洽）；本尝试候选与未发布子数据集清除；作业终结；败者无邮件/复制/入队等后置副作用
- **Pass Condition**: 并发测试后终态唯一确定、无重复副作用、数据集删除链路（cancel-dataset）仍可用
- **Evidence**: 单元测试断言

### AC-13: 迟到完成与重复领取无副作用
- **Type**: `rule`
- **Given**: 旧 worker 在作业已被新尝试领取/提交后继续执行
- **When**: 它尝试发布与终结
- **Then**: 条件写影响 0 行、事务回滚；无邮件、无 copy_to、无子作业；committed 状态仅属于最新尝试
- **Pass Condition**: 测试构造迟到 worker，断言副作用计数为 0 且 DB 中只有一个 committed 尝试
- **Evidence**: 单元测试断言

### AC-14: 垃圾清理只动终态无引用候选
- **Type**: `rule`
- **Given**: 多种尝试目录（running/committed/failed/cancelled/superseded，新旧不一）与已发布结果
- **When**: clean-temp-files 运行
- **Then**: 仅超过宽限期的 failed/cancelled/superseded 无引用目录被删；running/近期/committed 在宽限内保留；发布路径文件与他尝试目录绝不删除；动作有日志
- **Pass Condition**: 参数化测试覆盖每种状态/年龄组合的删除判定
- **Evidence**: 单元测试断言目录存留集合

### AC-15: 状态 API 暴露尝试状态与可恢复性
- **Type**: `rule`
- **Given**: 数据集存在进行中/失败/可恢复/已完成尝试
- **When**: 调用 `/api/check-query/` 与 `/api/check-processors/`
- **Then**: 响应包含 attempt_id、status、phase、error、recoverable、时间戳；parked 等待重启时呈现可恢复状态而非成功/空白；已完成数据集可查到最近 committed 尝试
- **Pass Condition**: API 响应字段在各状态下取值正确（测试构造 DataSet/尝试记录并调用视图函数或其组装逻辑）
- **Evidence**: 单元测试断言响应 JSON

### AC-16: 作业语义与下载契约兼容
- **Type**: `rule`
- **Given**: 成功的一次性作业、周期作业、单文件/zip+folder 结果
- **When**: 正常完成
- **Then**: 一次性作业行删除、周期作业重置 attempts/claim；下载 URL、文件基名、zip 成员与 folder 资源访问同现状；after_process 的 next/copy_to/attach_to/邮件/standalone 外部效果不缺失不重复
- **Pass Condition**: 测试与手工验证清单确认以上行为
- **Evidence**: 单元测试 + 浏览器/接口验证记录

### AC-17: 数据库迁移与全新安装一致
- **Type**: `rule`
- **Given**: 1.57 旧库与空库
- **When**: 执行 migrate 与全新初始化
- **Then**: `jobs.attempt_id`、`datasets.attempt_id`、`job_attempts` 表在两路径下结构一致；迁移幂等（重复执行不报错）；VERSION=1.58
- **Pass Condition**: `pytest tests/test_schema_consistency.py` 通过；迁移脚本重复运行成功；database.sql 含新结构
- **Evidence**: pytest 输出与迁移脚本执行输出

### AC-18: 框架低侵入适配既有处理器生态
- **Type**: `rubric`
- **Dimension**: 对既有处理器/数据源代码的侵入程度与透明接管质量
- **Scale**: 1-5
- **Anchors**: 1 = 需要大面积修改处理器或引入破坏性 API 变更；3 = 少量处理器需修改且有兼容垫片；5 = 处理器零改动，路径重定向/发布/对账全部由框架透明完成，自定义校验仅为可选扩展
- **Pass Threshold**: >= 4
- **Evidence**: 全仓处理器改动文件清单（期望为空或仅可选扩展）；`tests/test_modules.py` 全量加载通过

### AC-19: 测试与质量门禁
- **Type**: `rubric`
- **Dimension**: 隔离/校验/发布/对账/仲裁/清理的自动化证据充分度
- **Scale**: 1-5
- **Anchors**: 1 = 仅手工验证；3 = 主路径有单测但崩溃/仲裁分支缺失；5 = 状态机各分支、崩溃注入、仲裁、GC 均有自动化单测，且 pytest 与 ruff 全绿
- **Pass Threshold**: >= 4
- **Evidence**: `pytest` 全量输出、`ruff check .` 输出、新增测试文件与覆盖分支说明

## Open Questions
- [ ] 自定义归档（非框架 helper 产出的 zip）的“条目数 == num_rows”默认是否可能误伤个别处理器：实现阶段需全仓扫描 num_rows 语义；若存在反例，以处理器可选声明（自定义校验/豁免）解决，不改本规格默认行为。
- [ ] 心跳 stale 阈值（重启对账时辅助判断旧属主死亡）的具体秒数在实现中确定；正确性不依赖该阈值（重启以 release_all 纪元为准）。
