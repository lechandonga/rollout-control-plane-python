# 渐进式发布控制面（rollout-control-plane）

基于 FastAPI 的本地渐进式发布控制面：用本地 JSON 持久化模拟应用版本、
实例组与流量权重，支持蓝绿式切换、灰度放量、健康门禁、失败自动回滚与
人工中止，并支持一次覆盖多个实例组的协同发布（批次），在并发与进程
中断场景下保证一致且可解释的结论。

## 运行

```bash
uv sync
uv run uvicorn main:app --port 8000
```

存储文件默认为 `rollout_state.json`，可用环境变量 `ROLLOUT_STORAGE` 覆盖。

## 发布状态机

```
PENDING  --start-->  RUNNING  --advance-->  RUNNING   (每步门禁通过后推进)
PENDING  --abort-->  ABORTED
RUNNING  --succeed-->  SUCCEEDED            (权重到达 100)
RUNNING  --rollback-->  ROLLING_BACK  --finish_rollback-->  ROLLED_BACK
RUNNING  --abort-->    ROLLING_BACK  --finish_rollback-->  ROLLED_BACK
```

- 其余迁移一律非法，返回 `illegal_transition`（HTTP 409），并附可区分
  的说明（如 `plan is in a terminal state`）。
- 每次状态变更立即原子落盘（tmp 文件 + fsync + os.replace），刷新或
  中断不会留下半更新状态。

## 放量与健康门禁

- 计划的 `steps` 为严格递增、以 100 结尾的权重序列；相邻步长不得超过
  `max_step_weight_delta`（默认 100，可配置收紧）。
- 每一步推进前评估门禁：
  - 未上报指标 → `WAITING`，拒绝推进；
  - `error_rate` 或 `unhealthy_ratio` 超阈值 → `BLOCKED`，拒绝推进；
  - 门禁未通过时**不产生任何状态变更**，权重与实例状态不会出现丢失、
    跳跃或越过门禁的中间态。
- 门禁通过后权重单调推进，并把该步记录为新的**稳定点**。

## 多组协同发布（批次）

一次发布可覆盖多个实例组：`POST /batches` 创建批次，每个成员组有独立
的 `version` 与 `steps`（放量节奏），批次内组名不得重复。批次为每个组
生成一个成员计划（`batch_id` 指向批次），批次状态机与单计划一致。

- **整批推进（lockstep）**：`POST /batches/{id}/advance` 先对全体未完成
  成员做门禁判定，**任一组未通过则整批不动**——其余组不会各自扩量到
  无法回收的位置；全部通过才把所有成员一起推进一步，并在**同一次原子
  落盘**中持久化。各组按各自节奏前进，步数少的组先到 100。
- **整批回滚**：`POST /batches/{id}/rollback` 把所有成员一起恢复到各自
  最近稳定点。两阶段落盘：先整批标记 `ROLLING_BACK`（中断可续做），再
  统一恢复稳定点并置 `ROLLED_BACK`，单次原子落盘；观测接口持批次锁，
  对外**不会看到「部分组已回滚、部分未回滚」的中间状态**。已
  `SUCCEEDED` 的成员本就停在自己的稳定点（100），保持原状。
- **幂等与并发**：批次回滚重复触发为 no-op；并发触发由批次锁串行化，
  与串行执行等价，不产生额外副作用。批次的所有变更（推进/回滚）在
  并发下同样与某种串行顺序一致。
- **成员计划不可绕过批次**：对批次成员计划直接调用
  start/advance/rollback/abort/run 会被拒绝（`illegal_transition`，
  附 `managed by batch` 说明），整批语义只能经批次接口操作。
- **组冲突**：同一实例组上不允许同时存在两条相互冲突的进行中发布
  （单组计划或批次）。创建计划/批次时若目标组被占用，返回
  `group_conflict`（HTTP 409），消息中指明占用者（plan/batch id）；
  占用方到达终态后组自动释放。
- **统一观测**：`GET /batches/{id}` 返回每个成员当前的步进、流量权重、
  门禁判定结论与依据，以及整批统一结论（`overall_verdict` /
  `overall_basis`：任一成员 BLOCKED 则整批 BLOCKED，否则任一 WAITING
  则 WAITING，否则 PASS）。
- **后台自动放量**：`POST /batches/{id}/run` 与单计划共用
  `max_concurrent_tasks` 上限与 `overflow_policy`；门禁阻塞超过
  `step_timeout_seconds` 时**整批**自动回滚（`gate_timeout`）。

## 回滚规则

- 回滚恢复到**最近一个稳定点**（最后一次成功放量步的权重与版本）；
  从未放量过的计划回滚到权重 0。
- 回滚是**幂等**的：已 `ROLLED_BACK` 的计划重复触发为 no-op；并发重复
  触发与串行执行等价，不产生额外副作用。
- 人工中止（abort）：`PENDING` 直接终止为 `ABORTED`；`RUNNING` 走回滚
  路径回到稳定点，`failure_reason=aborted`。
- 后台自动放量（`POST /plans/{id}/run`）中，门禁阻塞超过
  `step_timeout_seconds` 自动回滚，`failure_reason=gate_timeout`。

## 并发与中断恢复

- 同一计划的所有变更由条纹锁串行化：N 个并发 `advance` 的结果与某种
  串行顺序完全一致（成功数 == 实际推进步数）。
- 后台任务数受 `max_concurrent_tasks` 限制，超限按 `overflow_policy`
  `reject`（HTTP 429）或 `queue` 处理；任务结束必定释放信号量与线程，
  不泄漏后台资源。
- 进程重启后 `recover()`：
  - 批次 `ROLLING_BACK`（整批回滚被中断）→ 所有成员一起续做到
    `ROLLED_BACK`，不留只回滚了一半的对外状态；
  - `ROLLING_BACK`（回滚被中断）→ 重新执行到 `ROLLED_BACK`；
  - `RUNNING` 但组状态与稳定点不一致（半更新）→ 修复回稳定点；
  - 计划与批次数据本身不丢失。

## 持久化格式演进

- 存储文件顶层 `format_version` 当前为 v3（接受 v1/v2/v3）；计划记录
  当前为 v3（接受 v1/v2/v3）；批次记录自 v1 起步。
- v1 计划缺少的字段（`gate`、`stable`、`failure_reason`、组权重等）
  加载时赋予确定缺省（门禁阈值 0.05/0.2，稳定点权重 0）。
- v2 计划缺少的 `batch_id` 缺省为 `None`（独立单组计划）；v1/v2 存储
  文件缺少的 `batches` 字段缺省为空批次列表。
- 旧数据与单组计划的兼容范围：升级前创建的单组计划加载后按**原单组
  语义**继续工作（推进、门禁、回滚、恢复均不变）。
- 未知或更高的版本、损坏的 JSON → `storage_corrupt`（HTTP 500），
  **明确拒绝**而不是静默按默认值继续。

## 可观测性

`GET /plans/{id}` 返回当前阶段与判定依据：

```json
{
  "status": "RUNNING", "current_weight": 10, "stable_weight": 10,
  "gate_verdict": "BLOCKED",
  "gate_basis": "error_rate 0.500 > max 0.050",
  "failure_reason": null, "group_state": {"version": "v2", "weight": 10}
}
```

日志按 `plan <id> gate verdict=... basis=...` / `rolled back ...` /
`recover: ...` 区分等待、门禁阻塞、失败与完成。

## HTTP 接口与错误码

| 接口 | 说明 |
| --- | --- |
| `POST /plans` | 创建计划（version/group/steps/gate） |
| `POST /plans/{id}/start` | 启动 |
| `POST /plans/{id}/advance` | 推进一步（先过门禁） |
| `POST /plans/{id}/run` | 后台自动放量至完成或超时回滚 |
| `POST /plans/{id}/rollback` | 回滚（幂等） |
| `POST /plans/{id}/abort` | 人工中止 |
| `GET /plans/{id}` / `GET /plans` | 观测 |
| `POST /batches` | 创建多组批次（members: group/version/steps/gate） |
| `POST /batches/{id}/start` | 启动批次 |
| `POST /batches/{id}/advance` | 整批推进一步（全成员门禁先行） |
| `POST /batches/{id}/run` | 批次后台自动放量 |
| `POST /batches/{id}/rollback` | 整批回滚（幂等） |
| `POST /batches/{id}/abort` | 批次人工中止 |
| `GET /batches/{id}` / `GET /batches` | 批次观测（成员明细 + 统一结论） |
| `POST /groups/{g}/health` | 上报健康指标 |

错误码：`plan_not_found` 404、`illegal_transition`/`gate_blocked`/
`group_conflict` 409、`validation_error` 422、`concurrency_limit` 429、
`storage_corrupt` 500。

## 验证方法

```bash
uv run pytest -v -s   # -s 可看到每个用例打印的输入与判定依据
```

测试覆盖：

- `test_service.py::test_concurrent_advance_is_serializable` — 8 线程并发
  放量，结果与串行一致；
- `test_rollback_reentrant_no_side_effects` — 回滚重入（含 6 线程并发）
  无副作用；
- `test_recovery_after_crash_mid_rollout` /
  `test_recovery_finishes_interrupted_rollback` — 中断恢复；
- `test_illegal_transitions_have_distinct_reasons` — 非法迁移原因可区分；
- `test_gate_blocks_and_waits` — 门禁 WAITING/BLOCKED 不产生中间态；
- `test_concurrency_limit_reject` — 并发上限拒绝且无资源泄漏；
- `test_batch.py::test_batch_advance_lockstep_and_observe` — 多组按各自
  节奏整批推进，逐组判定依据与整批统一结论可查；
- `test_batch_gate_block_stops_all_groups` — 任一组被门禁挡下时整批不动；
- `test_batch_rollback_restores_all_groups` /
  `test_batch_rollback_reentrant_and_concurrent` — 整批回滚到各自稳定点，
  重复与并发触发无副作用；
- `test_group_conflict_distinct_reason` — 同组冲突发布返回
  `group_conflict`，终态后释放；
- `test_recovery_finishes_interrupted_batch_rollback` /
  `test_recovery_repairs_half_advanced_batch` — 批次中断恢复；
- `test_legacy_v2_storage_without_batches_loads` /
  `test_legacy_v1_plan_still_rolls_back` — 旧数据兼容加载并按单组语义
  继续工作；`test_incompatible_batch_record_rejected` — 不兼容批次记录
  明确拒绝；
- `test_storage.py` — 原子写往返、v1 缺省、不兼容格式明确拒绝。
