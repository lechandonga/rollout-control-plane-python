# 渐进式发布控制面（rollout-control-plane）

基于 FastAPI 的本地渐进式发布控制面：用本地 JSON 持久化模拟应用版本、
实例组与流量权重，支持蓝绿式切换、灰度放量、健康门禁、失败自动回滚与
人工中止，并在并发与进程中断场景下保证一致且可解释的结论。

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

## 回滚规则

- 回滚恢复到**最近一个稳定点**（最后一次成功放量步的权重与版本）；
  从未放量过的计划回滚到权重 0。
- 回滚是**幂等**的：已 `ROLLED_BACK` 的计划重复触发为 no-op；并发重复
  触发与串行执行等价，不产生额外副作用。
- 人工中止（abort）：`PENDING` 直接终止为 `ABORTED`；`RUNNING` 走回滚
  路径回到稳定点，`failure_reason=aborted`。
- 后台自动放量（`POST /plans/{id}/run`）中，门禁阻塞超过
  `step_timeout_seconds` 自动回滚，`failure_reason=gate_timeout`。

## 多组协同发布（批次）

一次发布可覆盖多个实例组：`POST /batches` 创建批次，每个目标组有各自
的 `version`、`steps`（放量节奏）与可选 `gate`。批次复用与单组计划相同
的状态机（PENDING/RUNNING/SUCCEEDED/ROLLING_BACK/ROLLED_BACK/ABORTED）。

推进规则：

- `POST /batches/{id}/advance` 先对**每一组**分别做健康门禁判定，
  全部通过才推进；任一组 `WAITING`/`BLOCKED` 时整批**不产生任何状态
  变更**——其余组不得继续各自扩量到无法回收的位置。
- 门禁全过后，各未完成的组分别推进一步（各组步数可不同，已到 100 的
  组保持不动），批次状态与所有组权重在**同一把锁内一次原子落盘**，
  对外只能看到推进前或推进后的一致快照。
- 全部组到达 100 时批次进入 `SUCCEEDED`。

整批回滚规则：

- `POST /batches/{id}/rollback` 把**所有组一起**恢复到各自的最近稳定
  点；批次状态与全部组状态单次原子落盘，对外不会出现「一部分组已
  回滚、另一部分还没回滚」的中间状态。
- 回滚幂等且可重入：已 `ROLLED_BACK` 重复触发为 no-op；并发重复触发
  与串行执行等价，不产生额外副作用。
- 后台整批放量（`POST /batches/{id}/run`）中门禁阻塞超过
  `step_timeout_seconds` 时整批自动回滚，`failure_reason=gate_timeout`。

观测：`GET /batches/{id}` 返回每组的当前步、流量权重、门禁判定结论与
依据（`gate_verdict`/`gate_basis` 为实时判定，`last_verdict`/`last_basis`
为最近一次推进时的判定），以及整批统一结论 `conclusion`。

组冲突：

- 同一实例组上不允许同时存在两条进行中的发布（单组计划或批次）。
  创建时与启动时都会检查，冲突返回 `group_conflict`（HTTP 409），
  与参数非法（422）、状态机错误（409 illegal_transition）可区分。
- 发布到达终态（SUCCEEDED/ROLLED_BACK/ABORTED）后，该组可被新发布
  重新使用。

## 并发与中断恢复

- 同一计划的所有变更由条纹锁串行化：N 个并发 `advance` 的结果与某种
  串行顺序完全一致（成功数 == 实际推进步数）。
- 后台任务数受 `max_concurrent_tasks` 限制，超限按 `overflow_policy`
  `reject`（HTTP 429）或 `queue` 处理；任务结束必定释放信号量与线程，
  不泄漏后台资源。
- 进程重启后 `recover()`：
  - `ROLLING_BACK`（回滚被中断）→ 重新执行到 `ROLLED_BACK`（批次则
    整批一起完成，不留半回滚状态）；
  - `RUNNING` 但组状态与稳定点不一致（半更新）→ 修复回稳定点（批次
    对所有偏离组一次性原子修复）；
  - 计划与批次数据本身不丢失。

## 持久化格式演进

- 存储文件顶层 `format_version` 当前为 v3（新增 `batches`），计划为
  v2，批次为 v1；读取兼容顶层 v1/v2/v3。
- 旧文件缺失的内容有确定缺省：v1/v2 存储没有 `batches` → 空批次集合；
  v1 计划缺少 `gate`/`stable`/`failure_reason` → 门禁阈值 0.05/0.2、
  稳定点权重 0。旧的单组计划加载后按原单组语义继续工作。
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
| `POST /batches` | 创建多组批次（targets: group/version/steps/gate） |
| `POST /batches/{id}/start` | 启动批次 |
| `POST /batches/{id}/advance` | 整批推进一步（全部门禁通过才原子生效） |
| `POST /batches/{id}/run` | 后台自动整批放量，超时整批回滚 |
| `POST /batches/{id}/rollback` | 整批回滚（幂等、原子） |
| `POST /batches/{id}/abort` | 中止批次 |
| `GET /batches/{id}` / `GET /batches` | 批次观测（每组进度 + 整批结论） |
| `POST /groups/{g}/health` | 上报健康指标 |

错误码：`plan_not_found`/`batch_not_found` 404、
`illegal_transition`/`gate_blocked`/`group_conflict` 409、
`validation_error` 422、`concurrency_limit` 429、`storage_corrupt` 500。

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
- `test_storage.py` — 原子写往返、v1 缺省、不兼容格式明确拒绝；
- `test_batch.py` — 多组协同发布：
  - `test_batch_advance_per_group_progress_and_overall_conclusion` —
    各组独立节奏推进与整批结论观测；
  - `test_batch_gate_block_stops_whole_batch` — 任一组被挡下时整批
    不产生状态变更；
  - `test_batch_rollback_restores_all_groups_and_is_reentrant` /
    `test_batch_rollback_atomic_snapshot_via_storage` — 整批回滚原子
    一致、重复与并发触发无副作用；
  - `test_group_conflict_between_plan_and_batch` — 计划/批次互相冲突
    且原因可区分（`group_conflict`）；
  - `test_recovery_finishes_interrupted_batch_rollback` /
    `test_recovery_repairs_diverged_batch_group` — 批次中断恢复；
  - `test_legacy_v2_storage_without_batches_loads` /
    `test_legacy_v1_plan_defaults_still_apply` /
    `test_incompatible_batch_format_rejected` — 旧数据兼容加载与
    不兼容明确拒绝；
- `test_api.py::test_batch_flow_via_api` /
  `test_batch_conflict_via_api` — 批次 HTTP 端到端与冲突错误码。
