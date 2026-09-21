# 渐进式发布控制面（本地持久化模拟）

在本地服务内用文件持久化模拟「应用版本 / 实例组 / 流量权重」，保证蓝绿切换、
灰度放量、异常回滚在**并发**与**进程中断**下结论一致、可解释。

## 1. 运行

```bash
uv sync
uv run fastapi dev main.py          # 默认数据文件 data/rollout.json
ROLLOUT_DATA=/tmp/r.json MAX_CONCURRENT=2 MAX_STEP_WEIGHT=20 \
    uv run fastapi dev main.py
```

环境变量：`ROLLOUT_DATA`、`MAX_CONCURRENT`、`MAX_STEP_WEIGHT`、
`STEP_TIMEOUT`、`QUEUE_TIMEOUT`。

## 2. HTTP 接口

| 方法/路径 | 含义 |
| --- | --- |
| `POST /plans` | 创建计划：`plan_id, app, version, target_group, step_weight, from_version?` |
| `POST /plans/{id}/promote` | 推进一步：权重 +`step_weight`，随后做健康门禁 |
| `POST /plans/{id}/abort` | 人工中止（冻结当前对外状态） |
| `POST /plans/{id}/rollback` | 回到最近稳定点（幂等，可重复调用） |
| `GET  /plans/{id}/status` | 当前阶段、权重、对外版本、最近稳定点、门禁证据 |
| `GET  /plans/{id}/events` | 该计划的判定/阶段事件流 |
| `GET  /events` | 全部事件；`GET /healthz` 存活与活动/排队任务数 |

错误响应体 `detail.code` 可区分：`plan_not_found(404)`、
`plan_already_exists(409)`、`invalid_transition(409, 含 reason)`、
`limit_exceeded(422/429, 含 kind/limit/actual)`、`queue_timeout(503)`。

## 3. 发布状态机

```
                 create
                   │
                   ▼
              ┌─────────┐ promote(门禁通过) ┌────────────┐
              │ PENDING │ ───────────────► │ PROMOTING  │
              └─────────┘                   └─────┬──────┘
                   ▲                               │
                   │ (中断恢复 / 门禁阻塞 / 超时)   │
                   │                               ▼
              ┌─────────┐  promote(重过门禁)  ┌─────────┐
   中止/回滚  │  PAUSED │ ◄────────────────  │  weight  │
              └─────────┘                    │   +step  │
                │  │                         └─────────┘
       abort ┌──┘  └── start_rollback            │ 门禁=非critical未过 -> PAUSED,权重保持
              ▼      ▼                           │ 门禁=critical(灾难性) -> 自动回滚
        ┌─────────┐ ┌──────────────┐             │ 权重=100 且门禁过 -> COMPLETED
        │ ABORTED │ │ ROLLING_BACK │◄────────────┘
        └─────────└ ┴──────┬───────┘
                           │ 一次原子写恢复最近稳定点
                           ▼
                     ┌─────────────┐
                     │ ROLLED_BACK │ (终态, 回滚重入=无副作用)
                     └─────────────┘
  COMPLETED / FAILED 同为终态；ROLLING_BACK 失败 -> FAILED（可重试回滚）。
```

关键规则：

1. **权重不跳跃**：每次 `promote` 只增加一个配置的 `step_weight`，上限 100。
2. **不越过门禁**：权重提升到 W 后必须在 W 上通过门禁，W 才被记为稳定点；
   被阻塞后计划进入 PAUSED，再次 `promote` **先在 W 重测**，通过后下一次
   才能加到 W+step。
3. **中间态原子化**：权重 + 实例就绪状态 + 稳定点记录在同一把计划锁内完成并
   一次性整文档落盘（tmp→fsync→rename）。并发请求按计划串行化，结论与某种
   串行执行完全一致。
4. 门禁判定带**证据**：success_rate / error_rate / p99 与阈值、失败项列表、
   是否 critical 全部记录并可通过 `/status`、`/events` 查询。

门禁阈值（`HealthGate`）：
- 非灾难（任一指标不过阈值）：**暂停放量**，权重保持，不产生稳定点。
- 灾难（success_rate < 0.5 或 error_rate > 0.5）：**自动回滚**。

## 4. 回滚规则

- 稳定点：创建时记录基线点（权重 0、`from_version`、基线实例 READY）；
  此后每个**门禁通过的权重**记录一个稳定点（含整组实例快照）。
- 回滚 = 用最近稳定点的版本/权重/实例快照**原子替换**当前对外状态。
  例如已在 50 稳定、向 75 放量时触发回滚，则回到 50；第一步即灾难时回到 0。
- **幂等**：处于 ROLLED_BACK 且已位于最近稳定点时再次调用，直接返回成功，
  不递增版本号、不产生额外落盘或额外副作用（仅追加一条 no-op 观测事件）。
- 回滚与放量并发时由计划锁串行化，最终状态必然对应二者之一的合法串行顺序。
- 人工 abort 冻结当前对外可见权重（不自动回退），之后拒绝 promote/abort。

## 5. 中断恢复

- 所有状态迁移是「内存修改 → 整文档原子落盘」；崩溃只会留下旧文档或新文档，
  不会有半更新。
- 进程重启构造 `RolloutManager` 后由 API lifespan 自动调用 `recover()`
  （也可手动调用）：
  - `PROMOTING` 是瞬态：发现即转 `PAUSED`，停在**最近已落盘权重**，需要重过
    门禁才能继续（不会把未落盘的权重带入恢复）。
  - `ROLLING_BACK` 被发现说明回滚的原子写未完成；此时持久化状态本身即稳定点，
    恢复时补完到 `ROLLED_BACK`。

## 6. 持久化格式演进

文档：`{"format":"rollout-store","schema_version":2,"plans":{...}}`。

- **缺字段 -> 确定缺省**：v1 记录缺少 `policy.step_weight(=10)`、
  `reason(="")`、`history(=[])`、`version_counter(=0)`、`from_version(="")`
  等，按固定默认值迁移，默认值在代码中集中定义且跨版本稳定。
- **明确拒绝，不静默兜底**：
  - `schema_version` 高于当前支持版本；
  - 缺少必需身份字段（`plan_id/app/version/target_group`）；
  - 文件损坏/JSON 无法解析、`format` 未知、顶层结构非对象。
  统一抛 `IncompatibleData`（拒绝启动读取）。

## 7. 可配置上限与资源

`Config`：`max_concurrent_rollouts`（活动任务上限）、`queue_max`（排队上限）、
`max/min_step_weight`（步长上下限）、`step_timeout_s`（门禁评估超时）、
`queue_timeout_s`（排队超时）、`on_limit=queue|reject`。

- 超限时：`reject` 直接 429/422；`queue` 在 `queue_max` 内排队，超过排队
  超时返回 `queue_timeout(503)`，队列满返回 `queue_depth(429)`。
- 门禁评估使用一次性线程池，**每次评估后 shutdown 回收**，超时不残留线程；
  调度槽在任何路径（含超时/异常）都会释放；`shutdown()` 排空在途任务。
- 排队开始、门禁阻塞、失败、完成分别记录 `waiting/gate_blocked/failed/
  completed` 事件，日志级别可区分（WARNING/ERROR/INFO）。

## 8. 验证方法

```bash
uv run pytest -s                 # 全部用例；-s 打印每例输入与判定依据
uv run pytest tests/test_concurrent.py -v   # 并发放量串行等价
uv run pytest tests/test_rollback.py   -v   # 回滚重入/自动回滚/中止
uv run pytest tests/test_recovery.py  -v   # 中断恢复/格式演进/拒绝
uv run pytest tests/test_gates_limits.py -v # 超时/上限/排队/资源清理
```

测试重点（对应需求）：
- 12 线程并发放量同一计划：权重恰为 100、状态 completed、稳定点数量与串行
  一致，多余请求被带原因拒绝；
- 权重观测集合合法、首次出现顺序严格递增（无丢失/跳跃/越过门禁）；
- 灾难故障自动回滚、回滚重入无额外副作用（版本计数器不变）；
- 伪造 PROMOTING/ROLLING_BACK 落盘后重启恢复结论确定；
- v1 缺省迁移、未来 schema / 缺必需字段 / 损坏文件均被明确拒绝；
- 非法迁移参数化覆盖全部终态/在途态，断言可区分 `reason` 非空且状态正确。
