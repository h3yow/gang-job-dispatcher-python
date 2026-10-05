# Gang Job Dispatcher

基于 FastAPI 的本地可运行集群作业调度服务：提交方按资源需求提交（成组）作业，
执行方领取并运行，调度结论在并发与重启场景下保持一致且可解释。

## 调度模型

- **作业（Job）**：声明 `job_id`、可选 `group_id`、单副本资源（`cpu`、`memory_mb`）
  与副本数 `replicas`。作业需求 = 单副本资源 × 副本数。可选 `max_attempts`
  （尝试上限，默认 3，见「重试与尝试上限」）。
- **状态机**：`PENDING → READY → LEASED → SUCCEEDED/FAILED/CANCELLED`。
  提交成功后整组进入 `READY`；被领取后进入 `LEASED`；上报成功或取消后进入终态；
  上报失败时若尝试次数未达上限则回到 `READY` 等待重试，达上限才进入 `FAILED`。
- **成组（Gang）原子调度**：一次 `POST /jobs` 提交的一组作业要么全部通过校验与
  容量核算并整体进入 `READY`，要么整体拒绝。校验（非法声明、重复 ID）先于容量
  检查，容量检查通过前不写入任何状态，因此失败的组不会残留占位资源。
- **并发控制**：所有调度决策在单个进程内锁内串行完成，且先落盘（SQLite）后生效；
  并发领取同一作业时只有一个执行方能获得租约。

## 容量核算口径

- 集群容量固定（`main.py` 中配置，默认 8 CPU / 16384 MB）。
- **无组作业**：非终态即占用其需求；进入终态（成功/失败/取消）时释放。
- **成组作业（相同 `group_id`）**：整组的预留从准入起持有到**组内所有成员都
  进入终态**才释放。单个成员成功不释放其份额——这样组重试时无需重新获取容量，
  不存在"先释放再被他人占用、重试时超卖"的窗口。
- **重试期间容量不变**：失败重试只是状态在 `LEASED → READY` 间迁移，需求一直被
  预留，不重复占用、也不提前释放；只有整组（或单个无组作业）真正到达终态时才
  释放一次。
- **续约不动容量**：续约只延长租约过期时间，容量核算完全不变。
- 租约过期与抢占**不释放**容量，作业回到 `READY` 继续占位，等待重新分配。
- 重启后从 SQLite 重建占用，口径不变。

## 租约与围栏（Lease & Fencing）

- 执行方 `POST /claim` 领取作业时获得租约：含有效期（默认 30s）与**单调递增的
  围栏令牌（fencing token）**。令牌由持久化计数器分配，重启后仍然递增。
- 租约过期后作业自动回到 `READY` 可被他人领取，新租约携带更大的令牌。
- **续约**（`POST /jobs/{id}/renew`，携带 `{fencing_token}`）：执行方在运行期间
  周期性续约，把本次租约的过期时间向后顺延一个租约周期。只有当前租约的持有者
  能续约成功；续约**不改变**本次领取的身份（令牌、执行方不变），也**不影响**
  容量核算。若这次领取已被回收（租约过期后被别人领走、被抢占）或作业已结束，
  续约返回 `LEASE_LOST`——一个与其他拒绝原因明确区分的结论，执行方收到后应
  立即停止该作业的执行。
- 上报结果必须携带当前租约的令牌：持旧令牌/错误令牌上报被拒绝
  （`STALE_FENCING_TOKEN`）；对终态作业重复上报被拒绝（`ILLEGAL_STATE`），
  结果不会被重复计入。
- **取消**（`POST /jobs/{id}/cancel`）在任意状态下都有确定结论：非终态作业转为
  `CANCELLED` 并释放容量、作废旧租约；已终态则为幂等 no-op。
- **抢占**（`POST /jobs/{id}/preempt`）作废当前租约并将作业重新排队（容量仍保留），
  旧执行方此后的上报一律被围栏拒绝。

## 重试与尝试上限

- 每次领取都是一次**全新尝试**：`attempt` 计数加一并分配新的、更大的围栏令牌，
  与上一次领取的身份明确区分；持旧令牌上报/续约一律被拒。
- 上报失败时：若 `attempt < max_attempts`，作业回到 `READY` 可被重新领取；
  达到上限才真正进入 `FAILED` 终态。`max_attempts` 在提交时按作业指定
  （`POST /jobs` 的 `max_attempts` 字段，≥ 1），不指定则取默认值 3。
- 无论重试多少次，结果只计入一次（终态后重复上报被拒），容量只预留一份、
  且只在到达终态时释放一次（见「容量核算口径」）。
- **成组共进退**：组内任一成员失败触发重试时，**整组**（含已成功的成员，
  已取消的成员除外）一起回到 `READY` 重新排队，在运行中的成员租约被当场作废；
  任一成员耗尽尝试上限时，**整组**一起进入 `FAILED`。不会出现组内一部分在
  重试、另一部分已结束的中间态。重试判定以失败成员自身的
  `attempt/max_attempts` 为准。

## 拒绝原因（可区分）

| reason | 含义 |
|---|---|
| `INVALID_RESOURCES` | 资源声明非法（cpu/mem ≤ 0、replicas < 1、空 ID、空批次） |
| `DUPLICATE_JOB` | 作业 ID 已存在或批内重复；重复提交不会产生第二份可执行作业 |
| `INSUFFICIENT_CAPACITY` | 整组需求超出当前可用容量 |
| `UNKNOWN_JOB` | 作业不存在 |
| `STALE_FENCING_TOKEN` | 令牌与当前租约不符（旧租约/伪造令牌） |
| `ILLEGAL_STATE` | 状态不允许该操作（如对终态作业重复上报） |
| `LEASE_LOST` | 续约时本次领取已失效（作业结束/被回收/已转交他人），执行方必须停止 |

## HTTP 接口

- `POST /jobs` — 提交一组作业（gang），可选 `max_attempts`，返回 `{accepted, reason}`
- `POST /claim` — `{worker_id}` → `{job, fencing_token}`
- `POST /jobs/{id}/renew` — `{fencing_token}`，续约当前租约
- `POST /jobs/{id}/report` — `{fencing_token, success, result}`
- `POST /jobs/{id}/cancel`、`POST /jobs/{id}/preempt`
- `GET /jobs`、`GET /capacity`

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest -q        # 运行全部测试（并发领取、容量竞争、租约过期与续约、
                        # 围栏失效、重试与尝试上限、成组重试、非法提交、重启恢复）
uv run pytest -q -s     # -s 可看到调度日志：作业标识、容量变化与判定依据
uv run python main.py   # 启动服务（默认 127.0.0.1:8000，状态存于 dispatcher.db）
```

示例：

```bash
curl -X POST localhost:8000/jobs -H 'content-type: application/json' -d \
  '{"jobs":[{"job_id":"j1","group_id":"g","cpu":2,"memory_mb":512,"replicas":2}]}'
curl -X POST localhost:8000/claim -H 'content-type: application/json' -d '{"worker_id":"w1"}'
curl -X POST localhost:8000/jobs/j1/report -H 'content-type: application/json' -d \
  '{"fencing_token":<token>,"success":true,"result":"done"}'
```

## 代码结构

- `dispatcher/models.py` — 资源、作业、租约与状态机定义（含尝试计数与上限）
- `dispatcher/store.py` — SQLite 持久化（作业、租约、围栏计数器），支撑重启恢复
- `dispatcher/scheduler.py` — 容量核算、成组原子调度、租约/围栏、续约、
  有界重试（成组共进退）、取消/抢占
- `dispatcher/api.py` — FastAPI 路由
- `tests/` — 提交校验、租约围栏与续约、重试、并发、重启恢复与 API 端到端测试
