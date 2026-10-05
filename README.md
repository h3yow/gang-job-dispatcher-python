# Gang Job Dispatcher

基于 FastAPI 的本地可运行集群作业调度服务：提交方按资源需求提交（成组）作业，
执行方领取并运行，调度结论在并发与重启场景下保持一致且可解释。

## 调度模型

- **作业（Job）**：声明 `job_id`、可选 `group_id`、单副本资源（`cpu`、`memory_mb`）
  与副本数 `replicas`。作业需求 = 单副本资源 × 副本数。
- **状态机**：`PENDING → READY → LEASED → SUCCEEDED/FAILED/CANCELLED`。
  提交成功后整组进入 `READY`；被领取后进入 `LEASED`；上报成功或取消后进入终态；
  上报失败时若还有剩余尝试次数则回到 `READY` 重试，达到上限才进入 `FAILED`。
- **成组（Gang）原子调度**：一次 `POST /jobs` 提交的一组作业要么全部通过校验与
  容量核算并整体进入 `READY`，要么整体拒绝。校验（非法声明、重复 ID）先于容量
  检查，容量检查通过前不写入任何状态，因此失败的组不会残留占位资源。
- **并发控制**：所有调度决策在单个进程内锁内串行完成，且先落盘（SQLite）后生效；
  并发领取同一作业时只有一个执行方能获得租约。

## 容量核算口径

- 集群容量固定（`main.py` 中配置，默认 8 CPU / 16384 MB）。
- **占用 = 所有非终态作业的需求之和**（`READY` 与 `LEASED` 均占用容量）。
- 作业进入终态（成功/失败/取消）时释放其需求；租约过期、抢占与**失败重试**
  **不释放**容量，作业回到 `READY` 继续占位，因此重试期间不会发生超卖；
  容量只在进入终态时释放且恰好释放一次，不会被重复计入。
- **续约不触碰容量**：续约只延长租约有效期，占用口径不变。
- 重启后从 SQLite 重建占用，口径不变。

## 租约与围栏（Lease & Fencing）

- 执行方 `POST /claim` 领取作业时获得租约：含有效期（默认 30s）与**单调递增的
  围栏令牌（fencing token）**。令牌由持久化计数器分配，重启后仍然递增。
- 租约过期后作业自动回到 `READY` 可被他人领取，新租约携带更大的令牌。
- 上报结果必须携带当前租约的令牌：持旧令牌/错误令牌上报被拒绝
  （`STALE_FENCING_TOKEN`）；对终态作业重复上报被拒绝（`ILLEGAL_STATE`），
  结果不会被重复计入。
- **取消**（`POST /jobs/{id}/cancel`）在任意状态下都有确定结论：非终态作业转为
  `CANCELLED` 并释放容量、作废旧租约；已终态则为幂等 no-op。
- **抢占**（`POST /jobs/{id}/preempt`）作废当前租约并将作业重新排队（容量仍保留），
  旧执行方此后的上报一律被围栏拒绝。

## 续约（Renew）

- 执行方在租约有效期内 `POST /jobs/{id}/renew`，携带 `{worker_id, fencing_token}`，
  把本次领取的有效期顺延一个租约时长（从当前时刻重新计算），长作业因此不会因
  运行过久被判过期。
- 续约**只延长有效期**：围栏令牌（本次领取的身份）不变，容量占用不变。
- 续约会校验调用者是否仍是当前这次领取的持有人（worker_id 与令牌都要匹配）。
  若租约已过期、已被回收并交给其他执行方、或作业已进入终态，续约失败并返回
  **`LEASE_LOST`**——该结论与 `STALE_FENCING_TOKEN`、`ILLEGAL_STATE` 等其它拒绝
  原因可区分，执行方收到后应停止在该作业上的工作。

## 重试与尝试上限（Retry & max_attempts）

- 每次成功领取 = 一次全新尝试：`attempts` 加 1，并签发新的、更大的围栏令牌，
  因此本次领取的身份与上一次可区分；持旧身份（旧令牌）上报一律被
  `STALE_FENCING_TOKEN` 拒绝。
- 上报失败时：若 `attempts < max_attempts`，作业回到 `READY` 等待重新领取，
  容量保持预留；否则进入终态 `FAILED` 并释放容量。结果不会被重复计入
  （终态后再上报被 `ILLEGAL_STATE` 拒绝），容量也不会被重复占用或提前释放。
- `max_attempts` 在提交时按作业指定（默认 3，必须 ≥ 1，否则整组提交被
  `INVALID_RESOURCES` 拒绝）。
- **成组重试共进退**：组内任一成员上报失败时，重试决策（回 `READY` 或终态
  `FAILED`）原子地应用到组内所有非终态成员——要么整组一起重新可被领取
  （其他成员的未决租约同时被围栏作废），要么整组一起结束并各自释放容量，
  不会出现一部分在重试、另一部分仍在旧尝试上的中间态。已成功的成员的结果
  保持有效、不被重复计入。
- 尝试次数与上限持久化在 SQLite 中，重启后重试预算不丢失。

## 拒绝原因（可区分）

| reason | 含义 |
|---|---|
| `INVALID_RESOURCES` | 资源声明非法（cpu/mem ≤ 0、replicas < 1、空 ID、空批次） |
| `DUPLICATE_JOB` | 作业 ID 已存在或批内重复；重复提交不会产生第二份可执行作业 |
| `INSUFFICIENT_CAPACITY` | 整组需求超出当前可用容量 |
| `UNKNOWN_JOB` | 作业不存在 |
| `STALE_FENCING_TOKEN` | 令牌与当前租约不符（旧租约/伪造令牌） |
| `ILLEGAL_STATE` | 状态不允许该操作（如对终态作业重复上报） |
| `LEASE_LOST` | 续约时本次领取已失效：租约过期、已被他人领取或作业已终态；执行方应停止工作 |

## HTTP 接口

- `POST /jobs` — 提交一组作业（gang），返回 `{accepted, reason}`；
  每个作业可带 `max_attempts`（默认 3）
- `POST /claim` — `{worker_id}` → `{job, fencing_token}`
- `POST /jobs/{id}/renew` — `{worker_id, fencing_token}` 续约当前租约
- `POST /jobs/{id}/report` — `{fencing_token, success, result}`
- `POST /jobs/{id}/cancel`、`POST /jobs/{id}/preempt`
- `GET /jobs`（含 `attempts`/`max_attempts`）、`GET /capacity`

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest -q        # 运行全部测试（并发领取、容量竞争、租约过期、续约、
                        # 围栏失效、重试与上限、成组重试原子性、非法提交、重启恢复）
uv run pytest -q -s     # -s 可看到调度日志：作业标识、容量变化与判定依据
uv run pytest tests/test_renew.py tests/test_retry.py -q -s  # 只看续约/重试用例
uv run python main.py   # 启动服务（默认 127.0.0.1:8000，状态存于 dispatcher.db）
```

示例：

```bash
curl -X POST localhost:8000/jobs -H 'content-type: application/json' -d \
  '{"jobs":[{"job_id":"j1","group_id":"g","cpu":2,"memory_mb":512,"replicas":2}]}'
curl -X POST localhost:8000/claim -H 'content-type: application/json' -d '{"worker_id":"w1"}'
curl -X POST localhost:8000/jobs/j1/renew -H 'content-type: application/json' -d \
  '{"worker_id":"w1","fencing_token":<token>}'
curl -X POST localhost:8000/jobs/j1/report -H 'content-type: application/json' -d \
  '{"fencing_token":<token>,"success":true,"result":"done"}'
```

## 代码结构

- `dispatcher/models.py` — 资源、作业（含尝试次数/上限）、租约与状态机定义
- `dispatcher/store.py` — SQLite 持久化（作业、租约、围栏计数器、尝试计数），支撑重启恢复
- `dispatcher/scheduler.py` — 容量核算、成组原子调度、租约/围栏、续约、重试与成组重试、取消/抢占
- `dispatcher/api.py` — FastAPI 路由
- `tests/` — 提交校验、租约围栏、续约、重试与成组原子性、并发、重启恢复与 API 端到端测试
