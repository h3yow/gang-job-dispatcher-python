# Gang Job Dispatcher（本地集群作业调度器）

一个基于 FastAPI 的、可本地运行的集群作业调度服务。提交方按资源需求提交**成组作业**，
执行方通过**租约 + 围栏（fencing token）**领取并运行作业；调度结论在并发竞争与进程重启下
保持一致、可解释。

## 1. 调度模型

### 1.1 成组作业（Gang）原子调度

- 一次提交是一个**作业组（gang）**，组内包含一个或多个作业，每个作业声明
  `resources`（单副本资源 `cpu` / `memory_mb` / `gpu`）与 `replicas`（副本数）。
- 调度是 **all-or-nothing**：组内全部作业的总需求能被集群剩余容量满足时，
  整组一次性进入 `READY`（获得资源、可被领取）；否则**整组拒绝，不分配任何资源、
  不残留任何占位作业**。
- 提交先做幂等去重、再做资源合法性校验、最后做容量核算，三步全部通过才落库，
  因此"部分分配 / 中途失败"不可能破坏原子性。

### 1.2 作业状态机

```
READY ──claim──▶ RUNNING ──report(SUCCEEDED/FAILED/KILLED)──▶ 终态
  │                 │  │
  │                 │  └─ lease 过期后被他人 claim ─▶ 仍是 RUNNING（围栏 +1，旧持有者失效）
  │                 └─ 同 worker 有效期内重复 claim：幂等返回同一租约
  └─ cancel/preempt（任意非终态）──▶ CANCELLED / PREEMPTED
```

终态：`SUCCEEDED`、`FAILED`、`KILLED`、`CANCELLED`、`PREEMPTED`。

- 终态作业**立即释放容量**，不可再被领取。
- 取消 / 抢占以**整组**为单位：非终态成员原子迁移到目标终态并撤销租约；
  已是终态的成员保持原结论不变，因此对任意状态重复调用结论都确定（幂等）。

## 2. 容量核算口径

- 集群容量在服务初始化时固定（环境变量 `CLUSTER_CPU` / `CLUSTER_MEMORY_MB` /
  `CLUSTER_GPU`，默认 64 CPU / 128 GiB / 8 GPU）。
- **单作业需求 = 单副本资源 × replicas**；
- **已用容量 = 所有非终态作业（READY + RUNNING）需求之和**；
  `SUCCEEDED/FAILED/KILLED/CANCELLED/PREEMPTED` 一律不计入。
- 三维资源（cpu、memory_mb、gpu）**独立核算**，任一维度超出剩余容量即整组拒绝，
  响应中 `violating_dims` 明确给出是哪些维度超限，并附带 `capacity / used /
  available / requested` 全量口径，判定可解释。
- 可用量查询：`GET /capacity`。

> 注意：容量在**提交成功的瞬间**就按整组需求预留（READY 即占用），领取动作本身
> 不再改变容量。这保证了"已排队但未运行"的成组作业不会被超额超卖。

## 3. 租约与围栏规则

执行方 `claim` 成功后获得一份带有效期的租约：

| 字段 | 含义 |
| --- | --- |
| `fence_token` | **围栏标识，作业维度单调递增整数**。每次新领取（含过期接管）+1 |
| `worker_id` | 当前持有者 |
| `issued_at` / `expires_at` | 颁发时间 / 过期时间（默认 TTL 30s，`LEASE_TTL_SECONDS` 可调） |

规则：

1. **禁止并发重复执行**：作业在租约有效期内只能被一个 `worker_id` 持有；
   其他执行者领取返回 `409 lease_violation`。
2. **同 worker 重试幂等**：持有者在有效期内重复 claim，返回同一个 fence_token，不发新租约。
3. **租约过期可重新分配**：`now >= expires_at` 后，任意执行者可 claim 接管，
   获得**更大的**新 fence_token；作业回到可被新持有者运行的状态。
4. **围栏失效（fencing）**：心跳 `heartbeat` 与结果上报 `result` 都必须携带当前
   fence_token。token 不匹配（持旧租约）或租约已过期，一律返回 `409`。
   因此过期后"复活"的旧执行者的心跳/结果不会污染新执行者。
5. **结果只计一次**：终态迁移的瞬间回收租约并记录"消费该结果的 fence_token"。
   - 同一个 fence_token 的重复上报：幂等返回，不重复计入；
   - 其它（更旧/更新）fence_token 的上报：`409` 拒绝。
6. **取消 / 抢占立即撤销租约**：迁移终态后旧 fence_token 立刻失效，旧持有者随后的
   心跳/上报都被拒绝。

## 4. 拒绝原因（可区分）

所有业务错误返回统一信封 `{ "error", "message", "details" }`：

| error code | HTTP | 触发场景 |
| --- | --- | --- |
| `invalid_job_submission` | 422 | 资源声明非法（负数、未知维度、replicas<1、缺 dedup_key、请求体不合法） |
| `capacity_exceeded` | 409 | 整组需求超出任一维度的剩余容量（附 `violating_dims` 与容量口径） |
| `duplicate_submission` | 409 | `dedup_key` 重复（附 `existing_gang_id`）；不会产生第二份可执行作业 |
| `lease_violation` | 409 | 租约冲突 / 过期 / fence_token 不匹配 |
| `invalid_state_transition` | 409 | 对终态作业领取等非法迁移 |
| `job_not_found` / `gang_not_found` | 404 | 对象不存在 |

## 5. 持久化与重启恢复

- 每次状态迁移（提交 / 领取 / 心跳 / 上报 / 取消 / 抢占）都在同一把锁内完成
  "检查 → 迁移 → 落盘"。
- 落盘采用 JSON 快照 + 临时文件 `os.replace` **原子替换**（默认文件
  `scheduler_state.json`，可用 `SCHEDULER_STORE` 配置），崩溃不会留下半截文件。
- 进程重启时从快照恢复：集群容量、全部作业状态、租约与 fence_token、去重索引
  完全一致（容量以快照为准，而非新的构造参数）。RUNNING 作业恢复后仍受原租约
  约束：若已过期，旧围栏被拒、可被接管。

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/gangs` | 提交成组作业（body: `jobs[]`, `gang_id?`, `dedup_key`） |
| POST | `/jobs/{job_id}/claim` | 领取作业，body: `{"worker_id": "..."}` |
| POST | `/jobs/{job_id}/heartbeat` | 续租，body: `{"fence_token": N}` |
| POST | `/jobs/{job_id}/result` | 上报结果，body: `{"fence_token": N, "result": "SUCCEEDED|FAILED|KILLED"}` |
| POST | `/gangs/{gang_id}/cancel` | 整组取消 |
| POST | `/gangs/{gang_id}/preempt` | 整组抢占 |
| GET | `/jobs/{job_id}` | 查询作业 |
| GET | `/gangs/{gang_id}` | 查询成组作业及成员 |
| GET | `/capacity` | 容量口径与占用明细 |
| GET | `/health` | 健康检查 |

## 7. 本地验证方法

### 7.1 运行单元 / 并发 / 重启测试

```bash
uv sync
uv run pytest                 # 全部测试，日志直接打印作业标识/容量变化/判定依据
```

测试文件：

- `tests/test_scheduler.py`：非法提交、容量核算与原子拒绝、重复提交、租约围栏、
  过期接管、取消抢占、重启恢复（FakeClock 精确驱动过期）。
- `tests/test_concurrency.py`：16 线程并发领取只有 1 个赢家、容量竞争下整组
  all-or-nothing、相同 dedup_key 并发只产生 1 份作业、并发上报结果只计一次。
- `tests/test_api.py`：HTTP 端到端，区分 422/409 各类拒绝原因、围栏全流程、
  经真实 JSON 文件的进程重启恢复。
- `tests/test_store.py`：快照原子写入与反序列化一致性。

### 7.2 启动真实服务并手工演练

```bash
# 小容量、短 TTL 便于观察超额与过期
CLUSTER_CPU=8 CLUSTER_MEMORY_MB=8192 CLUSTER_GPU=2 LEASE_TTL_SECONDS=2 \
  uv run uvicorn main:app --host 127.0.0.1 --port 8000
```

```bash
# 1) 提交一个成组作业
curl -s -X POST localhost:8000/gangs -H 'Content-Type: application/json' -d '{
  "gang_id":"g1","dedup_key":"batch-001",
  "jobs":[
    {"job_id":"a","resources":{"cpu":2,"memory_mb":1024,"gpu":1},"replicas":2},
    {"job_id":"b","resources":{"cpu":1,"memory_mb":512}}
  ]}'

# 2) 重复提交 -> 409 duplicate_submission（不会产生第二份）
# 3) 超额提交 -> 409 capacity_exceeded（details.violating_dims 给出超限维度）
# 4) 领取 -> 返回 fence_token
curl -s -X POST localhost:8000/jobs/a/claim -d '{"worker_id":"w1"}' -H 'Content-Type: application/json'
# 5) 持围栏上报
curl -s -X POST localhost:8000/jobs/a/result -d '{"fence_token":1,"result":"SUCCEEDED"}' -H 'Content-Type: application/json'
# 6) 查询容量
curl -s localhost:8000/capacity
```

停掉进程再用同一 `SCHEDULER_STORE` 文件重启，即可观察作业状态、容量、租约围栏与
去重索引完整恢复。

## 8. 代码结构

```
scheduler/
  errors.py     # 可区分的拒绝原因（error code + HTTP 状态码）
  models.py     # Resources / Job / Gang / Lease / Snapshot 及序列化
  store.py      # JsonSnapshotStore（原子替换）/ InMemorySnapshotStore
  scheduler.py  # 核心：原子提交、容量核算、租约围栏、取消抢占（单锁串行化）
  api.py        # FastAPI 路由、请求模型、统一错误信封
main.py         # 本地启动入口（环境变量配置容量/TTL/快照路径）
tests/          # 行为、并发、HTTP 端到端、持久化测试
```
