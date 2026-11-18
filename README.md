# 告警值班控制台（FastAPI + SQLite + Docker Compose）

值班工程师维护服务时暂停新故障通知，同时保证通知端不会留下"已恢复但没通知"的故障。
系统由两个容器组成：

| 服务 | 端口 | 职责 |
| --- | --- | --- |
| `alert-app` | 8000 | 告警控制台：事件接入、静默管理、告警/通知状态查询、发件箱 worker |
| `notifier` | 8001 | 本地通知端：接收通知、模拟丢回执、按通知号去重、展示最终状态 |

## 快速启动

```bash
docker compose up --build
# 控制台:   http://localhost:8000/
# 通知端:   http://localhost:8001/
```

`LOSE_FIRST_N=1` 表示每个**新通知号**的首次投递返回 HTTP 500（回执丢失，消息已落库），
应用随后用**同一个通知号**重试，通知端去重后返回成功。设为 0 可关闭模拟。

无 Docker/PyPI 的环境可用标准库 runner 启动（业务逻辑与 FastAPI 版本完全共用 `common/`）：

```bash
ALERT_PORT=8000 NOTIFIER_PORT=8001 \
NOTIFIER_URL=http://127.0.0.1:8001 \
ALERT_DB_PATH=./data/alerts.db NOTIFIER_DB_PATH=./data/notifier.db \
python3 -I runner/__main__.py
```

## 事件接入 API

`POST /api/events`

```json
{ "fingerprint": "hostA/disk", "seq": 1, "kind": "firing", "summary": "disk 92%" }
```

- `kind`：`firing`（触发）或 `recovery`（恢复）
- `(fingerprint, seq)` 唯一；`seq` 在同指纹内严格递增

### 幂等与顺序规则

| 场景 | 行为 |
| --- | --- |
| 同指纹同序同文重试 | 返回 `duplicate_replayed`，沿用原结果，不产生新通知 |
| 同指纹同序异文 | HTTP 409 `conflicting_event`，拒绝 |
| 旧序号（`seq <= current_seq`） | HTTP 409 `stale_event`，当前状态不被改写 |
| 无告警时的恢复 | HTTP 409 `stale_event` |
| 恢复后再次触发 | 轮次（round）+1，视为新一轮，重新走触发/恢复通知 |

## 静默规则

- `POST /api/silence`，body：`{"duration_seconds": 60, "reason": "维护"}`
  （也支持绝对时间 `until_ts`）；`DELETE /api/silence` 立即解除；`GET /api/silence` 查询。
- 静默期内的新触发**只更新告警**（序号/摘要），不产生通知。
- **此前已通知故障的恢复不受静默影响**，恢复通知照常入发件箱。
- 静默到期（或手动解除）后，worker 只补发**仍在触发且本轮从未通知**的故障；
  静默期内已经恢复的轮次保持静默，不会补发。
- 每次静默的**实际生效区间**都落库（`silence_intervals`）：续期合并为同一连续区间，
  提前解除以解除时刻截止，自然到期以 `until_ts` 截止，供停机核算豁免使用。

## 可用性预算（非计划停机核算）

值班工程师在告警网页（或 API）按指纹设置**容许停机秒数**，系统据此核算最近 24 小时
预算是否耗尽。

| 方法/路径 | 说明 |
| --- | --- |
| `PUT /api/budgets` | body `{"fingerprint": "hostA/disk", "allowed_seconds": 600}`，设置/重置预算，自当前时刻重新核算 |
| `GET /api/budgets` | 全部预算核算结果 |
| `GET /api/budgets?fingerprint=hostA/disk` | 单指纹核算；未设置返回 404 |

核算口径（全部基于服务端**接收时间**，不接受客户端时间戳）：

- 自预算设置时刻起算；**设置时已触发的故障从设置时刻算**，更早的历史不计入。
- 已恢复故障计 `触发接收 → 恢复接收`；**未恢复故障计至每次查询时刻**，随查询增长。
- 统计窗口为最近 24 小时（`max(set_at, now-24h)` 起），跨越窗口的故障只截取窗内部分。
- 静默**实际生效区间**与故障区间的重叠部分计入 `exempt_seconds` 并从停机中扣除；
  静默续期、提前解除、自然到期、故障跨越静默边界都按真实区间计算。
- 响应字段：`downtime_seconds`（已计入停机）、`exempt_seconds`（静默豁免）、
  `remaining_seconds = allowed_seconds − downtime_seconds`、`exhausted`（剩余 ≤ 0），
  以及每个故障轮次的明细 `rounds`。

一致性与持久化：

- 计入时长、豁免时长、剩余预算在**同一个串行化事务**内，从同一份已提交状态读取；
  事件接入、静默修改与查询交错时不会读到混合状态。
- 预算写入失败（含非法载荷、约束错误、提交失败）时事务回滚，**旧预算仍可正常查询**。
- 预算与静默区间均在 SQLite 持久化，**进程重启后继续核算**。

## 通知投递与去重

- 通知号格式：`{fingerprint}#r{round}:{kind}`，同一轮的 firing/recovery 各一个稳定号码，
  重试永远复用该号码。
- 发件箱表（`notifications`）状态机：`pending → in_transit → delivered`，
  失败按 `attempts` 退避重试（1s 起，上限 30s）。
- **同指纹严格按状态顺序送达**：前一条未 delivered，后一条不会发出。
- 通知端以通知号为主键去重：首次见到的号码落库；同号重试返回 `duplicate: true`，
  只累加 `attempts_seen`；同号异文返回 409。
- 两个 SQLite 库都挂载在 Docker volume，重启后发件箱继续投递、通知端记录保留，
  不漏发、不重复；迟到的旧序号事件无法覆盖恢复后的状态。

## 其他 API

| 方法/路径 | 说明 |
| --- | --- |
| `GET /api/alerts` | 告警列表（含当前轮 firing/recovery 通知状态）+ 当前静默状态 |
| `GET /api/alerts/{fingerprint}/events` | 单指纹事件历史 |
| `GET /api/notifications` | 发件箱明细（状态、尝试次数、错误、入队/送达时间） |
| `GET /api/notifications`（通知端 8001） | 通知端收到的最终状态 |
| `PUT /api/budgets` / `GET /api/budgets` | 停机预算设置与核算（详见“可用性预算”） |
| `GET /health` | 健康检查 |

## 目录结构

```
common/        # 框架无关的核心逻辑（事件、静默、发件箱、通知端去重）+ SQLite schema
app/           # FastAPI 告警控制台（后台 worker 线程）
notifier/      # FastAPI 本地通知端（丢回执模拟）
runner/        # 纯标准库 HTTP runner，复用 common，供无 PyPI 环境运行与自测
web/           # 两个网页（原生 HTML/CSS/JS，2s 自动刷新）
tests/e2e.py   # 端到端关键链路自测（真实 HTTP + SQLite，无第三方依赖）
Dockerfile / Dockerfile.notifier / docker-compose.yml
```

## 自测

```bash
python3 -I tests/test_budget.py   # 停机核算单元测试（注入时钟，覆盖 24h 窗口/静默续期/提前解除等）
python3 -I tests/e2e.py           # 端到端关键链路（真实 HTTP + SQLite，无第三方依赖）
```

覆盖：健康检查与页面/静态资源、幂等/冲突/旧序拒绝、触发恢复全链路、丢回执同号重试、
静默期抑制触发但恢复照发、静默到期仅补发仍触发告警、恢复后再触发新一轮、
同指纹顺序投递、进程重启持久化不重不漏、非法载荷校验、
停机预算设置/查询/耗尽/重置换时、静默实际生效区间豁免（含续期与提前解除）、
预算写入失败旧值可查、预算跨重启继续核算。
