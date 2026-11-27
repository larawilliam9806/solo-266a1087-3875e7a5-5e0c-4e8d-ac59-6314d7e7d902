# 设备参数库：主库 + 查询副本（FastAPI + 双 SQLite）

标定员改完一批设备的偏移量后，复测端需要明确读到**某一次提交**。本服务提供：

- 多台设备的改动**一次提交**，返回一个单调递增的**提交号**；
- 主库与查询副本各用一个独立 SQLite 文件（WAL 模式）；
- 副本按**提交顺序回放每笔原始改动**，可限制每轮最多追几笔；
- 参数与回放进度**同一个事务**落库，崩溃不会出现「参数已改、进度没动」；
- 按设备编号批量查询，可带**最低提交号**与**最长等候时间**，返回参数、数据来源和同次读取所见的提交号；
- 一条 `docker compose up` 启动主库 + 副本两个容器。

## 查询路由语义

对 `GET /devices/params?device_id=...&min_commit=N&max_wait_ms=T`：

1. 副本水位（已回放提交号）≥ `min_commit` → **直接读副本**，即使主库已有更新也不追新；
2. 副本没达到 → 轮询等待，最长等 `max_wait_ms` 毫秒；**等候期间主库写入和副本回放照常进行**；
3. 到时副本仍没达到 → 兜底查主库，主库达到则返回 `source=primary`；
4. 两边都没达到 → HTTP 200 + `ready=false`（未就绪），不返回半截数据。

每次读取在**一只读事务**里同时取水位和参数，因此 `snapshot_commit` 与返回参数是同一个一致快照。

## 目录结构

```
app/
  config.py            # 环境变量配置
  db.py                # SQLite 表结构、提交写入、按序回放、一致快照
  schemas.py           # Pydantic 模型
  client.py            # 副本 -> 主库 异步 HTTP 客户端（按事件循环持有连接）
  replay.py            # 回放线程：私有事件循环 + 每轮 max_catchup 上限
  routers/primary.py   # 主库：写入 / 内部回放接口 / 兜底快照
  routers/replica.py   # 副本：等待 + 路由查询
  main.py              # 应用工厂，按 APP_ROLE 装配
tests/                 # 13 个 pytest 用例（进程内 ASGI 直连，无需起服务）
docker-compose.yml     # primary:8000 + replica:8001，各自独立卷
```

## docker compose 启动

```bash
docker compose up --build
# 主库   http://localhost:8000
# 副本   http://localhost:8001
```

关键环境变量（见 `docker-compose.yml`）：

| 变量 | 角色 | 说明 | 默认 |
| --- | --- | --- | --- |
| `APP_ROLE` | 两者 | `primary` / `replica` | `primary` |
| `DB_PATH` | 两者 | SQLite 文件路径 | `./data/primary.db` |
| `PRIMARY_URL` | 副本 | 主库地址 | `http://primary:8000` |
| `REPLICA_MAX_CATCHUP` | 副本 | **一轮最多回放几笔提交**，≤0 不限 | `5` |
| `REPLICA_POLL_INTERVAL` | 副本 | 轮询主库间隔秒数 | `0.2` |
| `REPLICA_LAG` | 副本 | 每轮回放后额外等待秒数，方便演示滞后 | `0` |

## API

### 主库

`POST /commits` — 多设备一起提交，返回提交号：

```bash
curl -X POST http://localhost:8000/commits \
  -H 'content-type: application/json' \
  -d '{"changes":[
    {"device_id":"DEV-1","params":{"offset":1.1}},
    {"device_id":"DEV-2","params":{"offset":2.2,"gain":1.0}}
  ]}'
# {"commit":1,"change_count":2}
```

同一提交内 `device_id` 不可重复、`params` 不可为空（422）。

### 副本查询

```bash
curl "http://localhost:8001/devices/params?device_id=DEV-1&device_id=DEV-2&min_commit=1&max_wait_ms=1000"
```

就绪响应：

```json
{
  "ready": true,
  "source": "replica",
  "snapshot_commit": 3,
  "min_commit": 1,
  "devices": {
    "DEV-1": {
      "device_id": "DEV-1",
      "params": {"offset": {"value": 1.2, "commit": 2}}
    }
  }
}
```

- `source`：`replica`（副本达到最低号，可能滞后于主库）/ `primary`（超时兜底）/
  `none`（两边都未达到）/ `unavailable`（兜底时主库不可达，HTTP 502）；
- `snapshot_commit`：同次读取所见的提交号（副本为回放水位，主库为最大提交号）；
- 每个参数带自己的 `commit`，即该参数最后一次被改动的提交号。

未就绪响应：

```json
{"ready":false,"source":"none","snapshot_commit":0,"min_commit":99,
 "devices":null,"reason":"primary_and_replica_both_below_min_commit"}
```

### 观测

- `GET /progress`：主库返回 `latest_commit`；副本额外返回 `replayed_commit`、`lag`；
- `GET /health`：容器健康检查；
- 交互式文档：`/docs`（OpenAPI）。

## 数据模型

| 表 | 作用 |
| --- | --- |
| `commit_log(seq, created_at, change_cnt)` | 提交号主表，`seq` 即提交号 |
| `change_log(seq, seq_ord, device_id, params)` | 每笔**原始改动**（JSON 原样保存，保留提交内顺序） |
| `param_current(device_id, param_name, value, seq)` | 每设备每参数的当前值与最后改动提交 |
| `replay_progress(id=1, last_seq)` | 副本回放水位，与参数同事务更新 |

回放只允许 `seq = last_seq + 1`，断序直接报错；重复拉取的提交幂等跳过。

## 本地开发（无 Docker）

```bash
pip install -r requirements-dev.txt
pytest -q                      # 13 个用例
python scripts/demo.py         # 本机起两个 uvicorn 跑完整场景
```
