# go 微服务实践 — 基于 go-zero 的数字货币现货交易平台

在线体验（演示机）：http://47.113.223.16

测试账号见 [resource/users.txt](resource/users.txt)

- 后端：https://github.com/ikun2021/gex
- 前端：https://github.com/ikun2021/gex-ui

基于 go-zero 实现现货交易核心能力：

- 限价单、市价单撮合
- 行情（盘口、K 线、Tick、Ticker）与个人订单变更的实时推送

## 架构说明（精简版）

相较早期多 API / 多 RPC 拆分，当前仓库已**合并大量服务**，核心只保留四个进程：

| 服务 | 目录 | 说明 |
|------|------|------|
| **Gateway** | `app/gateway` | 统一 HTTP 入口，聚合账户、订单、行情、盘口接口 |
| **Account RPC** | `app/account/rpc` | 用户认证、资产（Redis）、订单与成交归档（MongoDB） |
| **Match** | `app/match` | 撮合引擎，消费 Pulsar 订单流，盘口与快照存 Redis |
| **Quote RPC** | `app/quote/rpc` | K 线 / Tick / Ticker，持久化 MongoDB，推送 WebSocket |

已移除或不再作为主路径的组件示例：

- 独立的 `account/api`、`order/api`、`order/rpc`
- `matchmq` + `matchrpc` 双进程（合并为 `app/match`）
- 独立的 `quoteapi`、`klinerpc`（合并为 `app/quote/rpc`，由 Gateway 转发）
- 交易链路 **MySQL + gorm/gen**、**DTM Saga**（资产冻结/扣减改为 **Redis Lua** 原子脚本）

```mermaid
flowchart LR
  Client --> Gateway
  Gateway --> AccountRpc
  Gateway --> MatchRpc
  Gateway --> QuoteRpc
  AccountRpc --> MongoDB
  AccountRpc --> Redis
  AccountRpc --> Pulsar
  Match --> Redis
  Match --> Pulsar
  QuoteRpc --> MongoDB
  QuoteRpc --> Redis
  QuoteRpc --> Pulsar
  QuoteRpc --> WS[WebSocket 推送]
```

管理后台 `app/admin/api` 仍可选部署（配置类数据，与核心交易链路解耦）。

## 中间件依赖

核心链路依赖：

- **MongoDB**：用户、订单终态、成交记录、K 线、Tick 等
- **Redis**：用户资产、会话、撮合盘口与快照、行情缓存
- **Apache Pulsar**：订单与撮合结果消息
- **etcd**：RPC 服务注册发现
- **WebSocket**（`deploy/depend/ws`）：行情与订单推送

本地开发可参考各服务 `etc/*.yaml` 中的 `MongoConf`、`RedisConf`、`Pulsar` 等配置。

> **说明**：`deploy/depend/docker-compose.yaml` 中可能仍包含 MySQL、DTM 等历史依赖容器；**当前交易与账户主数据已迁移到 MongoDB**，启动核心服务时以 MongoDB + Redis + Pulsar + etcd 为准。

## 基本功能

### 限价单

![](https://cdn.learnku.com/uploads/images/202406/10/51993/bZZs8Xnchx.gif)

### 市价单

![](https://cdn.learnku.com/uploads/images/202406/10/51993/vVNUSmI7Pp.gif)

## 运行项目

### 1. 本地编译（示例）

```shell
# 生成 Gateway 代码（修改 .api 后）
make gapi

# 生成 Account / Quote RPC（修改 .proto 后）
make accountrpc
make quoterpc

# 编译各服务（可按需调整 Makefile，当前推荐二进制）
go build -o bin/gateway ./app/gateway
go build -o bin/accountrpc ./app/account/rpc
go build -o bin/match ./app/match
go build -o bin/quoterpc ./app/quote/rpc
```

### 2. Docker Compose（可选）

依赖与业务可分别使用：

```shell
docker network create gex   # 首次需要
make dep1   # deploy/depend：MongoDB、Redis、Pulsar、etcd、nginx、ws 等
make dep2   # deploy/dockerfiles：业务容器（若镜像与 Makefile 已同步）
```

`Makefile` 中的 `build` / `run` 目标仍指向旧的多服务二进制，**与当前目录结构可能不一致**；以实际上述四个 `app/*` 入口为准。

### 3. 访问

- Gateway 默认端口见 `app/gateway/etc/gateway.yaml`（如 `8888`）
- 若使用 nginx 反代，可配置 `api.gex.com` 指向 Gateway

## Go 实践要点

### go-zero API + RPC

Gateway 使用 `.api` 定义 HTTP，通过 etcd 发现 `AccountRpc`、`MatchRpc`、`QuoteRpc`。

### MongoDB 数据模型

- 账户：`user` 集合；订单终态 `order_final`；成交 `match_trade`
- 行情：`kline_history`、`tick` 等（见 `MongoConf` 集合名配置）
- DAO 位于 `app/account/rpc/internal/dao/mongodao`、`app/quote/rpc/internal/dao`

### Redis 资产与撮合状态

- 用户资产：Hash + Lua 脚本保证冻结/解冻/扣减原子性
- 登录会话：JWT + Redis 单点（`gex:account:session:*`）
- 撮合：订单簿与引擎快照

### 认证

Gateway `Auth` 中间件校验 Token，调用 `AccountRpc.ValidateToken`，请求上下文注入 `userId`。

### 日志上报（可选）

集成 [zlog](https://github.com/ikun2021/zlog)，可将指定级别日志推送到飞书 / 企业微信 / Telegram。

## 版本与后续

- 架构持续精简：以 **Gateway + 三个 RPC/撮合服务** 为主
- 部署脚本、Makefile、docker-compose 将逐步与 MongoDB 方案对齐
- 前端完善、k8s 部署等仍在迭代中
