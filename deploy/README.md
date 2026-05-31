# GEX 部署说明

## 架构概览

| 服务 | 说明 | 端口 |
|------|------|------|
| `gateway` | HTTP API 网关 | 8888 |
| `accountrpc` | 账户、下单、撮合结果结算 | 20002 |
| `match` | 撮合引擎（消费 `input_match_*`，产出 `output_match_*`） | 20003 |
| `quoterpc` | 行情、K 线、深度 | 20011 |
| `adminapi` | 管理后台 | 20015 |

基础设施（`deploy/depend/docker-compose.yaml`）：Pulsar、Redis、Etcd、MongoDB、WebSocket、Nginx。

宿主机端口（避免与本地默认端口冲突，nginx 仍为 80）：

| 服务 | 宿主机端口 |
|------|------------|
| Pulsar | 16650 / 18080 |
| Pulsar Manager | 19527 / 17750 |
| MongoDB | 37017（mongo） |
| Redis | 16379 |
| Etcd | 12379 / 12380 |
| WS Proxy | 20067 / 20068 |
| WS Socket | 19992 |
| Nginx | 80 |

## 快速启动

```bash
# 1. 编译 Linux 二进制
make build
make pre

# 2. 一键启动（基础设施 + 业务）
make run
```

或分步：

```bash
docker network create gex  # 若不存在
docker compose -f deploy/depend/docker-compose.yaml up -d
# 初始化 Etcd / Pulsar 后
docker compose -f deploy/dockerfiles/docker-compose.yaml up -d --build
```

## 配置

Docker 环境统一使用 `app/*/etc/*.prod.yaml`（中间件地址为 `gex-etcd`、`gex-redis` 等容器名）。

本地开发仍使用各模块 `app/*/etc/*.yaml`。

## Pulsar Topic

- `persistent://public/trade/input_match_{SYMBOL}`
- `persistent://public/trade/output_match_{SYMBOL}`

## 已移除的旧服务

以下模块已从部署中移除（代码目录若不存在可忽略旧 Dockerfile）：

- `accountapi` / `orderapi` / `orderrpc`（由 `gateway` + `accountrpc` 承担）
- `matchmq` / `matchrpc`（合并为 `match`）
- `quoteapi` / `klinerpc`（合并为 `quoterpc`）
