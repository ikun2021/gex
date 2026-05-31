#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

if ! docker network inspect gex >/dev/null 2>&1; then
  docker network create gex
  echo "网络 gex 创建成功"
fi

lang='50006: 超过最小精度
100001: 内部错误
100002: 内部错误
100003: 内部错误
100004: 参数错误
100005: 记录未找到
100006: 重复数据
100007: 内部错误
100009: 内部错误
100010: 内部错误
100011: 内部错误
100012: 验证码错误
200001: 用户不存在
200002: 用户余额不足
200003: token验证失败
200004: token到期
200005: 账户密码验证失败
500001: 订单未找到
500002: 订单已经成交或已经取消
500003: 市价单不允许手动取消
500004: 订单簿没有买单
500005: 订单簿没有卖单
500006: 超过币种最小精度'

coin_ikun='coinid: 1
coinname: IKUN
prec: 4'

coin_usdt='coinid: 4
coinname: USDT
prec: 4'

coin_eth='coinid: 3
coinname: ETH
prec: 4'

symbol_ikun_usdt='symbolname: IKUN_USDT
symbolid: 1
basecoinname: IKUN
basecoinid: 1
quotecoinname: USDT
quotecoinid: 4
baseCoinPrec: 4
quoteCoinPrec: 4'

symbol_eth_usdt='symbolname: ETH_USDT
symbolid: 2
basecoinname: ETH
basecoinid: 3
quotecoinname: USDT
quotecoinid: 4
baseCoinPrec: 4
quoteCoinPrec: 4'

echo ">>> 启动基础设施 (Pulsar / Redis / Etcd / Mongo / WS / Nginx)..."
docker compose -f deploy/depend/docker-compose.yaml up -d

echo ">>> 等待依赖就绪..."
sleep 30

echo ">>> 写入 Etcd 配置..."
docker exec etcd /usr/local/bin/etcdctl put language/zh-CN -- "$lang"
docker exec etcd /usr/local/bin/etcdctl put Coin/IKUN -- "$coin_ikun"
docker exec etcd /usr/local/bin/etcdctl put Coin/USDT -- "$coin_usdt"
docker exec etcd /usr/local/bin/etcdctl put Coin/ETH -- "$coin_eth"
docker exec etcd /usr/local/bin/etcdctl put Symbol/IKUN_USDT -- "$symbol_ikun_usdt"
docker exec etcd /usr/local/bin/etcdctl put Symbol/ETH_USDT -- "$symbol_eth_usdt"

echo ">>> 初始化 Pulsar 命名空间与 Topic..."
docker exec pulsar /pulsar/bin/pulsar-admin namespaces create public/trade 2>/dev/null || true
for sym in IKUN_USDT ETH_USDT; do
  docker exec pulsar /pulsar/bin/pulsar-admin topics create "persistent://public/trade/input_match_${sym}" 2>/dev/null || true
  docker exec pulsar /pulsar/bin/pulsar-admin topics create "persistent://public/trade/output_match_${sym}" 2>/dev/null || true
done

echo ">>> 启动业务服务 (accountrpc / match / quoterpc / gateway / adminapi)..."
docker compose -f deploy/dockerfiles/docker-compose.yaml up -d --build

echo ">>> 部署完成"
echo "    Gateway:  http://localhost:8888"
echo "    Nginx:    http://localhost/api/  -> gateway"
echo "    Admin:    http://localhost:20015"
echo "    Pulsar:   pulsar://localhost:6650"
