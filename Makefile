# =============================================================================
# GEX Makefile
# 部署推荐：make dep（基础设施） → make start（业务服务）
# =============================================================================

# -----------------------------------------------------------------------------
# 代码生成（修改 .proto / .api 后执行）
# -----------------------------------------------------------------------------

# 生成公共枚举 proto
enum:
	protoc   -I. --go_out=./  common/proto/enum/*.proto

# 生成 Match RPC 桩代码（app/match/pb/match.proto）
matchrpc:
	goctl rpc  protoc -I./ app/match/pb/match.proto --go_out=app/match --go-grpc_out=app/match  --zrpc_out=app/match -style=go_zero  -home=template

# 生成 Gateway HTTP 代码与 Swagger 文档（app/gateway/desc/gateway.api）
gapi:
	goctl api go --api=app/gateway/desc/gateway.api -dir=app/gateway -style=go_zero  -home=template
	goctl api swagger -api .\app\gateway\desc\gateway.api -dir ./doc

# 生成 Quote RPC 桩代码（app/quote/rpc/pb/quote.proto）
quoterpc:
	goctl rpc  protoc -I./ app/quote/rpc/pb/quote.proto --go_out=app/quote/rpc --go-grpc_out=app/quote/rpc  --zrpc_out=app/quote/rpc -style=go_zero  -home=template

# 生成 Account RPC 桩代码（app/account/rpc/pb/account.proto）
accountrpc:
	goctl rpc    protoc -I./ -Icommon/proto   app/account/rpc/pb/account.proto --go_out=app/account/rpc --go-grpc_out=app/account/rpc   --zrpc_out=app/account/rpc -style=go_zero  -home=template   --multiple

# 生成撮合 MQ 消息 proto（common/proto/mq/match/match.proto）
# --go_out 路径与 option go_package 共同决定生成位置及 import 路径
matchmq:
	protoc    -Icommon/proto -I./ --go_out=./ common/proto/mq/match/match.proto

# -----------------------------------------------------------------------------
# 编译
# -----------------------------------------------------------------------------

# 交叉编译 Linux 二进制到 bin/（供 Docker 镜像 COPY 使用）
build:
	go env -w GOOS=linux
	go env -w GOPROXY=https://goproxy.cn,direct
	go env -w CGO_ENABLED=0
	go build -ldflags="-s -w" -o ./bin/accountrpc ./app/account/rpc/account.go
	go build -ldflags="-s -w" -o ./bin/match ./app/match/match.go
	go build -ldflags="-s -w" -o ./bin/quoterpc ./app/quote/rpc/quote.go
	go build -ldflags="-s -w" -o ./bin/gateway ./app/gateway/gateway.go

# 为 bin/ 与 ws 二进制添加可执行权限
pre:
	chmod +x ./bin/accountrpc
	chmod +x ./bin/match
	chmod +x ./bin/quoterpc
	chmod +x ./bin/gateway
	chmod +x ./deploy/depend/ws/proxy/proxy
	chmod +x ./deploy/depend/ws/socket/socket

# -----------------------------------------------------------------------------
# 部署（Docker Compose）
# -----------------------------------------------------------------------------

# 启动基础设施：MongoDB / Redis / Pulsar / Etcd / Nginx / WebSocket
# 并自动初始化 MongoDB、Pulsar Topic、Redis 测试账户
dep:
	chmod 0400 ./deploy/depend/mongo/conf/rs_keyfile
	chmod 777 ./deploy/depend/mongo/data
	docker network inspect gex >/dev/null 2>&1 || docker network create gex
	docker compose -f deploy/depend/docker-compose.yaml up -d
	make mongo-init
	make pulsar-init
	make redis

# 编译业务二进制并启动 gateway / accountrpc / match / quoterpc 容器
start:
	make build
	docker compose -f deploy/dockerfiles/docker-compose.yaml up -d --build

# 执行 MongoDB 初始化脚本（用户、索引等）
mongo-init:
	docker exec -i gex-mongo mongosh -u admin -p admin --authenticationDatabase admin --quiet < deploy/depend/mongo/scripts/mongo-init.js

# 等待 Pulsar 就绪并创建 public/trade 命名空间及撮合 Topic
pulsar-init:
	@echo ">>> 等待 Pulsar 就绪..."
	@for i in $$(seq 1 30); do \
		docker exec gex-pulsar /pulsar/bin/pulsar-admin clusters list >/dev/null 2>&1 && break; \
		sleep 2; \
	done
	docker exec gex-pulsar /pulsar/bin/pulsar-admin namespaces create public/trade 2>/dev/null || true
	for sym in IKUN_USDT ETH_USDT; do \
		docker exec gex-pulsar /pulsar/bin/pulsar-admin topics create "persistent://public/trade/input_match_$${sym}" 2>/dev/null || true; \
		docker exec gex-pulsar /pulsar/bin/pulsar-admin topics create "persistent://public/trade/output_match_$${sym}" 2>/dev/null || true; \
	done

# 写入 Redis 测试用户（user_id=1）初始资产
redis:
	docker exec gex-redis redis-cli --no-auth-warning -a 123456 HSET balance:{1}:1 USDT 1000000000000 USDT_frozen 0 IKUN 1000000000000 IKUN_frozen 0

# -----------------------------------------------------------------------------
# 清理与调试
# -----------------------------------------------------------------------------

# 旧版一键启动脚本（依赖 deploy/scripts/run.sh，已弃用，请改用 dep + start）
run:
	make pre
	chmod +x ./deploy/scripts/run.sh
	./deploy/scripts/run.sh

# 删除所有 gex-* 容器并清空 mongo / redis / pulsar 数据目录
clean:
	@ids=$$(docker ps -aq --filter name=^gex-); \
	if [ -n "$$ids" ]; then docker rm -f $$ids; fi
	rm -rf deploy/depend/mongo/data/*
	rm -rf deploy/depend/mongo/log/*
	rm -rf deploy/depend/redis/data/*
	rm -rf deploy/depend/pulsar/data/*

# 停止 MongoDB 并清空数据（重置数据库时使用）
c1:
	docker stop gex-mongo
	rm -rf ./deploy/depend/mongo/data/*

# 停止业务容器并删除本地构建镜像
c2:
	docker compose -f deploy/dockerfiles/docker-compose.yaml down --rmi local

# 查看各业务镜像内打包的配置文件（调试用）
c2-cat:
	docker run --rm --entrypoint cat gex/accountrpc:latest /app/account.rpc.yaml
	docker run --rm --entrypoint cat gex/gateway:latest /app/gateway.yaml
	docker run --rm --entrypoint cat gex/match:latest /app/match.yaml
	docker run --rm --entrypoint cat gex/quoterpc:latest /app/quote.rpc.yaml
