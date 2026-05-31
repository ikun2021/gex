
genaccount:
#gormt 通过数据库生成指定的结构体 https://github.com/xxjwxc/gormt -z config.yaml 指定配置文件路径
	gentool --dsn="root:root@tcp(192.168.2.159:3307)/trade?charset=utf8mb4&parseTime=True&loc=Local" --db=mysql --tables=user,asset  -outPath=app/account/rpc/internal/dao/query -fieldMap="decimal:string;tinyint:int32;"

enum:
	protoc   -I. --go_out=./  common/proto/enum/*.proto
matchmq:
	#--go_out指定的路径和option go_package = "trade/common/proto/mq/match;proto"; 指定的路径一起决定文件生成的位置 这个路径trade/common/proto/mq/match也是别人导入时用到的路径。
	protoc    -Icommon/proto -I./ --go_out=./ common/proto/mq/match/match.proto
matchrpc:
	goctl rpc  protoc -I./ app/match/pb/match.proto --go_out=app/match --go-grpc_out=app/match  --zrpc_out=app/match -style=go_zero  -home=template
matchmodel:
	gentool --dsn="root:root@tcp(192.168.2.159:3307)/trade?charset=utf8mb4&parseTime=True&loc=Local" --db=mysql --tables=matched_order  -outPath=app/match/rpc/internal/dao/query -fieldMap="decimal:string;tinyint:int32;int:int64"

gapi:
	goctl api go -api=app/gateway/desc/gateway.api -dir=app/gateway -style=go_zero  -home=template

quoterpc:
	goctl rpc  protoc -I./ app/quote/rpc/pb/quote.proto --go_out=app/quote/rpc --go-grpc_out=app/quote/rpc  --zrpc_out=app/quote/rpc -style=go_zero  -home=template
accountrpc:
	   goctl rpc    protoc -I./ -Icommon/proto   app/account/rpc/pb/account.proto --go_out=app/account/rpc --go-grpc_out=app/account/rpc   --zrpc_out=app/account/rpc -style=go_zero  -home=template   --multiple
adminapi:
	goctl api go -api=app/admin/api/desc/admin.api -dir=app/admin/api -style=go_zero  -home=template &&   make admindoc

admindoc:
	goctl api plugin -plugin goctl-swagger="swagger -filename doc/admin.json -host api.gex.com" -api app/admin/api/desc/admin.api -dir .

matchmq:
	#--go_out指定的路径和option go_package = "trade/common/proto/mq/match;proto"; 指定的路径一起决定文件生成的位置 这个路径trade/common/proto/mq/match也是别人导入时用到的路径。
	protoc    -Icommon/proto -I./ --go_out=./ common/proto/mq/match/match.proto

gapi:
	goctl api go --api=app/gateway/desc/gateway.api -dir=app/gateway -style=go_zero  -home=template
	goctl api swagger -api .\app\gateway\desc\gateway.api -dir ./doc


model1:
	gentool --dsn="root:root@tcp(192.168.2.159:3308)/gex?charset=utf8mb4&parseTime=True&loc=Local" --db=mysql  -outPath=app/quote/rpc/internal/dao/quote/query -fieldMap="decimal:string;tinyint:int32;int:int64;bigint:int64" -tables="trades,kline"

model2:
	gentool --dsn="root:root@tcp(192.168.2.159:3308)/gex?charset=utf8mb4&parseTime=True&loc=Local" --db=mysql  -outPath=app/accoun/rpc/internal/dao/quote/query -fieldMap="decimal:string;tinyint:int32;int:int64;bigint:int64"  -tables="matched_order"

run:
	make pre
	chmod +x ./deploy/scripts/run.sh
	./deploy/scripts/run.sh
clear:
	chmod +x ./deploy/scripts/remove_containers.sh
	chmod +x ./deploy/scripts/remove_images.sh
	./deploy/scripts/remove_containers.sh
	./deploy/scripts/remove_images.sh
	rm -rf deploy/depend/mysql/data/*

pre:
	chmod +x ./bin/accountrpc
	chmod +x ./bin/match
	chmod +x ./bin/quoterpc
	chmod +x ./bin/gateway
	chmod +x ./bin/adminapi
	chmod +x ./deploy/depend/ws/proxy/proxy
	chmod +x ./deploy/depend/ws/socket/socket

dep1:
	docker-compose -f deploy/depend/docker-compose.yaml up
dep2:
	docker-compose -f deploy/dockerfiles/docker-compose.yaml up

build:
	go env -w GOOS=linux
	go env -w GOPROXY=https://goproxy.cn,direct
	go env -w CGO_ENABLED=0
	go build -ldflags="-s -w" -o ./bin/accountrpc ./app/account/rpc/account.go
	go build -ldflags="-s -w" -o ./bin/match ./app/match/match.go
	go build -ldflags="-s -w" -o ./bin/quoterpc ./app/quote/rpc/quote.go
	go build -ldflags="-s -w" -o ./bin/gateway ./app/gateway/gateway.go
	go build -ldflags="-s -w" -o ./bin/adminapi ./app/admin/api/admin.go
