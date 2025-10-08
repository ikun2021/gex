package main

import (
	"context"
	"fmt"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/zeromicro/go-zero/core/stringx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"time"
)

func main() {
	conn, err := grpc.Dial("localhost:20002", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Panicf("Failed to connect: %v", err)
	}
	defer conn.Close()
	cli := pb.NewAccountServiceClient(conn)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		username := fmt.Sprintf("test%d", i+1)
		password := stringx.Randn(8)
		result, err := cli.Register(ctx, &pb.RegisterReq{
			Username:    username,
			Password:    password,
			PhoneNumber: time.Now().Unix(),
		})
		if err != nil {
			log.Panicf("Failed to register: %v", err)
		}

		_, err = cli.AddUserAsset(ctx, &pb.AddUserAssetReq{
			Uid:      result.Uid,
			CoinName: "USDT",
			Qty:      "10000000",
		})
		if err != nil {
			log.Panicf("Failed to add user asset: %v", err)
		}
		_, err = cli.AddUserAsset(ctx, &pb.AddUserAssetReq{
			Uid:      result.Uid,
			CoinName: "IKUN",
			Qty:      "1000",
		})
		if err != nil {
			log.Panicf("Failed to add user asset: %v", err)
		}
		fmt.Printf("uid %v username %v password %v\n", result.Uid, username, password)

	}
}
