package test

import (
	"context"
	"github.com/ikun2021/gex/app/order/rpc/pb"
	"github.com/ikun2021/gex/common/proto/enum"
	"google.golang.org/grpc"
	"log"
	"testing"
)

func TestOrderGrpc(t *testing.T) {
	conn, err := grpc.Dial("192.168.2.138:20001", grpc.WithInsecure())
	if err != nil {
		log.Println("did not connect.", err)
		return
	}
	defer conn.Close()
	client := pb.NewOrderServiceClient(conn)
	_, err = client.Order(context.Background(), &pb.CreateOrderReq{
		UserId:     1,
		SymbolId:   1,
		SymbolName: "IKUN_USDT",
		Qty:        "1.0",
		Price:      "100",
		Amount:     "100",
		Side:       enum.Side_Sell,
		OrderType:  2,
	})
	if err != nil {
		log.Println(err)
	}
	//_, err = client.Order(context.Background(), &pb.CreateOrderReq{
	//	UserId:      3,
	//	SymbolId:    1,
	//	SymbolName:  "IKUN_USDT",
	//	Qty:         "1.0",
	//	Price:       "100",
	//	Amount:      "100",
	//	Side:        enum.Side_Buy,
	//	OrderType:   2,
	//	BaseCoinID:  1,
	//	QuoteCoinID: 2,
	//})
	//if err != nil {
	//	log.Println(err)
	//}
}
func TestOrderGrpc1(t *testing.T) {
	conn, err := grpc.Dial("192.168.2.138:20001", grpc.WithInsecure())
	if err != nil {
		log.Println("did not connect.", err)
		return
	}
	defer conn.Close()
	client := pb.NewOrderServiceClient(conn)
	_, err = client.Order(context.Background(), &pb.CreateOrderReq{
		UserId:     3,
		SymbolId:   1,
		SymbolName: "IKUN_USDT",
		Qty:        "1.0",
		Price:      "90",
		Amount:     "90",
		Side:       enum.Side_Buy,
		OrderType:  2,
	})
	if err != nil {
		log.Println(err)
	}
}
