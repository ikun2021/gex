package logic

import (
	"context"
	"fmt"
	"github.com/dtm-labs/client/dtmgrpc"
	accountpb "github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/ikun2021/gex/app/order/rpc/internal/svc"
	"github.com/ikun2021/gex/app/order/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	enum "github.com/ikun2021/gex/common/proto/enum"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"github.com/spf13/cast"
	"github.com/yitter/idgenerator-go/idgen"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"strings"
)

type OrderLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *OrderLogic {
	return &OrderLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 下单。
func (l *OrderLogic) Order(in *pb.CreateOrderReq) (*pb.OrderEmpty, error) {

	freezeReq := &accountpb.FreezeUserAssetReq{
		Uid:    in.UserId,
		CoinId: 0,
		Qty:    in.Qty,
	}
	//如果是买冻结计价币
	if in.Side == enum.Side_Buy {
		freezeReq.CoinId = l.svcCtx.Config.SymbolInfo.QuoteCoinID
		freezeReq.Qty = in.Amount
		if in.OrderType == enum.OrderType_LO {
			freezeReq.Qty = utils.NewFromStringMaxPrec(in.Qty).Mul(utils.NewFromStringMaxPrec(in.Price)).String()
		}
	} else {
		freezeReq.CoinId = l.svcCtx.Config.SymbolInfo.BaseCoinID
	}

	//订单Id的规则
	//市价单MO
	//限价单LO
	//买1 卖 2
	orderId := "mo"
	if in.OrderType == enum.OrderType_LO {
		orderId = "lo"
	}
	orderId = fmt.Sprintf("%v%v%v", orderId, int32(in.Side), idgen.NextId())

	createOrderReq := &pb.CreateOrderReq{
		UserId:     in.UserId,
		SymbolId:   in.SymbolId,
		SymbolName: in.SymbolName,
		Qty:        in.Qty,
		Price:      in.Price,
		Side:       in.Side,
		OrderType:  in.OrderType,
		Amount:     in.Amount,
		OrderId:    orderId,
	}
	gid, err := l.svcCtx.DtmClient.NewGid(l.ctx, &emptypb.Empty{})
	if err != nil {
		logx.Errorw("get gid failed", logger.ErrorField(err))
		return nil, errs.DtmErr
	}

	accountTarget, err := l.svcCtx.Config.AccountRpcConf.BuildTarget()
	if err != nil {
		logx.Errorw("get account client failed", logger.ErrorField(err))
		return nil, errs.Internal
	}
	//配置的key加上symbol
	orderTarget, err := l.svcCtx.Config.OrderRpcConf.BuildTarget()
	if err != nil {
		logx.Errorw("get order client failed", logger.ErrorField(err))
		return nil, errs.Internal
	}
	//l.svcCtx.Config.OrderRpcConf.Endpoints
	var (
		freezeUserAddr    = accountTarget + "/account.AccountService/FreezeUserAsset"
		unFreezeUserAddr  = accountTarget + "/account.AccountService/UnFreezeUserAsset"
		createOrderAddr   = orderTarget + "/order.OrderService/CreateOrder"
		createOrderRevert = orderTarget + "/order.OrderService/CreateOrderRevert"
	)

	dtmAddr, err := l.svcCtx.Config.DtmConf.BuildTarget()
	if err != nil {
		logx.Errorw("get dtm client failed", logger.ErrorField(err))
		return nil, err
	}
	sagaGrpc := dtmgrpc.NewSagaGrpc(dtmAddr, gid.Gid)
	sagaGrpc.WaitResult = true
	if err := sagaGrpc.
		Add(freezeUserAddr, unFreezeUserAddr, freezeReq).
		Add(createOrderAddr, createOrderRevert, createOrderReq).Submit(); err != nil {
		logx.Errorw("Submit saga  failed", logger.ErrorField(err))
		s, ok := status.FromError(err)
		if ok {
			if s.Code() == codes.Aborted && strings.Contains(s.Message(), "=") {
				msg := s.Message()
				start, end := strings.Index(msg, "="), strings.LastIndex(msg, "=")
				d := msg[start+1 : end]
				e, err := cast.ToInt32E(d)
				if err != nil {
					return nil, errs.Internal
				}
				return nil, errs.Code(e).Error("")
			}

		}
		return nil, errs.DtmErr
	}
	return &pb.OrderEmpty{}, nil
}
