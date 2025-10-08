package logic

import (
	"context"
	"github.com/ikun2021/gex/app/account/rpc/accountservice"
	matchpb "github.com/ikun2021/gex/app/match/rpc/pb"
	orderpb "github.com/ikun2021/gex/app/order/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/proto/define"
	enum "github.com/ikun2021/gex/common/proto/enum"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
	"google.golang.org/grpc/metadata"
	"strings"

	"github.com/ikun2021/gex/app/order/api/internal/svc"
	"github.com/ikun2021/gex/app/order/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateOrderLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCreateOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateOrderLogic {
	return &CreateOrderLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}
func (l *CreateOrderLogic) validateUserBalance(uid int64, coinId int32, freezeQty string) error {

	coinInfo, err := l.svcCtx.AccountRpcClient.GetUserAssetByCoin(l.ctx, &accountservice.GetUserAssetReq{
		Uid:    cast.ToInt64(uid),
		CoinId: coinId,
	})
	if err != nil {
		logx.Errorw("call GetUserAssetByCoin failed", logger.ErrorField(err))
		return err
	}
	if utils.NewFromStringMaxPrec(coinInfo.Asset.AvailableQty).LessThan(utils.NewFromStringMaxPrec(freezeQty)) {
		return errs.AmountInsufficient
	}
	return nil
}

func (l *CreateOrderLogic) CreateOrder(req *types.CreateOrderReq) (resp *types.Empty, err error) {
	ctx := metadata.NewIncomingContext(l.ctx, metadata.Pairs("symbol", req.SymbolName))

	//参数校验
	s, ok := l.svcCtx.Symbols.Load(req.SymbolName)
	if !ok {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "symbol not existed")
	}
	symbolInfo, ok := s.(*define.SymbolInfo)
	if !ok {
		return nil, errs.Internal
	}
	uid := l.ctx.Value("uid")
	//参数校验
	if _, ok := enum.Side_name[req.Side]; !ok {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "side must is 1 or 2")
	}
	if _, ok := enum.OrderType_name[req.OrderType]; !ok {
		return nil, errs.WarpMessage(errs.ParamValidateFailed, "order type must is 1 or 2")
	}
	zero, basePrec, quotePrec := decimal.NewFromInt32(0), 0, 0
	switch {
	case enum.OrderType(req.OrderType) == enum.OrderType_MO && enum.Side(req.Side) == enum.Side_Sell:
		qty, err := decimal.NewFromString(req.Qty)
		if err != nil || qty.Equal(zero) {
			return nil, errs.WarpMessage(errs.ParamValidateFailed, "qty must is a number")
		}
		//价格精度
		q := strings.Split(req.Qty, ".")
		if len(q) == 2 {
			basePrec = len(q[1])
		}

		//价格精度
		if int(symbolInfo.BaseCoinPrec.Load()) < basePrec {
			return nil, errs.ErrPrec
		}

		//验证用户金额
		if err := l.validateUserBalance(cast.ToInt64(uid), symbolInfo.BaseCoinID, req.Qty); err != nil {
			return nil, err
		}

		//卖买盘校验，市价单在没有买卖盘的时候不能下

		depthList, err := l.svcCtx.MatchClient.GetDepth(ctx, &matchpb.GetDepthReq{
			Symbol: req.SymbolName,
			Level:  1,
		})
		if err != nil {
			logx.Errorw("CreateOrder call GetDepth failed", logger.ErrorField(err))
			return nil, errs.Internal
		}
		if len(depthList.Bids) == 0 {
			return nil, errs.NotBids
		}

	case enum.OrderType(req.OrderType) == enum.OrderType_MO && enum.Side(req.Side) == enum.Side_Buy:
		amount, err := decimal.NewFromString(req.Amount)
		if err != nil || amount.Equal(zero) {
			return nil, errs.WarpMessage(errs.ParamValidateFailed, "amount must is a number")
		}
		//价格精度
		a := strings.Split(req.Amount, ".")
		if len(a) == 2 {
			quotePrec = len(a[1])
		}
		//价格精度
		if int(symbolInfo.QuoteCoinPrec.Load()) < quotePrec {
			return nil, errs.ErrPrec
		}

		//验证用户金额
		if err := l.validateUserBalance(cast.ToInt64(uid), symbolInfo.BaseCoinID, req.Qty); err != nil {
			return nil, err
		}

		//验证用户金额
		if err := l.validateUserBalance(cast.ToInt64(uid), symbolInfo.QuoteCoinID, req.Amount); err != nil {
			return nil, err
		}

		depthList, err := l.svcCtx.MatchClient.GetDepth(ctx, &matchpb.GetDepthReq{
			Symbol: req.SymbolName,
			Level:  1,
		})
		if err != nil {
			logx.Errorw("CreateOrder call GetDepth failed", logger.ErrorField(err))
			return nil, errs.Internal
		}
		if len(depthList.Asks) == 0 {
			return nil, errs.NotAsks
		}
	//限价单参数校验
	case enum.OrderType(req.OrderType) == enum.OrderType_LO:
		qty, err := decimal.NewFromString(req.Qty)
		if err != nil || qty.Equal(zero) {
			return nil, errs.WarpMessage(errs.ParamValidateFailed, "qty must is a number")
		}
		p, err := decimal.NewFromString(req.Price)
		if err != nil || p.Equal(zero) {
			return nil, errs.WarpMessage(errs.ParamValidateFailed, "price must is a number")
		}
		req.Amount = utils.NewFromStringMaxPrec(req.Qty).Mul(utils.NewFromStringMaxPrec(req.Price)).String()
		//判断用户是否拥有足够的币
		//价格精度
		price := strings.Split(req.Price, ".")
		if len(price) == 2 {
			quotePrec = len(price[1])
		}
		//价格精度
		if int(symbolInfo.QuoteCoinPrec.Load()) < quotePrec {
			return nil, errs.ErrPrec
		}
		q := strings.Split(req.Qty, ".")
		if len(q) == 2 {
			basePrec = len(q[1])
		}
		//价格精度
		if int(symbolInfo.BaseCoinPrec.Load()) < basePrec {
			return nil, errs.ErrPrec
		}

		if enum.Side(req.Side) == enum.Side_Buy {
			if err := l.validateUserBalance(cast.ToInt64(uid), symbolInfo.QuoteCoinID, req.Amount); err != nil {
				logx.Sloww("validate user balance failed", logger.ErrorField(err))
				return nil, err
			}
		}
		//判断用户是否拥有足够的币
		if enum.Side(req.Side) == enum.Side_Sell {
			if err := l.validateUserBalance(cast.ToInt64(uid), symbolInfo.BaseCoinID, req.Qty); err != nil {
				return nil, err
			}
		}

	}

	//用户资产校验
	_, err = l.svcCtx.OrderClient.Order(ctx, &orderpb.CreateOrderReq{
		UserId:     cast.ToInt64(uid),
		SymbolId:   symbolInfo.SymbolID,
		SymbolName: req.SymbolName,
		Qty:        req.Qty,
		Price:      req.Price,
		Amount:     req.Amount,
		Side:       enum.Side(req.Side),
		OrderType:  enum.OrderType(req.OrderType),
		OrderId:    "",
	})
	if err != nil {
		logx.Errorw("call create order failed", logger.ErrorField(err))
		return nil, err
	}
	resp = &types.Empty{}
	return
}
