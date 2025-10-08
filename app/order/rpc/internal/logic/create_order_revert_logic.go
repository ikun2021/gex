package logic

import (
	"context"
	"database/sql"
	"github.com/dtm-labs/client/dtmgrpc"
	"github.com/ikun2021/gex/app/order/rpc/internal/dao/model"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/proto/enum"
	commonUtils "github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"gorm.io/gorm"

	"github.com/ikun2021/gex/app/order/rpc/internal/svc"
	"github.com/ikun2021/gex/app/order/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateOrderRevertLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateOrderRevertLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateOrderRevertLogic {
	return &CreateOrderRevertLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 下单补偿
func (l *CreateOrderRevertLogic) CreateOrderRevert(in *pb.CreateOrderReq) (*pb.OrderEmpty, error) {
	logx.Infow("CreateOrderRevert invoke")
	barrier, err := dtmgrpc.BarrierFromGrpc(l.ctx)
	if err != nil {
		logx.Errorw("BarrierFromGrpc db failed", logger.ErrorField(err))
		return nil, errs.DtmErr
	}
	entrustOrder := l.svcCtx.Query.EntrustOrder
	db, err := entrustOrder.WithContext(l.ctx).UnderlyingDB().DB()
	if err != nil {
		logx.Errorw("get UnderlyingDB failed", logger.ErrorField(err))
		return nil, errs.ExecSqlFailed
	}
	//修改订单的状态为无效
	order := &model.EntrustOrder{
		Status: int32(enum.OrderStatus_Wasted),
	}
	//构建sql
	toSQL := entrustOrder.WithContext(l.ctx).UnderlyingDB().ToSQL(func(tx *gorm.DB) *gorm.DB {
		return tx.Table(commonUtils.WithShardingSuffix(order.TableName(), in.UserId)).
			Select(entrustOrder.Status.ColumnName().String()).
			Where("order_id = ?", in.OrderId).
			Updates(order)
	})
	logx.Infof("Create Order Revert:%s", toSQL)
	if err := barrier.CallWithDB(db, func(tx *sql.Tx) error {
		if _, err := tx.Exec(toSQL); err != nil {
			return err
		}
		logx.Slowf("Create Order Revert Sql %v", toSQL)
		return nil
	}); err != nil {
		logx.Errorw("Create Order Revert exec failed sql", logx.Field("sql", toSQL), logger.ErrorField(err), logx.Field("data", in))
		return nil, errs.Internal

	}
	return &pb.OrderEmpty{}, nil
}
