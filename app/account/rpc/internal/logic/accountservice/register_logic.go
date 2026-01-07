package accountservicelogic

import (
	"context"
	"errors"
	"github.com/ikun2021/gex/app/account/rpc/internal/dao/model"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"gorm.io/gorm"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type RegisterLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRegisterLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RegisterLogic {
	return &RegisterLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 注册
func (l *RegisterLogic) Register(in *pb.RegisterReq) (*pb.RegisterResp, error) {
	// todo: add your logic here and delete this line
	user := &model.User{
		Username:    in.Username,
		Password:    utils.BcryptHash(in.Password),
		PhoneNumber: in.PhoneNumber,
		Status:      1,
	}
	if err := l.svcCtx.Query.User.WithContext(l.ctx).Create(user); err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return nil, errs.DuplicateDataErr
		}
		logx.Errorw("create user failed", logger.ErrorField(err))
		return nil, errs.ExecSqlFailed
	}
	return &pb.RegisterResp{
		Username: in.Username,
		Uid:      int64(user.ID),
	}, nil
}
