package account

import (
	"context"

	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type ValidateTokenLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewValidateTokenLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ValidateTokenLogic {
	return &ValidateTokenLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ValidateTokenLogic) ValidateToken(req *types.ValidateTokenReq) (resp *types.ValidateTokenResp, err error) {
	// todo: add your logic here and delete this line

	return
}
