package quote

import (
	"context"

	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetTickListLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetTickListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetTickListLogic {
	return &GetTickListLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetTickListLogic) GetTickList(req *types.GetTickReq) (resp *types.GetTickResp, err error) {
	// todo: add your logic here and delete this line

	return
}
