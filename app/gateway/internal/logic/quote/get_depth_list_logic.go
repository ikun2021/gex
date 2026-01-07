package quote

import (
	"context"

	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetDepthListLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetDepthListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetDepthListLogic {
	return &GetDepthListLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetDepthListLogic) GetDepthList(req *types.GetDepthListReq) (resp *types.GetDepthListResp, err error) {
	// todo: add your logic here and delete this line

	return
}
