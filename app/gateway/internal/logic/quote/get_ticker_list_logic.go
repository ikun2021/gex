package quote

import (
	"context"

	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetTickerListLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetTickerListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetTickerListLogic {
	return &GetTickerListLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetTickerListLogic) GetTickerList(req *types.GetTickerListReq) (resp *types.GetTickerListResp, err error) {
	// todo: add your logic here and delete this line

	return
}
