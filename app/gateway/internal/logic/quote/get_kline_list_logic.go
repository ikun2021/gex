package quote

import (
	"context"

	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetKlineListLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetKlineListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetKlineListLogic {
	return &GetKlineListLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetKlineListLogic) GetKlineList(req *types.KlineListReq) (resp *types.KlineListResp, err error) {
	// todo: add your logic here and delete this line

	return
}
