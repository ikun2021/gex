package logic

import (
	"context"

	"github.com/ikun2021/gex/app/admin/api/internal/svc"
	"github.com/ikun2021/gex/app/admin/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetSymbolListLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetSymbolListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetSymbolListLogic {
	return &GetSymbolListLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetSymbolListLogic) GetSymbolList(req *types.GetSymbolListReq) (resp *types.GetSymbolListResp, err error) {
	symbol := l.svcCtx.AdminQuery.Symbol
	offset := (req.PageNo - 1) * req.PageSize
	data, count, err := symbol.WithContext(l.ctx).
		Order(symbol.ID.Desc()).
		FindByPage(int(offset), int(req.PageSize))
	list := make([]*types.SymbolInfo, 0, len(data))
	for _, v := range data {
		s := &types.SymbolInfo{
			ID:            v.ID,
			SymbolName:    v.SymbolName,
			SymbolId:      v.SymbolID,
			BaseCoinID:    v.BaseCoinID,
			BaseCoinName:  v.BaseCoinName,
			BaseCoinPrec:  v.BaseCoinPrec,
			QuoteCoinID:   int32(v.QuoteCoinID),
			QuoteCoinName: v.QuoteCoinName,
			QuotePrec:     v.QuoteCoinPrec,
		}
		list = append(list, s)

	}
	return &types.GetSymbolListResp{
		List:  list,
		Total: count,
	}, nil
}
