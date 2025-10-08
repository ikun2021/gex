package logic

import (
	"context"
	"github.com/ikun2021/gex/app/admin/api/internal/dao/match/model"
	"github.com/ikun2021/gex/app/admin/api/internal/svc"
	"github.com/ikun2021/gex/app/admin/api/internal/types"
	"github.com/ikun2021/gex/common/utils"
	"github.com/zeromicro/go-zero/core/logx"
)

type GetMatchListLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetMatchListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetMatchListLogic {
	return &GetMatchListLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetMatchListLogic) GetMatchList(req *types.GetMatchListReq) (resp *types.GetMatchListResp, err error) {
	matchedOrder := l.svcCtx.MatchQuery.MatchedOrder
	db := matchedOrder.WithContext(l.ctx).UnderlyingDB()

	offset := (req.PageNo - 1) * req.PageSize
	//想一次查出sql比较复杂,直接原生
	sql := `
SELECT
	mo.id,
	mo.symbol_id,
	mo.symbol_name,
	mo.taker_is_buyer,
	mo.match_id,
	mo.created_at,
	sum(amount) as total_amount,
	sum(qty) as total_qty,
	JSON_ARRAYAGG(
	JSON_OBJECT( 'match_sub_id',mo.match_sub_id,'price',CAST(mo.price as char),'amount', CAST(mo.amount as char),'qty',CAST(mo.qty as char),'taker_user_id', mo.taker_user_id,'maker_user_id', mo.maker_user_id,'maker_order_id', mo.maker_order_id,'taker_order_id', mo.taker_order_id,'match_time', mo.match_time))  as sub_match_list
FROM
	matched_order mo 
GROUP BY
	match_id 
ORDER BY
	created_at DESC 
	LIMIT ? offset ?
	`
	data := make([]*model.MatchedOrderAgg, 0, req.PageSize)
	if err := db.Raw(sql, req.PageSize, offset).Scan(&data).Error; err != nil {
		logx.Errorw("GetMatchList failed ", logx.Field("err", err))
		return nil, err
	}
	list := make([]*types.MatchInfo, 0, len(data))
	for _, v := range data {
		avgPrice := utils.NewFromStringMaxPrec(v.TotalAmount).Div(utils.NewFromStringMaxPrec(v.TotalQty))
		m := &types.MatchInfo{
			ID:          v.ID,
			MatchID:     v.MatchID,
			SymbolID:    v.SymbolID,
			SymbolName:  v.SymbolName,
			TotalQty:    utils.PrecCut(v.TotalQty, 5),
			TotalAmount: utils.PrecCut(v.TotalAmount, 5),
			AvgPrice:    avgPrice.StringFixed(5),
			CreatedAt:   v.CreatedAt,
		}
		subOrders := []*model.SubMatchedOrder(v.SubMatchList)
		s := make([]*types.SubMatchInfo, 0, len(subOrders))
		for _, v := range subOrders {
			d := &types.SubMatchInfo{
				TakerUserID: v.TakerUserID,
				MakerUserID: v.MakerUserID,
				MatchPrice:  utils.PrecCut(v.Price, 5),
				MatchQty:    utils.PrecCut(v.Qty, 5),
				MatchAmount: utils.PrecCut(v.Amount, 5),
				MatchTime:   v.MatchTime / 1e9,
			}
			s = append(s, d)
		}
		m.SubMatchInfoList = s
		list = append(list, m)
	}
	// 查询总数
	count, err := matchedOrder.WithContext(l.ctx).Distinct(matchedOrder.MatchID).Count()
	if err != nil {
		return nil, err
	}

	return &types.GetMatchListResp{
		List:  list,
		Total: count,
	}, nil

}
