// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package order

import (
	"net/http"

	"github.com/ikun2021/gex/app/gateway/internal/logic/order"
	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// 获取用户订单列表
func GetOrderListHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetOrderListReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := order.NewGetOrderListLogic(r.Context(), svcCtx)
		resp, err := l.GetOrderList(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
