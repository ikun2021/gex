// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package quote

import (
	"net/http"

	"github.com/ikun2021/gex/app/gateway/internal/logic/quote"
	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// 获取深度
func GetDepthListHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetDepthListReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := quote.NewGetDepthListLogic(r.Context(), svcCtx)
		resp, err := l.GetDepthList(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
