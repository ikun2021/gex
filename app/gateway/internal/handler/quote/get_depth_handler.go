// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package quote

import (
	"github.com/ikun2021/gex/app/gateway/internal/logic/quote"
	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/pkg/response"
	"github.com/zeromicro/go-zero/rest/httpx"
	"net/http"
)

func GetDepthHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetDepthReq
		if err := httpx.Parse(r, &req); err != nil {
			response.Response(w, r, nil, errs.WarpMessage(errs.ParamValidateFailed, err.Error()))
			return
		}
		l := quote.NewGetDepthLogic(r.Context(), svcCtx)
		resp, err := l.GetDepth(&req)
		response.Response(w, r, resp, err)

	}
}
