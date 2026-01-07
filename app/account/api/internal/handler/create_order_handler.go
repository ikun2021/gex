package handler

import (
	"github.com/ikun2021/gex/app/account/api/internal/logic"
	"github.com/ikun2021/gex/app/account/api/internal/svc"
	"github.com/ikun2021/gex/app/account/api/internal/types"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/pkg/response"
	"github.com/zeromicro/go-zero/rest/httpx"
	"net/http"
)

func CreateOrderHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.CreateOrderReq
		if err := httpx.Parse(r, &req); err != nil {
			response.Response(w, r, nil, errs.WarpMessage(errs.ParamValidateFailed, err.Error()))
			return
		}

		l := logic.NewCreateOrderLogic(r.Context(), svcCtx)
		resp, err := l.CreateOrder(&req)
		response.Response(w, r, resp, err)

	}
}
