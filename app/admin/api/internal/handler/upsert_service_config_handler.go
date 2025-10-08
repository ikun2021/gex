package handler

import (
	"github.com/ikun2021/gex/app/admin/api/internal/logic"
	"github.com/ikun2021/gex/app/admin/api/internal/svc"
	"github.com/ikun2021/gex/app/admin/api/internal/types"
	"github.com/ikun2021/gex/common/pkg/response"
	"github.com/zeromicro/go-zero/rest/httpx"
	"net/http"
)

func UpsertServiceConfigHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.UpsertServiceConfigReq
		if err := httpx.Parse(r, &req); err != nil {
			response.Response(w, r, nil, err)
			return
		}

		l := logic.NewUpsertServiceConfigLogic(r.Context(), svcCtx)
		resp, err := l.UpsertServiceConfig(&req)
		response.Response(w, r, resp, err) //

	}
}
