package handler

import (
	"github.com/ikun2021/gex/app/account/api/internal/logic"
	"github.com/ikun2021/gex/app/account/api/internal/svc"
	"github.com/ikun2021/gex/common/pkg/response"
	"net/http"
)

func GetUserAssetListHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		l := logic.NewGetUserAssetListLogic(r.Context(), svcCtx)
		resp, err := l.GetUserAssetList()
		response.Response(w, r, resp, err) //

	}
}
