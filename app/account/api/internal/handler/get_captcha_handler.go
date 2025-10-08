package handler

import (
	"github.com/ikun2021/gex/app/account/api/internal/logic"
	"github.com/ikun2021/gex/app/account/api/internal/svc"
	"github.com/ikun2021/gex/common/pkg/response"
	"net/http"
)

func GetCaptchaHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		l := logic.NewGetCaptchaLogic(r.Context(), svcCtx)
		resp, err := l.GetCaptcha()
		response.Response(w, r, resp, err) //

	}
}
