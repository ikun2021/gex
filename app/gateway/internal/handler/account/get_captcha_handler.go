// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package account

import (
	"github.com/ikun2021/gex/app/gateway/internal/logic/account"
	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/pkg/response"
	"github.com/zeromicro/go-zero/rest/httpx"
	"net/http"
)

// 获取验证码
func GetCaptchaHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := account.NewGetCaptchaLogic(r.Context(), svcCtx)
		resp, err := l.GetCaptcha()
		response.Response(w, r, resp, err)

	}
}
