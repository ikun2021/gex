// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package account

import (
	"net/http"

	"github.com/ikun2021/gex/app/gateway/internal/logic/account"
	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/common/pkg/response"
)

// 登出（从 Authorization 读取 token）
func LoginOutHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := account.NewLoginOutLogic(r.Context(), svcCtx)
		resp, err := l.LoginOut()
		response.Response(w, r, resp, err)
	}
}
