// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package account

import (
	"github.com/ikun2021/gex/app/gateway/internal/logic/account"
	"github.com/ikun2021/gex/app/gateway/internal/svc"
	"github.com/ikun2021/gex/app/gateway/internal/types"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/pkg/response"
	"github.com/zeromicro/go-zero/rest/httpx"
	"net/http"
)

// 验证token
func ValidateTokenHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.ValidateTokenReq
		if err := httpx.Parse(r, &req); err != nil {
			response.Response(w, r, nil, errs.WarpMessage(errs.ParamValidateFailed, err.Error()))
			return
		}
		l := account.NewValidateTokenLogic(r.Context(), svcCtx)
		resp, err := l.ValidateToken(&req)
		response.Response(w, r, resp, err)

	}
}
