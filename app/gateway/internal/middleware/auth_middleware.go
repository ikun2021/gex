package middleware

import (
	"net/http"
	"strings"

	"github.com/ikun2021/gex/app/account/rpc/client/accountservice"
	"github.com/ikun2021/gex/app/gateway/internal/ctxdata"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/pkg/response"
)

type AuthMiddleware struct {
	accountRpc accountservice.AccountService
}

func NewAuthMiddleware(accountRpc accountservice.AccountService) *AuthMiddleware {
	return &AuthMiddleware{accountRpc: accountRpc}
}

func (m *AuthMiddleware) Handle(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := extractToken(r)
		if token == "" {
			response.Response(w, r, nil, errs.TokenValidateFailed)
			return
		}

		resp, err := m.accountRpc.ValidateToken(r.Context(), &accountservice.ValidateTokenReq{
			Token: token,
		})
		if err != nil {
			response.Response(w, r, nil, err)
			return
		}
		if resp.Uid <= 0 {
			response.Response(w, r, nil, errs.TokenValidateFailed)
			return
		}

		ctx := ctxdata.WithUser(r.Context(), resp.Uid, resp.Username, token)
		next(w, r.WithContext(ctx))
	}
}

func extractToken(r *http.Request) string {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(auth) >= 7 && strings.EqualFold(auth[:7], "Bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	if t := strings.TrimSpace(r.Header.Get("token")); t != "" {
		return t
	}
	return ""
}
