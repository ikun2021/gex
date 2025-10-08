package middleware

import (
	"context"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/pkg/response"

	"github.com/ikun2021/gex/app/account/rpc/accountservice"
	"net/http"
)

type AuthMiddleware struct {
	AccountRpcClient accountservice.AccountService
}

func NewAuthMiddleware(AccountRpcClient accountservice.AccountService) *AuthMiddleware {
	return &AuthMiddleware{
		AccountRpcClient: AccountRpcClient,
	}
}

func (m *AuthMiddleware) Handle(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("gexToken")
		if token == "" {
			response.Response(w, r, nil, errs.TokenValidateFailed)
			return
		}
		userInfo, err := m.AccountRpcClient.ValidateToken(context.Background(), &accountservice.ValidateTokenReq{Token: token})
		if err != nil {
			response.Response(w, r, nil, errs.TokenValidateFailed)
			return
		}
		reqCtx := r.Context()
		ctx := context.WithValue(reqCtx, "uid", userInfo.Uid)
		ctx = context.WithValue(ctx, "username", userInfo.Username)
		newReq := r.WithContext(ctx)
		next(w, newReq)
	}
}
