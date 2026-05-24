package ctxdata

import (
	"context"

	"github.com/ikun2021/gex/common/errs"
)

type ctxKey int

const (
	userIDKey ctxKey = iota
	usernameKey
	tokenKey
)

// WithUser 将登录用户写入 context（由 Auth 中间件调用）。
func WithUser(ctx context.Context, uid int64, username, token string) context.Context {
	ctx = context.WithValue(ctx, userIDKey, uid)
	ctx = context.WithValue(ctx, usernameKey, username)
	return context.WithValue(ctx, tokenKey, token)
}

// GetUid 从 context 获取当前登录用户 ID。
func GetUid(ctx context.Context) (int64, error) {
	uid, ok := ctx.Value(userIDKey).(int64)
	if !ok || uid <= 0 {
		return 0, errs.TokenValidateFailed
	}
	return uid, nil
}

// GetUsername 从 context 获取当前登录用户名。
func GetUsername(ctx context.Context) string {
	name, _ := ctx.Value(usernameKey).(string)
	return name
}

// GetToken 从 context 获取当前请求的 JWT token。
func GetToken(ctx context.Context) string {
	t, _ := ctx.Value(tokenKey).(string)
	return t
}
