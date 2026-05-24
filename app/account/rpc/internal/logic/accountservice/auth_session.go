package accountservicelogic

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/gookit/goutil/strutil"
	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/common/proto/define"
	"github.com/redis/go-redis/v9"
)

func tokenRedisKey(token string) string {
	return define.AccountToken.WithParams(strutil.Md5(token))
}

func sessionRedisKey(userId int64) string {
	return define.AccountSession.WithParams(strconv.FormatInt(userId, 10))
}

// saveSession 单点登录：写入当前 token，并踢掉该用户旧 token。
func saveSession(ctx context.Context, svcCtx *svc.ServiceContext, userId int64, token string, ttl time.Duration) error {
	tokenKey := tokenRedisKey(token)
	sessKey := sessionRedisKey(userId)
	tokenMd5 := strutil.Md5(token)

	oldMd5, err := svcCtx.RedisCli.Get(ctx, sessKey).Result()
	if err != nil && !errorsIsNil(err) {
		return err
	}
	if oldMd5 != "" && oldMd5 != tokenMd5 {
		_ = svcCtx.RedisCli.Del(ctx, define.AccountToken.WithParams(oldMd5)).Err()
	}

	pipe := svcCtx.RedisCli.Pipeline()
	pipe.Set(ctx, sessKey, tokenMd5, ttl)
	pipe.Set(ctx, tokenKey, strconv.FormatInt(userId, 10), ttl)
	_, err = pipe.Exec(ctx)
	return err
}

// validateSession 校验 token 是否为该用户当前有效会话。
func validateSession(ctx context.Context, svcCtx *svc.ServiceContext, userId int64, token string) (bool, error) {
	tokenMd5 := strutil.Md5(token)
	sessKey := sessionRedisKey(userId)
	tokenKey := tokenRedisKey(token)

	sessionMd5, err := svcCtx.RedisCli.Get(ctx, sessKey).Result()
	if err != nil {
		if errorsIsNil(err) {
			return false, nil
		}
		return false, err
	}
	if sessionMd5 != tokenMd5 {
		return false, nil
	}

	uidStr, err := svcCtx.RedisCli.Get(ctx, tokenKey).Result()
	if err != nil {
		if errorsIsNil(err) {
			return false, nil
		}
		return false, err
	}
	return uidStr == strconv.FormatInt(userId, 10), nil
}

// revokeSession 登出：删除 token 与 session（仅当仍为当前会话）。
func revokeSession(ctx context.Context, svcCtx *svc.ServiceContext, userId int64, token string) error {
	tokenMd5 := strutil.Md5(token)
	sessKey := sessionRedisKey(userId)
	tokenKey := tokenRedisKey(token)

	sessionMd5, err := svcCtx.RedisCli.Get(ctx, sessKey).Result()
	if err != nil && !errorsIsNil(err) {
		return err
	}
	keys := []string{tokenKey}
	if sessionMd5 == tokenMd5 {
		keys = append(keys, sessKey)
	}
	if len(keys) == 0 {
		return nil
	}
	return svcCtx.RedisCli.Del(ctx, keys...).Err()
}

func errorsIsNil(err error) bool {
	return errors.Is(err, redis.Nil)
}

func jwtTTL(svcCtx *svc.ServiceContext) time.Duration {
	sec := svcCtx.Config.JwtConf.GetValidSecond()
	if sec <= 0 {
		sec = 7 * 24 * 3600
	}
	return time.Duration(sec) * time.Second
}
