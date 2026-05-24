package utils

import "time"

// JWT 基于 JwtConf 与 JwtClaims 的 token 签发与解析封装。
type JWT struct {
	conf *JwtConf
}

func NewJWT(conf *JwtConf) *JWT {
	if conf != nil {
		DefaultJwtConf = conf
	}
	return &JWT{conf: conf}
}

func (j *JWT) confOrDefault() *JwtConf {
	if j != nil && j.conf != nil {
		return j.conf
	}
	return DefaultJwtConf
}

// CreateToken 签发 token，返回 token 与过期时间（秒级时间戳）。
func (j *JWT) CreateToken(userId int64) (token string, expireAt int64, err error) {
	claims, err := NewCustomClaims(JwtClaims{UserId: userId}, j.confOrDefault())
	if err != nil {
		return "", 0, err
	}
	token, err = claims.GenerateToken()
	if err != nil {
		return "", 0, err
	}
	if claims.ExpiresAt != nil {
		expireAt = claims.ExpiresAt.Time.Unix()
	} else {
		expireAt = time.Now().Add(7 * 24 * time.Hour).Unix()
	}
	return token, expireAt, nil
}

// ParseToken 解析 token，Extra 中为 JwtClaims（仅 userId）。
func (j *JWT) ParseToken(token string) (*CustomClaims[JwtClaims], error) {
	return ParseToken[JwtClaims](token, j.confOrDefault())
}
