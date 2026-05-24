package utils

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"google.golang.org/grpc/status"
)

var DefaultJwtConf *JwtConf

type SigningMethod string

const (
	SigningMethodHS256   SigningMethod = "HS256"
	SigningMethodEd25519               = "EdDSA"
)

type JwtClaims struct {
	UserId int64 `json:"userId"`
}

type JwtConf struct {
	SignKey        string        `json:"SignKey,optional"`        //签名密钥，支持公钥、私钥 和字符串的形式。
	PrivateKeyPath string        `json:"PrivateKeyPath,optional"` //私钥路径
	PublicKeyPath  string        `json:"PublicKeyPath,optional"`  //公钥路径
	ValidTime      string        `json:"ValidTime,default=7d"`    //有效期，单位秒
	RefreshTime    string        `json:"RefreshTime,default=3d"`  //刷新时间，token剩余时间小于refreshTime时，自动刷新token
	PrivateKeyType SigningMethod `json:"privateKeyType,default=HS256"`
}

func (j *JwtConf) GetValidSecond() int64 {
	duration, err := ParseDuration(j.ValidTime)
	if err != nil {
		return 0
	}
	return int64(duration.Seconds())
}

func (j *JwtConf) GetRefreshSecond() int64 {
	duration, err := ParseDuration(j.RefreshTime)
	if err != nil {
		return 0
	}
	return int64(duration.Seconds())
}

const (
	InValidTokenCode = 100013
	TokenExpiredCode = 100014
)

var (
	InValidTokenErr = status.Error(InValidTokenCode, "")
	TokenExpiredErr = status.Error(TokenExpiredCode, "")
)

type CustomClaims[T any] struct {
	Extra       T
	jwtConf     *JwtConf
	NeedRefresh bool `json:"-"`
	jwt.RegisteredClaims
}

func NewCustomClaims[T any](extra T, jwtConf ...*JwtConf) (*CustomClaims[T], error) {
	var jc *JwtConf
	if len(jwtConf) == 0 {
		jc = DefaultJwtConf
	} else {
		jc = jwtConf[0]
	}
	if jc == nil {
		return nil, fmt.Errorf("jwtConf is nil")
	}
	if jc.PrivateKeyPath == "" && jc.SignKey == "" {
		return nil, fmt.Errorf("jwtConf.SignKey and jwtConf.PrivateKeyPath are both empty")
	}
	customClaim := &CustomClaims[T]{
		Extra:   extra,
		jwtConf: jc,
	}
	if jc.ValidTime != "" {
		duration, err := ParseDuration(jc.ValidTime)
		if err != nil {
			return nil, err
		}
		customClaim.RegisteredClaims = jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(duration)),
		}
	}
	return customClaim, nil
}

func (c *CustomClaims[T]) GenerateToken() (string, error) {
	var (
		key   interface{} = []byte(c.jwtConf.SignKey)
		token *jwt.Token
	)
	switch c.jwtConf.PrivateKeyType {
	case SigningMethodHS256:
		token = jwt.NewWithClaims(jwt.SigningMethodHS256, c)
		if c.jwtConf.PrivateKeyPath != "" {
			data, err := os.ReadFile(c.jwtConf.PrivateKeyPath)
			if err != nil {
				return "", err
			}
			key, err = jwt.ParseRSAPrivateKeyFromPEM(data)
			if err != nil {
				return "", err
			}
		}
	case SigningMethodEd25519:
		token = jwt.NewWithClaims(jwt.SigningMethodEdDSA, c)
		if c.jwtConf.PrivateKeyPath != "" {
			data, err := os.ReadFile(c.jwtConf.PrivateKeyPath)
			if err != nil {
				return "", err
			}
			key, err = jwt.ParseEdPrivateKeyFromPEM(data)
			if err != nil {
				return "", err
			}
		}
	default:
		return "", fmt.Errorf("privateKeyType is not supported")
	}

	return token.SignedString(key)

}

func ParseToken[T any](tokenKey string, jwtConf ...*JwtConf) (*CustomClaims[T], error) {
	var jc *JwtConf
	if len(jwtConf) == 0 {
		jc = DefaultJwtConf
	} else {
		jc = jwtConf[0]
	}
	if jc == nil {
		return nil, fmt.Errorf("jwtConf is nil")
	}
	if (jc.PublicKeyPath == "" || jc.PrivateKeyPath == "") && jc.SignKey == "" {
		return nil, fmt.Errorf("jwtConf.SignKey and jwtConf.PrivateKeyPath are both empty")
	}

	var customClaims CustomClaims[T]
	token, err := jwt.ParseWithClaims(tokenKey, &customClaims, func(c *jwt.Token) (interface{}, error) {
		var key interface{} = []byte(jc.SignKey)
		if jc.PublicKeyPath != "" {
			data, err := os.ReadFile(jc.PublicKeyPath)
			if err != nil {
				return nil, err
			}
			switch jc.PrivateKeyType {
			case SigningMethodHS256:
				return jwt.ParseRSAPublicKeyFromPEM(data)
			case SigningMethodEd25519:
				return jwt.ParseEdPublicKeyFromPEM(data)
			}
		}
		return key, nil

	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) || errors.Is(err, jwt.ErrTokenNotValidYet) {
			return nil, TokenExpiredErr
		}
		return nil, InValidTokenErr
	}
	if token != nil {
		if claims, ok := token.Claims.(*CustomClaims[T]); ok && token.Valid {
			if jc.RefreshTime != "" {
				duration, err := ParseDuration(jc.RefreshTime)
				if err != nil {
					return nil, err
				}

				//判断是否要刷新token
				if claims.ExpiresAt.Unix()-time.Now().Unix() < int64(duration.Seconds()) {
					claims.NeedRefresh = true
				}

			}

			return claims, nil
		}
		return nil, InValidTokenErr

	} else {
		return nil, InValidTokenErr
	}
}

func ParseDuration(d string) (time.Duration, error) {
	d = strings.TrimSpace(d)
	dr, err := time.ParseDuration(d)
	if err == nil {
		return dr, nil
	}
	if strings.Contains(d, "d") {
		index := strings.Index(d, "d")

		hour, _ := strconv.Atoi(d[:index])
		dr = time.Hour * 24 * time.Duration(hour)
		ndr, err := time.ParseDuration(d[index+1:])
		if err != nil {
			return dr, nil
		}
		return dr + ndr, nil
	}

	dv, err := strconv.ParseInt(d, 10, 64)
	return time.Duration(dv), err
}
