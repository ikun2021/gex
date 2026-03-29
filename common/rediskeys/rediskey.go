package rediskeys

import "github.com/spf13/cast"

type RedisKey string

func (rk RedisKey) String() string {
	return string(rk)
}

func (rk RedisKey) WithParam(param ...interface{}) string {
	var key = rk.String()
	for _, v := range param {
		key += ":" + cast.ToString(v)
	}
	return key
}
