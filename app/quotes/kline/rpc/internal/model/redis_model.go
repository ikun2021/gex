package model

import "github.com/ikun2021/gex/app/quotes/kline/rpc/internal/dao/model"

type RedisModel struct {
	model.Kline
	MatchID int64 `json:"match_id"`
}
