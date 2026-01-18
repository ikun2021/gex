package model

type RedisModel struct {
	Kline
	MatchID int64 `json:"match_id"`
}
