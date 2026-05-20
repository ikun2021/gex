package dao

type RedisModel struct {
	KlineHistory
	MatchID int64 `json:"match_id"`
}
