package define

type RedisKey string

const (
	Ticker                   RedisKey = "gex:ticker"
	Tick                     RedisKey = "gex:tick"
	Kline                    RedisKey = "gex:kline"
	AccountToken             RedisKey = "gex:account:token"
	AccountConsumedMessageId RedisKey = "gex:account:consumed:messageId"
	OrderConsumedMessageId   RedisKey = "gex:order:consumed:messageId"
	OpenOrder                RedisKey = "gex:open_order"
	AccountMatchProcessed    RedisKey = "gex:account_match_processed"
)

func (key RedisKey) WithSymbol(symbol string) string {
	return string(key) + "_" + symbol
}

func (key RedisKey) WithParams(params ...string) string {
	if len(params) == 0 {
		return string(key)
	}
	k := string(key)
	for _, v := range params {
		k += ":" + v
	}
	return k
}
