package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

const matchOutputConsumedTTL = 30 * time.Minute

func matchOutputConsumedKey(messageId int64) string {
	return fmt.Sprintf("match_output:consumed:%d", messageId)
}

// tryMarkMatchOutputConsumed 使用 message_id 占位，防止重复消费。返回 duplicated=true 表示已处理过。
func tryMarkMatchOutputConsumed(ctx context.Context, rdb *redis.Client, messageId int64) (duplicated bool, err error) {
	if messageId == 0 {
		return false, nil
	}
	ok, err := rdb.SetNX(ctx, matchOutputConsumedKey(messageId), "1", matchOutputConsumedTTL).Result()
	if err != nil {
		return false, err
	}
	return !ok, nil
}

func releaseMatchOutputConsumed(ctx context.Context, rdb *redis.Client, messageId int64) {
	if messageId == 0 {
		return
	}
	if err := rdb.Del(ctx, matchOutputConsumedKey(messageId)).Err(); err != nil {
		logx.Errorw("release match output consumed mark failed",
			logx.Field("messageId", messageId),
			logx.Field("error", err.Error()))
	}
}
