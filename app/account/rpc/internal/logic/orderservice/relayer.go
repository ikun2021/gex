package orderservicelogic

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/redis/go-redis/v9"
	"time"
)

// relayer.go
func StartRelayer(rdb *redis.Client, pulsarProducer pulsar.Producer) {
	ctx := context.Background()
	stream := "mq_outbox"
	group := "relayer_group"
	consumer := "worker_1"

	// 1. 确保消费者组存在 (如果不存在则从头开始 0，或者从最新 $)
	// MkStream 表示如果 Stream 不存在自动创建
	rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	for {
		// 2. 阻塞读取消息 (Block 2秒，避免空轮询 CPU 飙升)
		// ">" 表示读取本组还没被消费过的新消息
		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    10, // 批处理，一次搬运10条
			Block:    2000 * time.Millisecond,
		}).Result()

		if err != nil {
			// Redis 连不上等错误，打印日志并 Sleep 一会重试
			fmt.Println("Redis Read Error:", err)
			time.Sleep(time.Second)
			continue
		}

		// 3. 处理读取到的消息
		for _, xstream := range streams {
			for _, msg := range xstream.Messages {
				processMessage(ctx, rdb, pulsarProducer, stream, group, msg)
			}
		}
	}
}

func processMessage(ctx context.Context, rdb *redis.Client, producer pulsar.Producer, stream, group string, msg redis.XMessage) {
	// 4. 解析消息
	payload := msg.Values["payload"].(string)

	// 5. 发送给 Pulsar (包含重试逻辑)
	// 这里可以使用 producer.SendAsync 提高吞吐，但 Send 更安全
	_, err := producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: []byte(payload),
		Key:     msg.ID, // 将 Redis ID 传给 Pulsar 方便追踪
	})

	if err != nil {
		// 发送失败：
		// 千万不要 ACK！直接 return。
		// 下次循环或者 Crash 重启后，可以通过 XREADGROUP 读取 Pending 消息重试
		fmt.Printf("Pulsar send failed: %v\n", err)
		return
	}

	// 6. 关键步骤：发送成功后，向 Redis 确认
	rdb.XAck(ctx, stream, group, msg.ID)

	// 7. 可选：物理删除消息以节省 Redis 内存
	rdb.XDel(ctx, stream, msg.ID)
}
