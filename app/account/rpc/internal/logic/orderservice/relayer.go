package orderservicelogic

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/redis/go-redis/v9"
	"time"
)

func StartRelayer(rdb *redis.Client, pulsarProducer pulsar.Producer) {
	ctx := context.Background()
	stream := "mq_outbox"
	group := "relayer_group"
	consumer := "worker_1"

	// 1. 初始化消费者组
	err := rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !isGroupExistErr(err) {
		fmt.Printf("Create Group Error: %v\n", err)
	}

	// 2. 【核心修复】死循环：先处理 Pending 消息（Crash 恢复）
	// Pending 消息是指：已经被读取但没有 XACK 的消息
	for {
		// 使用 ID "0" 表示读取 Pending List 中的所有消息
		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, "0"},
			Count:    10,
			Block:    0, // 不阻塞，读完即止
		}).Result()

		if err != nil {
			fmt.Println("Pending Read Error:", err)
			time.Sleep(time.Second)
			continue
		}

		// 如果没有 Pending 消息了，跳出修复循环，进入实时消费模式
		if len(streams) == 0 || len(streams[0].Messages) == 0 {
			fmt.Println("Pending messages cleared. Starting real-time consumption...")
			break
		}

		// 处理 Pending 消息
		for _, xstream := range streams {
			for _, msg := range xstream.Messages {
				processMessage(ctx, rdb, pulsarProducer, stream, group, msg)
			}
		}
	}

	// 3. 实时消费循环
	for {
		// 使用 ">" 读取从未被消费的新消息
		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    10,
			Block:    2000 * time.Millisecond,
		}).Result()

		if err != nil {
			// 超时未读到数据不是错误，是正常现象 (redis.Nil)
			if !errors.Is(err, redis.Nil) {
				fmt.Println("Realtime Read Error:", err)
				time.Sleep(time.Second)
			}
			continue
		}

		for _, xstream := range streams {
			for _, msg := range xstream.Messages {
				processMessage(ctx, rdb, pulsarProducer, stream, group, msg)
			}
		}
	}
}

func processMessage(ctx context.Context, rdb *redis.Client, producer pulsar.Producer, stream, group string, msg redis.XMessage) {
	// 4. 解析
	// 注意做空值检查
	val, ok := msg.Values["payload"]
	if !ok {
		// 异常数据，直接Ack并删除，防止死循环
		rdb.XAck(ctx, stream, group, msg.ID)
		rdb.XDel(ctx, stream, msg.ID)
		return
	}
	payload := val.(string)

	// 5. 发送 Pulsar
	_, err := producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: []byte(payload),
		Key:     msg.ID, // 建议把 RedisMsgID 透传，方便下游去重日志
	})

	if err != nil {
		fmt.Printf("Pulsar send failed: %v. Will retry later.\n", err)
		// 发送失败直接 return。
		// 下次循环时：
		// - 如果是在 Pending 恢复阶段，会再次读到它。
		// - 如果是在实时阶段，虽然 Loop 会继续读新的，但下次服务重启会重新处理 Pending。
		// - 或者可以引入 XCLAIM 机制让其他 worker 抢占（高级用法）。
		return
	}

	// 6. 成功后 Ack
	// Pipeline 优化：Ack 和 Del 可以合并发送
	pipe := rdb.Pipeline()
	pipe.XAck(ctx, stream, group, msg.ID)
	pipe.XDel(ctx, stream, msg.ID)
	_, err = pipe.Exec(ctx)

	if err != nil {
		fmt.Printf("Redis Ack/Del failed: %v\n", err)
		// 这里失败没事，因为 Pulsar 已经发出去了。
		// 最坏情况是服务重启后再次从 Pending 读出 -> 重复发送 Pulsar。
		// 所以下游撮合引擎必须做幂等 (MatchID / Unique Key)。
	}
}

func isGroupExistErr(err error) bool {
	return err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists"
}
