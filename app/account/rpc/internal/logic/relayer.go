package logic

import (
	"context"
	"errors"
	"fmt"
	"github.com/zeromicro/go-zero/core/logx"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/redis/go-redis/v9"
)

func StartRelayer(rdb *redis.Client, pulsarProducer map[string]pulsar.Producer) {
	const (
		MaxSlots       = 16384
		SlotsPerWorker = 300
	)

	for symbol, producer := range pulsarProducer {
		// 计算分片消费
		numWorkers := (MaxSlots + SlotsPerWorker - 1) / SlotsPerWorker

		for i := 0; i < numWorkers; i++ {
			startSlot := i * SlotsPerWorker
			endSlot := (i + 1) * SlotsPerWorker
			if endSlot > MaxSlots {
				endSlot = MaxSlots
			}

			// 启动分片协程
			go func(symbol string, producer pulsar.Producer, start, end int) {
				ctx := context.Background()
				group := fmt.Sprintf("relayer_group_%s", symbol)
				consumer := fmt.Sprintf("worker_%s_%d", symbol, start)

				// 1. 准备该分片负责的所有 Stream Key 列表
				var streams []string
				for s := start; s < end; s++ {
					streams = append(streams, fmt.Sprintf("mq_outbox_%s:{%d}", symbol, s))
				}

				// 2. 初始化消费者组 (批量流的话可能耗时，按需创建或全量初试化)
				for _, s := range streams {
					if err := rdb.XGroupCreateMkStream(ctx, s, group, "0").Err(); err != nil {
						if !isGroupExistErr(err) {
							logx.Errorf("start redis relayer group failed err: %v", err)
						}
					}
				}

				// 3. 消费循环
				for {
					// 构造 XREADGROUP 参数：[s1, s2, ..., sn, ">", ">", ..., ">"]
					readArgs := &redis.XReadGroupArgs{
						Group:    group,
						Consumer: consumer,
						Streams:  make([]string, len(streams)*2),
						Count:    10,
						Block:    2000 * time.Millisecond,
					}
					for idx, s := range streams {
						readArgs.Streams[idx] = s
						readArgs.Streams[idx+len(streams)] = ">"
					}

					res, err := rdb.XReadGroup(ctx, readArgs).Result()
					if err != nil {
						if !errors.Is(err, redis.Nil) {
							fmt.Printf("Read Error (Symbol: %s, Slots: %d-%d): %v\n", symbol, start, end, err)
							time.Sleep(time.Second)
						}
						continue
					}

					for _, xstream := range res {
						for _, msg := range xstream.Messages {
							processMessage(ctx, rdb, producer, xstream.Stream, group, msg)
						}
					}
				}
			}(symbol, producer, startSlot, endSlot)
		}
	}
}

func processMessage(ctx context.Context, rdb *redis.Client, producer pulsar.Producer, stream, group string, msg redis.XMessage) {
	val, ok := msg.Values["payload"]
	if !ok {
		rdb.XAck(ctx, stream, group, msg.ID)
		rdb.XDel(ctx, stream, msg.ID)
		return
	}
	payload := val.(string)

	// 使用 oid 作为 Pulsar 消息 Key
	msgKey := msg.ID
	if oid, ok := msg.Values["oid"]; ok {
		msgKey = oid.(string)
	}

	_, err := producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: []byte(payload),
		Key:     msgKey,
	})

	if err != nil {
		fmt.Printf("Pulsar send failed: %v\n", err)
		return
	}

	// 成功后 Ack 并删除，维持 Stream 整洁
	pipe := rdb.Pipeline()
	pipe.XAck(ctx, stream, group, msg.ID)
	pipe.XDel(ctx, stream, msg.ID)
	_, _ = pipe.Exec(ctx)
}

func isGroupExistErr(err error) bool {
	return err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists"
}
