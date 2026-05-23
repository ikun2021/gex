package engine

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
)

// snapshotJSON Redis 持久化结构（pulsar_msg_id 使用 MessageID.Serialize 的 base64）。
type snapshotJSON struct {
	Asks         []*InputMessage `json:"asks"`
	Bids         []*InputMessage `json:"bids"`
	CurrentMsgId int64           `json:"current_msg_id"`
	PulsarMsgID  string          `json:"pulsar_msg_id"`
}

func encodeMessageID(id pulsar.MessageID) string {
	if id == nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(id.Serialize())
}

func decodeMessageID(encoded string) (pulsar.MessageID, error) {
	if encoded == "" {
		return nil, nil
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode pulsar message id: %w", err)
	}
	return pulsar.DeserializeMessageID(data)
}

// parsePulsarMsgIDJSON 兼容旧快照：曾直接把 pulsar.MessageID 接口 JSON 化（会变成 object，无法反序列化）。
func parsePulsarMsgIDJSON(raw json.RawMessage) (pulsar.MessageID, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err == nil && encoded != "" {
		return decodeMessageID(encoded)
	}
	var legacy struct {
		LedgerID     int64 `json:"ledgerID"`
		EntryID      int64 `json:"entryID"`
		BatchIdx     int32 `json:"batchIdx"`
		PartitionIdx int32 `json:"partitionIdx"`
		BatchSize    int32 `json:"batchSize"`
	}
	if err := json.Unmarshal(raw, &legacy); err != nil {
		return nil, fmt.Errorf("parse pulsar_msg_id: %w", err)
	}
	if legacy.LedgerID == 0 && legacy.EntryID == 0 {
		return nil, nil
	}
	return pulsar.NewMessageID(legacy.LedgerID, legacy.EntryID, legacy.BatchIdx, legacy.PartitionIdx), nil
}

func marshalSnapshot(data *SnapshotData) ([]byte, error) {
	if data == nil {
		return json.Marshal(&snapshotJSON{})
	}
	return json.Marshal(&snapshotJSON{
		Asks:         data.Asks,
		Bids:         data.Bids,
		CurrentMsgId: data.CurrentMsgId,
		PulsarMsgID:  encodeMessageID(data.PulsarMsgID),
	})
}

func unmarshalSnapshot(val string) (*SnapshotData, error) {
	if val == "" {
		return &SnapshotData{}, nil
	}
	var raw struct {
		Asks         []*InputMessage `json:"asks"`
		Bids         []*InputMessage `json:"bids"`
		CurrentMsgId int64           `json:"current_msg_id"`
		PulsarMsgID  json.RawMessage `json:"pulsar_msg_id"`
	}
	if err := json.Unmarshal([]byte(val), &raw); err != nil {
		return nil, err
	}
	msgID, err := parsePulsarMsgIDJSON(raw.PulsarMsgID)
	if err != nil {
		return nil, err
	}
	return &SnapshotData{
		Asks:         raw.Asks,
		Bids:         raw.Bids,
		CurrentMsgId: raw.CurrentMsgId,
		PulsarMsgID:  msgID,
	}, nil
}
