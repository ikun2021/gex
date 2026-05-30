package engine

import (
	"encoding/json"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
)

func TestSnapshotMessageIDRoundTrip(t *testing.T) {
	orig := pulsar.NewMessageID(808479063773317, 808479069040773, 0, 0)
	data := &SnapshotData{
		CurrentMsgId: 42,
		Version:      100,
		PulsarMsgID:  orig,
	}
	raw, err := marshalSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	got, err := unmarshalSnapshot(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	if got.CurrentMsgId != 42 {
		t.Fatalf("msg id: got %d want 42", got.CurrentMsgId)
	}
	if got.Version != 100 {
		t.Fatalf("version: got %d want 100", got.Version)
	}
	if got.PulsarMsgID == nil || got.PulsarMsgID.String() != orig.String() {
		t.Fatalf("pulsar id: got %v want %v", got.PulsarMsgID, orig)
	}
}

func TestUnmarshalLegacyPulsarMsgIDObject(t *testing.T) {
	legacy := `{"asks":[],"bids":[],"current_msg_id":1,"pulsar_msg_id":{"ledgerID":1,"entryID":2,"batchIdx":0,"partitionIdx":0}}`
	got, err := unmarshalSnapshot(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if got.PulsarMsgID == nil {
		t.Fatal("expected pulsar message id")
	}
	if got.PulsarMsgID.LedgerID() != 1 || got.PulsarMsgID.EntryID() != 2 {
		t.Fatalf("unexpected id: %s", got.PulsarMsgID.String())
	}
}

func TestMarshalDoesNotEmitMessageIDObject(t *testing.T) {
	data := &SnapshotData{
		PulsarMsgID: pulsar.NewMessageID(1, 2, 0, 0),
	}
	raw, err := marshalSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	var s string
	if err := json.Unmarshal(m["pulsar_msg_id"], &s); err != nil {
		t.Fatalf("pulsar_msg_id should be string, got %s", string(m["pulsar_msg_id"]))
	}
}
