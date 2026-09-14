package producer

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestNewWriterRequiresDurableAcknowledgement(t *testing.T) {
	writer := newWriter("kafka:29092")
	defer writer.Close()

	if writer.RequiredAcks != kafka.RequireAll {
		t.Fatalf("RequiredAcks = %v, want %v", writer.RequiredAcks, kafka.RequireAll)
	}
}
