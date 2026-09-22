package destination

import (
	"context"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Observer measures pipeline health from the consumer side: end-to-end events
// applied, poison events quarantined, and the group's lag against the topic's
// log end. The lag number is exactly "how far behind Kafka the writer is",
// which is the answer to the retention-versus-destination-lag question (#22).
type Observer struct {
	client *kgo.Client
	admin  *kadm.Client
	topic  string
	group  string

	applied     atomic.Uint64
	duplicates  atomic.Uint64
	deadLetters atomic.Uint64
}

func NewObserver(client *kgo.Client, topic, group string) *Observer {
	return &Observer{
		client: client,
		admin:  kadm.NewClient(client),
		topic:  topic,
		group:  group,
	}
}

// Start runs the metrics and lag loop until the context is done.
func (observer *Observer) Start(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				observer.logMetrics(ctx)
			}
		}
	}()
}

func (observer *Observer) logMetrics(ctx context.Context) {
	end, err := observer.admin.ListEndOffsets(ctx, observer.topic)
	if err != nil {
		log.Printf("warn stage=consumer_metrics topic=%s err=%v", observer.topic, err)
		return
	}
	start, err := observer.admin.ListStartOffsets(ctx, observer.topic)
	if err != nil {
		log.Printf("warn stage=consumer_metrics topic=%s err=%v", observer.topic, err)
		return
	}
	commits, err := observer.admin.FetchOffsetsForTopics(ctx, observer.group, observer.topic)
	if err != nil {
		log.Printf("warn stage=consumer_metrics group=%s topic=%s err=%v", observer.group, observer.topic, err)
		return
	}

	endOffset, endOK := offsetOf(end.Lookup(observer.topic, 0))
	startOffset, startOK := offsetOf(start.Lookup(observer.topic, 0))
	committed := int64(-1)
	if response, ok := commits.Lookup(observer.topic, 0); ok && response.Err == nil {
		committed = response.At
	}

	produced, consumed, lag := int64(-1), int64(-1), int64(-1)
	if endOK && startOK && endOffset >= startOffset {
		produced = endOffset - startOffset
		if committed >= 0 {
			consumed = committed - startOffset
			if endOffset >= committed {
				lag = endOffset - committed
			}
		}
	}

	// #22 retention-versus-lag: Kafka emulates unbounded log growth by evicting
	// the oldest segments once they pass the topic's retention window. If the
	// committed offset slips behind log_start, already-evicted records would be
	// needed: the destination is permanently missing that data. Catching it here
	// (and keeping lag comfortably inside the retention window) is the alarm.
	retentionMs := observer.topicRetention(ctx)
	if startOK && committed >= 0 && committed < startOffset {
		log.Printf("err stage=retention_outran_destination committed=%d < log_start=%d topic=%s: "+
			"Kafka evicted records the destination never consumed; data is lost. "+
			"Reduce lag or raise the topic retention window",
			committed, startOffset, observer.topic)
	}
	log.Printf(
		"trace stage=consumer_metrics topic=%s group=%s log_start=%d log_end=%d produced=%d committed=%d consumed=%d lag=%d retention_ms=%d applied=%d duplicates=%d dead_letters=%d",
		observer.topic, observer.group,
		startOffset, endOffset,
		produced,
		committed, consumed, lag, retentionMs,
		observer.applied.Load(), observer.duplicates.Load(), observer.deadLetters.Load(),
	)
}

// topicRetention reads the topic's configured retention window in milliseconds.
// Unknown or unset values report 0 and the reader of the value simply omits the
// retention field from its metrics line.
func (observer *Observer) topicRetention(ctx context.Context) int64 {
	configs, err := observer.admin.DescribeTopicConfigs(ctx, observer.topic)
	if err != nil {
		return 0
	}
	config, err := configs.On(observer.topic, nil)
	if err != nil || config.Err != nil {
		return 0
	}
	for _, entry := range config.Configs {
		if entry.Key != "retention.ms" {
			continue
		}
		retention, err := strconv.ParseInt(entry.MaybeValue(), 10, 64)
		if err != nil {
			return 0
		}
		return retention
	}
	return 0
}

func (observer *Observer) RecordApplied() {
	observer.applied.Add(1)
}

func (observer *Observer) RecordDuplicate() {
	observer.duplicates.Add(1)
}

func (observer *Observer) RecordDeadLetter() {
	observer.deadLetters.Add(1)
}

func offsetOf(offset kadm.ListedOffset, ok bool) (int64, bool) {
	if !ok {
		return -1, false
	}
	if offset.Err != nil {
		return -1, false
	}
	return offset.Offset, true
}

// KafkaLagForDemo makes the computed lag accessible to documentation and tests.
func KafkaLagForDemo(produced, consumed int64) int64 {
	if produced < 0 || consumed < 0 || consumed > produced {
		return -1
	}
	return produced - consumed
}

// RetentionOutranConsumption reports whether Kafka has already evicted records
// the consumer group never read, i.e. the committed offset fell behind the start
// of the retained log (#22). This is the alarm state for a destination that
// cannot keep up with the source.
func RetentionOutranConsumption(committed, startOffset int64) bool {
	return committed >= 0 && committed < startOffset
}
