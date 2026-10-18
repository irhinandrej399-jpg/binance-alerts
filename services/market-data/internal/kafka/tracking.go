package kafka

import (
	"context"
	"errors"
	"example.com/binance-alerts/services/market-data/internal/tracking"
	"fmt"
	"github.com/IBM/sarama"
	"time"
)

type TrackingRepository interface {
	tracking.Repository
	RejectTracking(context.Context, string, int32, int64, string) error
}
type TrackingHandler struct {
	Repository TrackingRepository
	RetryDelay time.Duration
	OnError    func(error)
}

func (h *TrackingHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *TrackingHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *TrackingHandler) report(err error) {
	if h.OnError != nil {
		h.OnError(err)
	}
}
func (h *TrackingHandler) delay() time.Duration {
	if h.RetryDelay > 0 {
		return h.RetryDelay
	}
	return time.Second
}
func consumerWait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func (h *TrackingHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()
	for {
		select {
		case <-ctx.Done():
			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			for ctx.Err() == nil {
				attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := tracking.Handle(attemptCtx, message.Key, message.Value, h.Repository)
				if errors.Is(err, tracking.ErrInvalidCommand) || errors.Is(err, tracking.ErrVersionConflict) {
					reason := "invalid_command"
					if errors.Is(err, tracking.ErrVersionConflict) {
						reason = "version_conflict"
					}
					err = h.Repository.RejectTracking(attemptCtx, message.Topic, message.Partition, message.Offset, reason)
					if err == nil {
						h.report(fmt.Errorf("tracking command rejected: topic=%s partition=%d offset=%d reason=%s", message.Topic, message.Partition, message.Offset, reason))
					}
				}
				cancel()
				if err == nil {
					if ctx.Err() == nil {
						session.MarkMessage(message, "")
					}
					break
				}
				h.report(err)
				if !consumerWait(ctx, h.delay()) {
					return nil
				}
			}
		}
	}
}

// RunTracking повторяет создание consumer group и сессий; transient-ошибки не теряют offset.
func RunTracking(ctx context.Context, brokers []string, groupID, topic string, h *TrackingHandler) error {
	conf := Config()
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	conf.Consumer.Return.Errors = true
	conf.Consumer.Group.Session.Timeout = 10 * time.Second
	conf.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	for ctx.Err() == nil {
		group, err := sarama.NewConsumerGroup(brokers, groupID, conf)
		if err != nil {
			h.report(err)
			if !consumerWait(ctx, h.delay()) {
				break
			}
			continue
		}
		errorsCtx, stopErrors := context.WithCancel(ctx)
		errorsDone := make(chan struct{})
		go func() {
			defer close(errorsDone)
			for {
				select {
				case err, ok := <-group.Errors():
					if !ok {
						return
					}
					h.report(err)
				case <-errorsCtx.Done():
					return
				}
			}
		}()
		for ctx.Err() == nil {
			if err = group.Consume(ctx, []string{topic}, h); err != nil {
				h.report(err)
				break
			}
		}
		stopErrors()
		closeErr := group.Close()
		<-errorsDone
		if closeErr != nil {
			h.report(closeErr)
		}
		if !consumerWait(ctx, h.delay()) {
			break
		}
	}
	return ctx.Err()
}
