package goq

import (
	"context"
	"errors"
)

var (
	NotStartedErr = errors.New("consumer/publisher not started")
)

// Consumer - basic consumer implementation
type Consumer interface {
	Start(ctx context.Context) <-chan BaseMsg
	Stop() error
}

// Publisher - directly push messages onto SQS queue
type Publisher interface {
	Start(ctx context.Context) chan<- PublishMsg
	Stop() error
}
