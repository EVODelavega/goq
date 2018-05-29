package goq

import (
	"context"
)

// Consumer - basic consumer implementation
type Consumer interface {
	Start(ctx context.Context) <-chan BaseMsg
	Stop() error
}
