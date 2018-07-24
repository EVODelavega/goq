package goq

import (
	"context"
)

type Topic interface {
	Start(ctx context.Context) chan<- PublishMsg
	Stop() error
}
