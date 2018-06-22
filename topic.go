package goq

import (
	"context"
)

type Topic interface {
	Start(ctx context.Context) chan<- BaseMsg
	Stop() error
}
