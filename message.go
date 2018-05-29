package goq

// BaseMsg - core interface for message handling
type BaseMsg interface {
	Ack()
	Nack()
}
