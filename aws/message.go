package aws

import (
	"github.com/EVODelavega/goq"

	"github.com/aws/aws-sdk-go/service/sqs"
)

// MessageWrapper - callback to use to wrap SQS messages into custom type
type MessageWrapper func(*sqs.Message, chan<- *sqs.Message) (goq.BaseMsg, error)

// Default AWSMessage type
type AWSMessage struct {
	goq.BaseMsg
	raw *sqs.Message
	dch chan<- *sqs.Message
}

// NewMessage - New AWS specific implementation of goq.BaseMsg
func NewMessage(raw *sqs.Message, dch chan<- *sqs.Message) (goq.BaseMsg, error) {
	return &AWSMessage{
		raw: raw,
		dch: dch,
	}, nil
}

// Ack - implementation of BaseMsg interface
func (m *AWSMessage) Ack() {
	m.dch <- m.raw
}

// Nack - implementation of BaseMsg interface, noop in AWS, really
func (m AWSMessage) Nack() {
}
