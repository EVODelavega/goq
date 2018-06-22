package aws

import (
	"github.com/EVODelavega/goq"

	"github.com/aws/aws-sdk-go/service/sqs"
)

// MessageWrapper - callback to use to wrap SQS messages into custom type
type MessageWrapper func(*sqs.Message, chan<- *sqs.Message) (goq.BaseMsg, error)

// MessageMarshaller - callback used to marshal messages according to requirements
type MessageMarshaller func(goq.BaseMsg) ([]byte, error)

// Default AWSMessage type
type AWSMessage struct {
	goq.BaseMsg
	subject string // used for SNS
	raw     *sqs.Message
	dch     chan<- *sqs.Message
	err     error
	id      string
}

// NewMessage - New AWS specific implementation of goq.BaseMsg
func NewMessage(raw *sqs.Message, dch chan<- *sqs.Message) (goq.BaseMsg, error) {
	return &AWSMessage{
		raw: raw,
		dch: dch,
	}, nil
}

// JSONMessage - MessageMarshaller used by default -> sends messages as JSON
func JSONMessage(msg goq.BaseMsg) ([]byte, error) {
	// this assumes the body returns a string that is already marshalled
	return []byte(msg.Body()), nil
}

// Ack - implementation of BaseMsg interface
func (m *AWSMessage) Ack() {
	m.dch <- m.raw
}

// Nack - implementation of BaseMsg interface, noop in AWS, really
func (m AWSMessage) Nack() {
}

// Error - check if message was published Succesfully, used by publisher only ATM
func (m AWSMessage) Error() error {
	return m.err
}

// Attributes - used for sending
func (m AWSMessage) Attributes() map[string]goq.MessageAttribute {
	if m.raw != nil {
		ret := map[string]goq.MessageAttribute{}
		for k, a := range m.raw.MessageAttributes {
			ret[k] = goq.MessageAttribute{
				Value: *a.StringValue,
				Type:  *a.DataType,
			}
		}
		return ret
	}
	return nil
}

// Body - get full body
func (m AWSMessage) Body() string {
	if m.raw != nil {
		return *m.raw.Body
	}
	return ""
}

func (m *AWSMessage) SetError(err error) {
	m.err = err
}

func (m *AWSMessage) SetId(id string) {
	m.id = id
}

func (m AWSMessage) ID() string {
	return m.id
}

// GetSubject
func (m AWSMessage) GetSubject() string {
	return m.subject
}
