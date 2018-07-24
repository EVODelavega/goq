package aws

import (
	"github.com/EVODelavega/goq"
)

// MessageMarshaller - callback used to marshal messages according to requirements
type MessageMarshaller func(goq.PublishMsg) ([]byte, error)

// AWSPublishMessage - Default AWS publish message type
type AWSPublishMessage struct {
	goq.PublishMsg
	body, subject string
	attributes    map[string]goq.MessageAttribute
	id            string
	err           error
}

// NewPublishMessage - create a goq.BaseMsg better suited for marshalling
func NewPublishMessage(body, subject string, attributes map[string]goq.MessageAttribute) *AWSPublishMessage {
	msg := AWSPublishMessage{
		body:    body,
		subject: subject,
	}
	if attributes != nil {
		msg.attributes = attributes
	}
	return &msg
}

// JSONMessage - MessageMarshaller used by default -> sends messages as JSON
func JSONMessage(msg goq.PublishMsg) ([]byte, error) {
	// this assumes the body returns a string that is already marshalled
	return []byte(msg.Body()), nil
}

func (m AWSPublishMessage) Error() error {
	return m.err
}

func (m AWSPublishMessage) Attributes() map[string]goq.MessageAttribute {
	return m.attributes
}

func (m AWSPublishMessage) Body() string {
	return m.body
}

func (m AWSPublishMessage) ID() string {
	return m.id
}

func (m AWSPublishMessage) GetSubject() string {
	return m.subject
}

func (m *AWSPublishMessage) SetId(id string) {
	m.id = id
}

func (m *AWSPublishMessage) SetError(err error) {
	m.err = err
}

func (m *AWSPublishMessage) AddAttribute(name string, attr goq.MessageAttribute) {
	m.attributes[name] = attr
}
