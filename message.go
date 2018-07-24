package goq

type MessageAttribute struct {
	Value string
	Type  string
}

// BaseMsg - core interface for message handling
type BaseMsg interface {
	Ack()
	Nack()
	Error() error
	Attributes() map[string]MessageAttribute
	Body() string
	SetError(err error)
	SetId(id string)
	ID() string
}

// PublishMsg - Message type used for publishing
type PublishMsg interface {
	Error() error
	Attributes() map[string]MessageAttribute
	AddAttribute(name string, attr MessageAttribute)
	Body() string
	SetError(err error)
	SetId(id string)
	ID() string
	GetSubject() string
}
