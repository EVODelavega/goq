package example

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/EVODelavega/goq"
	"github.com/EVODelavega/goq/aws"

	"github.com/aws/aws-sdk-go/service/sqs"
)

// MyData - interface to go with MyMessage for easier type-assertions
type MyData interface {
	GetName() string
	GetAddress() string
	GetAge() int
	GetTimeSent() time.Time
}

type MyMessage struct {
	// This is an embedded interface type, check New func for details
	goq.BaseMsg
	// my message's specific stuff
	Name    string `json:"name"`
	Address string `json:"address"`
	Age     int    `json:"age"`
	sentTS  time.Time
}

// NewMyMessage - type-specific callback to wrap message
func NewMyMessage(raw *sqs.Message, dch chan<- *sqs.Message) (goq.BaseMsg, error) {
	baseMsg, err := aws.NewMessage(raw, dch)
	if err != nil {
		return nil, err
	}
	// start populating the type-specific fields
	t, err := time.Parse(time.RFC3339, *raw.MessageAttributes["time_sent"].StringValue)
	if err != nil {
		return nil, err
	}
	myMsg := MyMessage{
		sentTS: t,
	}
	// explicitly set the embedded interface -> we are guaranteed to implement it now
	myMsg.BaseMsg = baseMsg
	// unmarshal body into type
	if err := json.Unmarshal([]byte(*raw.Body), &myMsg); err != nil {
		return nil, err
	}
	return &myMsg, nil
}

func (m MyMessage) GetName() string {
	return m.Name
}

func (m MyMessage) GetAddress() string {
	return m.Address
}

func (m MyMessage) GetAge() int {
	return m.Age
}

func (m MyMessage) GetTimeSent() time.Time {
	return m.sentTS
}

func useConsumer() {
	conf := aws.Config{}
	// pass NewMyMessage here, this consumer is now tied to the type of message we're consuming
	consumer, _ := aws.New(conf, nil, NewMyMessage)

	ctx, cfunc := context.WithTimeout(context.Background(), time.Minute)
	defer cfunc()
	ch := consumer.Start(ctx)
	for msg := range ch {
		myMsg := msg.(MyData) // type-assert interface because it's more efficient here
		fmt.Printf("Message for %s (address: %s) aged %d was sent %v\n", myMsg.GetName(), myMsg.GetAddress(), myMsg.GetAge(), myMsg.GetTimeSent())
		// done processing:
		msg.Ack()
	}
}
