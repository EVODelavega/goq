package example

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/EVODelavega/goq"
	"github.com/EVODelavega/goq/aws"

	"github.com/aws/aws-sdk-go/aws/session"
)

type myPublishMsg struct {
	*aws.AWSPublishMessage
	// custom fields, add these for custom marshalling, for example
	publishTime time.Time
}

type MyPublishMsg interface {
	goq.PublishMsg
	PublishTime() time.Time
	SetPublishTime(t time.Time)
}

func myPublishMsgMarshaller(msg goq.PublishMsg) ([]byte, error) {
	myMsg, ok := msg.(MyPublishMsg)
	if !ok {
		return nil, errors.New("myPublishMsgMarshaller only works with MyPublishMsg compliant messages")
	}
	_ = myMsg.PublishTime() // ensure publish time attribute is select
	return aws.JSONMessage(myMsg)
}

func GetPublisher() (goq.Publisher, error) {
	conf, err := aws.GetConfig()
	if err != nil {
		return nil, err
	}
	var sess *session.Session
	if awsConf, err := conf.GetAWSConfig(); err != nil {
		log.Printf("failed to load AWS config: %+v", err)
		sess = session.New()
	} else {
		sess = session.New(awsConf)
	}
	return aws.NewSNSPublisher(
		*conf,
		myPublishMsgMarshaller,
		sess,
	)
}

func startPublisher(p goq.Publisher) {
	ctx, cfunc := context.WithTimeout(context.Background(), time.Minute)
	defer cfunc()
	ch := p.Start(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg := myPublishMsg{
				AWSPublishMessage: aws.NewPublishMessage("body", "subject", nil),
			}
			// send message
			ch <- msg
		}
	}
}

func (m *myPublishMsg) PublishTime() time.Time {
	if m.publishTime.IsZero() {
		m.SetPublishTime(time.Now())
	}
	return m.publishTime
}

func (m *myPublishMsg) SetPublishTime(t time.Time) {
	m.publishTime = t
	// ensure attribute is set to correct value
	m.AddAttribute(
		"publish_time",
		goq.MessageAttribute{
			Type:  "string",
			Value: t.Format(time.RFC3339),
		},
	)
}
