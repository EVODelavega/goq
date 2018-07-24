package example

import (
	"context"
	"errors"
	"fmt"
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
	endpoint := "http://localstack:4575"
	// localstack endpoint
	sess.Config.Endpoint = &endpoint
	return aws.NewSNSPublisher(
		*conf,
		myPublishMsgMarshaller,
		sess,
	)
}

func StartPublisher(ctx context.Context, p goq.Publisher) {
	ch := p.Start(ctx)
	count := 1
	attr := map[string]goq.MessageAttribute{
		"message_cnt": goq.MessageAttribute{
			Type:  "string",
			Value: "",
		},
	}
	for {
		a := attr["message_cnt"]
		a.Value = fmt.Sprintf("%d", count)
		attr["message_cnt"] = a
		count++
		select {
		case <-ctx.Done():
			return
		default:
			// send message
			ch <- myPublishMsg{
				AWSPublishMessage: aws.NewPublishMessage("body", "subject", attr),
			}
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
