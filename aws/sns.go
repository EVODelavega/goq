package aws

import (
	"context"
	"errors"

	"github.com/EVODelavega/goq"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

type prodSNS struct {
	conf   Config
	topic  *string
	client *sns.SNS
	stopCB context.CancelFunc
	ch     chan goq.BaseMsg
}

func NewSNSPublisher(conf Config, sess *session.Session) (*prodSNS, error) {
	if conf.TopicArn == nil {
		return nil, errors.New("topic ARN must be set")
	}
	var err error
	p := prodSNS{
		conf:  conf,
		topic: conf.TopicArn,
	}
	var creds *credentials.Credentials
	if conf.Credentials != nil {
		creds = credentials.NewStaticCredentials(conf.Credentials.AccessKey, conf.Credentials.SecretKey, conf.Credentials.Token)
	} else {
		creds = credentials.NewEnvCredentials()
	}
	if sess == nil {
		sess, err = session.NewSession()
		if err != nil {
			return nil, err
		}
	}
	p.client = sns.New(
		sess,
		&aws.Config{
			Credentials: creds,
			Region:      &conf.Credentials.Region,
		},
	)
	return &p, nil
}

func (p *prodSNS) Start(ctx context.Context, marshal MessageMarshaller) chan<- goq.BaseMsg {
	if p.ch != nil {
		return p.ch
	}
	p.ch = make(chan goq.BaseMsg, p.conf.BatchSize)
	ctx, p.stopCB = context.WithCancel(ctx)
	return p.ch
}

func (p *prodSNS) publishLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			close(p.ch)
			return
		case msg := <-p.ch:
			body := msg.Body()
			subj := msg.GetSubject()
			in := sns.PublishInput{
				TopicArn:          p.topic,
				Subject:           &subj,
				MessageAttributes: getSNSMessageAttributes(msg),
				Message:           &body,
			}
			out, err := p.client.PublishWithContext(ctx, &in, nil)
			if err != nil {
				msg.SetError(err)
				continue
			}
			out.SetMessageId(*out.MessageId)
		}
	}
}

// Stop - duh...
func (p *prodSNS) Stop() error {
	if p.stopCB == nil {
		return goq.NotStartedErr
	}
	p.stopCB()
	p.stopCB = nil
	return nil
}

func getSNSMessageAttributes(msg goq.BaseMsg) (r map[string]*sns.MessageAttributeValue) {
	attr := msg.Attributes()
	if len(attr) == 0 {
		return
	}
	r = map[string]*sns.MessageAttributeValue{}
	for k, a := range attr {
		r[k] = &sns.MessageAttributeValue{
			DataType:    &a.Type,
			StringValue: &a.Value,
		}
	}
	return
}
