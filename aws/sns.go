package aws

import (
	"context"
	"errors"

	"github.com/EVODelavega/goq"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

type prodSNS struct {
	conf    Config
	topic   *string
	client  *sns.SNS
	marshal MessageMarshaller
	stopCB  context.CancelFunc
	ch      chan goq.PublishMsg
}

func NewSNSPublisher(conf Config, marshal MessageMarshaller, sess *session.Session) (goq.Publisher, error) {
	if conf.TopicArn == nil {
		return nil, errors.New("topic ARN must be set")
	}
	awsConf, err := conf.GetAWSConfig()
	if err != nil {
		if sess == nil {
			return nil, err
		}
		awsConf = sess.Config
	}
	if marshal == nil {
		marshal = JSONMessage
	}
	// session contains credentials and region
	if sess != nil {
		return &prodSNS{
			conf:  conf,
			topic: conf.TopicArn,
			client: sns.New(
				sess,
				awsConf,
			),
			marshal: marshal,
		}, nil
	}
	// create new session if needed
	sess = session.New(awsConf)
	return &prodSNS{
		conf:  conf,
		topic: conf.TopicArn,
		client: sns.New(
			sess,
			awsConf,
		),
		marshal: marshal,
	}, nil
}

// Start - start publisher, returns channel used to publish
func (p *prodSNS) Start(ctx context.Context) chan<- goq.PublishMsg {
	if p.ch != nil {
		return p.ch
	}
	// buffer to 1, so we're less likely to actually interrupt the routine publishing
	// while at the same time a shutdown doesn't risk losing a ton of data
	// what we could do have multiple publishLoop routines running, but that's a bit iffy
	p.ch = make(chan goq.PublishMsg, 1)
	ctx, p.stopCB = context.WithCancel(ctx)
	go p.publishLoop(ctx)
	return p.ch
}

func (p *prodSNS) publishLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			close(p.ch)
			return
		case msg := <-p.ch:
			b, err := p.marshal(msg)
			if err != nil {
				msg.SetError(err)
				continue
			}
			body := string(b)
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

func getSNSMessageAttributes(msg goq.PublishMsg) (r map[string]*sns.MessageAttributeValue) {
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
