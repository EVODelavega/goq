package aws

import (
	"context"
	"fmt"
	"time"

	"github.com/EVODelavega/goq"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
)

type subSQS struct {
	goq.Consumer
	conf     Config
	client   *sqs.SQS
	queueURL *string
	ch       chan goq.BaseMsg
	dch      chan *sqs.Message
	newCB    MessageWrapper
	stopCB   context.CancelFunc
}

type pubSQS struct {
	goq.Publisher
	conf     Config
	client   *sqs.SQS
	queueURL *string
	ch       chan goq.BaseMsg
	stopCB   context.CancelFunc
}

// NewPublsiher - publish to queue, create if needed
func NewPublsiher(conf Config, sess *session.Session) (goq.Publisher, error) {
	var (
		queueURL *string
	)
	awsConf, err := conf.GetAWSConfig()
	if err != nil {
		// we can't do anything -> no credential info
		// we should be able to use a session, though
		if sess == nil {
			return nil, err
		}
		awsConf = sess.Config
	}
	if sess == nil {
		sess = session.New(awsConf)
	}
	// pass in the specific conf to override anything that may or may not have been in session
	sc := sqs.New(sess, awsConf)
	if !conf.CreateQueue {
		if queueURL, err = getQueue(conf, sess, sc); err != nil {
			return nil, err
		}
		return &pubSQS{
			conf:     conf,
			client:   sc,
			queueURL: queueURL,
		}, nil
	}
	if queueURL, err = createQueue(sc, conf); err != nil {
		return nil, err
	}
	return &pubSQS{
		conf:     conf,
		client:   sc,
		queueURL: queueURL,
	}, nil
}

// NewConsumer - consume from given queue, create if needed and subscribe to topic if desired
func NewConsumer(conf Config, newCB MessageWrapper, sess *session.Session) (goq.Consumer, error) {
	var (
		queueURL *string
	)
	if newCB == nil {
		newCB = NewMessage
	}
	awsConf, err := conf.GetAWSConfig()
	if err != nil {
		if sess == nil {
			return nil, err
		}
		awsConf = sess.Config
	}
	if sess == nil {
		// create session using config
		sess = session.New(awsConf)
	}
	sc := sqs.New(sess, awsConf)
	if !conf.CreateQueue {
		if queueURL, err = getQueue(conf, sess, sc); err != nil {
			return nil, err
		}
		return &subSQS{
			conf:     conf,
			client:   sc,
			queueURL: queueURL,
			newCB:    newCB,
		}, nil
	}
	if queueURL, err = createQueue(sc, conf); err != nil {
		return nil, err
	}
	if err := subscribeQueue(sess, conf.TopicArn, queueURL); err != nil {
		log.Errorf("Failed to subscribe queue %s to topic: %+v", *queueURL, err)
		return nil, err
	}
	return &subSQS{
		conf:     conf,
		client:   sc,
		queueURL: queueURL,
	}, nil
}

func getQueue(conf Config, sess *session.Session, sc *sqs.SQS) (*string, error) {
	out, err := sc.GetQueueUrl(
		&sqs.GetQueueUrlInput{
			QueueName:              &conf.QueueName,
			QueueOwnerAWSAccountId: &conf.AccountID,
		},
	)
	if err != nil {
		return nil, err
	}
	// No topic set, so we don't have to create the subscription here
	if conf.TopicArn == nil {
		return out.QueueUrl, nil
	}
	if err := subscribeQueue(sess, conf.TopicArn, out.QueueUrl); err != nil {
		log.Errorf("Failed to subscribe queue %s to topic: %+v", *out.QueueUrl, err)
		return nil, err
	}
	return out.QueueUrl, nil
}
func (s *subSQS) Start(ctx context.Context) <-chan goq.BaseMsg {
	s.ch = make(chan goq.BaseMsg, s.conf.BatchSize)
	// dch is not exposed, so it handles raw SQS type
	s.dch = make(chan *sqs.Message, s.conf.BatchSize)
	ctx, s.stopCB = context.WithCancel(ctx)
	// start routines for consuming and deleting messages...
	go s.delLoop(ctx)
	go s.consumeLoop(ctx)
	return s.ch
}

func (s *subSQS) Stop() error {
	if s.stopCB == nil {
		return goq.NotStartedErr
	}
	s.stopCB()
	return nil
}

func (s *subSQS) consumeLoop(ctx context.Context) {
	wait := aws.Int64(s.conf.WaitTimeSeconds)
	batch := aws.Int64(s.conf.BatchSize)
	for {
		select {
		case <-ctx.Done():
			close(s.ch)
			return
		default:
			rec, err := s.client.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
				MaxNumberOfMessages: batch,
				QueueUrl:            s.queueURL,
				WaitTimeSeconds:     wait,
			})
			if err != nil {
				log.Errorf("Failed to receive messages: %+v", err)
			} else {
				log.Infof("Received %d messages", len(rec.Messages))
				for _, msg := range rec.Messages {
					if wrapped, err := s.newCB(msg, s.dch); err == nil {
						s.ch <- wrapped
					} else {
						log.Warnf("Failed to wrap message: %#v -> %+v", msg, err)
					}
				}
			}
		}
	}
}

func (s *subSQS) delLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			close(s.dch)
			return
		case msg := <-s.dch:
			out, err := s.client.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      s.queueURL,
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				log.Errorf("Failed to delete (ack) message %s from queue %s (%+v)", msg.String(), *s.queueURL, err)
			} else {
				log.Infof("Deleted (ack) message %s from queue %s (%s)", msg.String(), *s.queueURL, out.String())
			}
		}
	}
}

// Start - implement publisher interface
func (s *pubSQS) Start(ctx context.Context) chan<- goq.BaseMsg {
	s.ch = make(chan goq.BaseMsg, s.conf.BatchSize)
	ctx, s.stopCB = context.WithCancel(ctx)
	// @TODO start publishing loop
	go s.publishLoop(ctx)
	return s.ch
}

// Stop - implement publisher interface
func (s *pubSQS) Stop() error {
	if s.stopCB == nil {
		return goq.NotStartedErr
	}
	s.stopCB()
	return nil
}

func (s *pubSQS) publishLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.conf.WaitTimeSeconds) * time.Second)
	var buf []goq.BaseMsg
	if s.conf.BatchSize > 0 {
		buf = make([]goq.BaseMsg, 0, s.conf.BatchSize)
	}
	delay := aws.Int64(s.conf.WaitTimeSeconds)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			close(s.ch)
			return
		case <-ticker.C:
			// only do something if there's something in the buffer to begin with
			if len(buf) > 0 {
				s.sendBatch(ctx, buf, delay)
				// clear slice, but keep cap
				buf = buf[:0]
			}
		case msg := <-s.ch:
			if s.conf.BatchSize > 0 {
				buf = append(buf, msg)
				// cap and len are equal, we have reached the batch size
				if len(buf) == cap(buf) {
					s.sendBatch(ctx, buf, delay)
					// clear buffer, but keep cap
					buf = buf[:0]
					// restart ticker if we've just sent a batch through
					ticker.Stop()
					ticker = time.NewTicker(time.Duration(s.conf.WaitTimeSeconds) * time.Second)
				}
			} else {
				// just send one by one
				// technically, we could call this in a routine, but that's what batching would be for anyway
				s.sendMessage(ctx, msg, delay)
			}
		}
	}
}

func (p *pubSQS) sendMessage(ctx context.Context, msg goq.BaseMsg, delay *int64) {
	body := msg.Body()
	attr := msg.Attributes()
	msgAttr := map[string]*sqs.MessageAttributeValue{}
	if len(attr) > 0 {
		for k, a := range attr {
			msgAttr[k] = &sqs.MessageAttributeValue{
				DataType:    aws.String(a.Type),
				StringValue: aws.String(a.Value),
			}
		}
	}
	in := sqs.SendMessageInput{
		DelaySeconds:      delay,
		QueueUrl:          p.queueURL,
		MessageBody:       &body,
		MessageAttributes: msgAttr,
	}
	if out, err := p.client.SendMessageWithContext(ctx, &in); err != nil {
		msg.SetError(err)
	} else {
		msg.SetId(*out.MessageId)
	}
}

func (p *pubSQS) sendBatch(ctx context.Context, buffer []goq.BaseMsg, delay *int64) {
	in := sqs.SendMessageBatchInput{
		Entries:  make([]*sqs.SendMessageBatchRequestEntry, 0, len(buffer)),
		QueueUrl: p.queueURL,
	}
	idMap := map[string]int{}
	for i, msg := range buffer {
		body := msg.Body()
		attr := msg.Attributes()
		id := fmt.Sprintf("%d", i)
		idMap[id] = i
		msgAttr := map[string]*sqs.MessageAttributeValue{}
		if len(attr) > 0 {
			for k, a := range attr {
				msgAttr[k] = &sqs.MessageAttributeValue{
					DataType:    aws.String(a.Type),
					StringValue: aws.String(a.Value),
				}
			}
		}
		in.Entries = append(
			in.Entries,
			&sqs.SendMessageBatchRequestEntry{
				Id:                aws.String(id),
				DelaySeconds:      delay,
				MessageBody:       &body,
				MessageAttributes: msgAttr,
			},
		)
	}
	out, err := p.client.SendMessageBatchWithContext(ctx, &in, nil)
	if err != nil {
		for _, msg := range buffer {
			msg.SetError(err)
			return
		}
	}
	for _, f := range out.Failed {
		i := idMap[*f.Id]
		// @TODO set proper error
		buffer[i].SetError(newBatchError(f))
	}
	for _, s := range out.Successful {
		i := idMap[*s.Id]
		buffer[i].SetId(*s.MessageId)
	}
}

func createQueue(sc *sqs.SQS, conf Config) (*string, error) {
	out, err := sc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: &conf.QueueName,
	})
	if err != nil {
		return nil, err
	}
	return out.QueueUrl, nil
}

func subscribeQueue(sess *session.Session, topicArn, queueURL *string) error {
	topic := sns.New(sess)
	out, err := topic.Subscribe(&sns.SubscribeInput{
		TopicArn: topicArn,
		Protocol: aws.String("sqs"),
		Endpoint: queueURL,
	})
	if err != nil {
		return err
	}
	log.Infof("Succesfully subscribed queue %s to topic %s -> subscription Arn: %s", *queueURL, *topicArn, *out.SubscriptionArn)
	return nil
}
