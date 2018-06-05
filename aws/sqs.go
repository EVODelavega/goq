package aws

import (
	"context"

	"github.com/EVODelavega/goq"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
)

type subSQS struct {
	conf     Config
	client   *sqs.SQS
	queueURL *string
	ch       chan goq.BaseMsg
	dch      chan *sqs.Message
	newCB    MessageWrapper
	stopCB   context.CancelFunc
}

func New(conf Config, sess *session.Session, newCB MessageWrapper) (goq.Consumer, error) {
	// create session if none passed
	var (
		err      error
		queueURL *string
	)
	if newCB == nil {
		newCB = NewMessage
	}
	if sess == nil {
		if sess, err = session.NewSession(); err != nil {
			return nil, err
		}
	}
	var creds *credentials.Credentials
	if conf.Credentials != nil {
		creds = credentials.NewStaticCredentials(conf.Credentials.AccessKey, conf.Credentials.SecretKey, conf.Credentials.Token)
	} else {
		creds = credentials.NewEnvCredentials()
	}
	sc := sqs.New(sess, &aws.Config{
		Credentials: creds,
		Region:      &conf.Credentials.Region,
	})
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
		newCB:    newCB,
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
				if len(rec.Messages) > 0 {
					log.Infof("Received %d messages", len(rec.Messages))
				}
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
