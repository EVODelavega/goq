package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	goqaws "github.com/EVODelavega/goq/aws"
	"github.com/EVODelavega/goq/example"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

func main() {
	// let's wait for localstack
	time.Sleep(time.Second * 10)
	env := map[string]string{
		"AWS_ACCESS_KEY":        "some-random-key",
		"AWS_SECRET_KEY":        "some-secret-sauce",
		"AWS_TOKEN":             "token",
		"AWS_REGION":            "eu-west-1",
		"AWS_ACCOUNT_ID":        "1",
		"AWS_CREATE_QUEUE":      "true",
		"AWS_WAIT_TIME_SECONDS": "1",
		"AWS_QUEUE_NAME":        "myQ",
	}
	arn, err := createTopic()
	if err != nil {
		fmt.Printf("Failed to create topic: %+v\n", err)
		return
	}
	// set ARN for newly created topic
	env["AWS_TOPIC_ARN"] = *arn
	if err := setEnv(env); err != nil {
		log.Printf("Error setting environment variables: %+v\n", err)
		return
	}
	pub, err := example.GetPublisher()
	if err != nil {
		fmt.Printf("Failed to get publisher: %+v\n", err)
		return
	}
	ctx, cfunc := context.WithTimeout(context.Background(), time.Minute)
	defer cfunc()
	if err := startConsumer(ctx); err != nil {
		fmt.Printf("Error starting consumer: %+v\n", err)
		return
	}
	example.StartPublisher(ctx, pub)
	<-ctx.Done()
}

func startConsumer(ctx context.Context) error {
	conf, err := goqaws.GetConfig()
	if err != nil {
		fmt.Printf("Failed getting config: %+v\n", err)
		return err
	}
	awsC, err := conf.GetAWSConfig()
	if err != nil {
		fmt.Printf("Failed to get AWS config: %+v", err)
		return err
	}
	awsC.Endpoint = aws.String("http://localstack:4576")
	sess, err := session.NewSession(
		awsC,
	)
	if err != nil {
		fmt.Printf("Something wrong with the session: %+v\n", err)
		return err
	}
	cons, err := goqaws.NewConsumer(*conf, nil, sess)
	if err != nil {
		fmt.Printf("Failed to get consumer: %+v\n", err)
		return err
	}
	ch := cons.Start(ctx)
	go func() {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Context cancelled, done publishing and consuming")
				fmt.Println("Clearing messages in channel")
				for msg := range ch {
					fmt.Printf("received message %#v\n", msg)
				}
				return
			case msg := <-ch:
				fmt.Printf("Received message %#v\n", msg)
				fmt.Println("Acknowledge message")
				msg.Ack()
			}
		}
	}()
	return nil
}

func createTopic() (*string, error) {
	cred := credentials.NewStaticCredentials(
		"some-random-key",
		"some-secret-sauce",
		"token",
	)
	sess := session.New()
	sc := sns.New(
		sess,
		&aws.Config{
			Region:      aws.String("eu-west-1"),
			Credentials: cred,
			Endpoint:    aws.String("http://localstack:4575"),
		},
	)
	out, err := sc.CreateTopic(
		&sns.CreateTopicInput{
			Name: aws.String("test-topic"),
		},
	)
	if err != nil {
		return nil, err
	}
	return out.TopicArn, nil
}

func setEnv(vals map[string]string) error {
	for k, v := range vals {
		if err := os.Setenv(k, v); err != nil {
			return err
		}
	}
	return nil
}
