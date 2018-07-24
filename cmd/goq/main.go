package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/EVODelavega/goq/example"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

func main() {
	env := map[string]string{
		"AWS_ACCESS_KEY":        "some-random-key",
		"AWS_SECRET_KEY":        "some-secret-sauce",
		"AWS_TOKEN":             "token",
		"AWS_REGION":            "eu-west-1",
		"AWS_ACCOUNT_ID":        "1",
		"AWS_CREATE_QUEUE":      "false",
		"AWS_WAIT_TIME_SECONDS": "1ms",
		"AWS_QUEUE_URL":         "q.com",
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
	example.StartPublisher(ctx, pub)
}

func createTopic() (*string, error) {
	sess := session.New()
	sc := sns.New(sess, aws.NewConfig().WithRegion("eu-west-1"))
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
