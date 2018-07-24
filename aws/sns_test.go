package aws

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
)

func TestNewSNSPublisherErr(t *testing.T) {
	cfg := Config{}
	_, err := NewSNSPublisher(cfg, nil, nil)
	if err == nil {
		t.Fatal("Expected an error to be returned...")
	}
	if err.Error() != "topic ARN must be set" {
		t.Fatalf("Expected error to be raised on nil topicARN, instead saw %+v", err)
	}
	topic := "foobar"
	cfg.TopicArn = &topic
	_, err = NewSNSPublisher(cfg, nil, nil)
	if err == nil {
		t.Fatal("Expected an error to be returned")
	}
	if err.Error() != "no credentials configured" {
		t.Fatalf("Expected error to be raised on nil credentials, instead saw %+v", err)
	}
}

func TestNewSNSPublisherSuccess(t *testing.T) {
	sess := session.New()
	topic := "foobar"
	cfg := Config{
		TopicArn: &topic,
	}
	_, err := NewSNSPublisher(cfg, nil, sess)
	if err != nil {
		t.Fatalf("expected publisher to be created successfully, instead saw %+v", err)
	}
}
