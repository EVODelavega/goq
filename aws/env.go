package aws

import (
	"os"

	"github.com/caarlos0/env"
)

const (
	envVarAWSQueueURL = "AWS_QUEUE_URL"
	envVarAWSTopicARN = "AWS_TOPIC_ARN"
)

// GetConfig parses the variable environment and creates the config object
func GetConfig() (*Config, error) {

	credentials := &Credentials{}
	if err := env.Parse(credentials); err != nil {
		return nil, err
	}

	config := &Config{}

	if err := env.Parse(config); err != nil {
		return nil, err
	}

	// explicitly assign optional values, presented as pointers and can be nullable
	if val, found := os.LookupEnv(envVarAWSQueueURL); found {
		config.QueueUrl = &val
	}

	if val, found := os.LookupEnv(envVarAWSTopicARN); found {
		config.TopicArn = &val
	}

	config.Credentials = credentials

	return config, nil
}
