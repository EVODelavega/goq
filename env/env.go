package env

import (
	"os"

	goqaws "github.com/EVODelavega/goq/aws"

	"github.com/caarlos0/env"
)

// Get parses the variable environment and creates the config object
func Get() (*goqaws.Config, error) {

	credentials := &goqaws.Credentials{}
	if err := env.Parse(credentials); err != nil {
		return nil, err
	}

	config := &goqaws.Config{}
	if err := env.Parse(config); err != nil {
		return nil, err
	}

	// Parse manually properties, that are presented as pointers
	config.QueueUrl = getStringPtr(os.Getenv("AWS_QUEUE_URL"))
	config.TopicArn = getStringPtr(os.Getenv("AWS_TOPIC_ARN"))

	config.Credentials = credentials

	return config, nil
}

// Optional properties are presented as poitners allowing to have nil's when value were not provided.
// This simple function returns pointer only in case when string value is not empty
func getStringPtr(v string) *string {
	if len(v) > 0 {
		return &v
	}
	return nil
}
