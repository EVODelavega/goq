package aws

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

var (
	NoCredentialsErr      = errors.New("no credentials configured")
	CredentialsExpiredErr = errors.New("environment credentials invalid/expired")
)

type Credentials struct {
	AccessKey string `env:"AWS_ACCESS_KEY"`
	SecretKey string `env:"AWS_SECRET_KEY"`
	Token     string `env:"AWS_TOKEN"`
	Region    string `env:"AWS_REGION"`
}

type Config struct {
	AccountID       string  `env:"AWS_ACCOUNT_ID"`
	CreateQueue     bool    `env:"AWS_CREATE_QUEUE"`
	BatchSize       int64   `env:"AWS_BATCH_SIZE"`
	WaitTimeSeconds int64   `env:"AWS_WAIT_TIME_SECONDS"`
	QueueName       string  `env:"AWS_QUEUE_NAME"`
	QueueUrl        *string `env:"AWS_QUEUE_URL"`
	TopicArn        *string `env:"AWS_TOPIC_ARN"`
	Credentials     *Credentials
}

// GetCredentials - returs credentials object using config or env
func (c Config) GetCredentials() (*credentials.Credentials, error) {
	if c.Credentials == nil {
		return nil, NoCredentialsErr
	}
	if c.Credentials.AccessKey != "" && c.Credentials.SecretKey != "" {
		cred := credentials.NewStaticCredentials(
			c.Credentials.AccessKey,
			c.Credentials.SecretKey,
			c.Credentials.Token,
		)
		return cred, nil
	}
	cred := credentials.NewEnvCredentials()
	if cred.IsExpired() {
		return nil, CredentialsExpiredErr
	}
	return cred, nil
}

// GetAWSConfig - get config + creds for AWS
func (c Config) GetAWSConfig() (*aws.Config, error) {
	cred, err := c.GetCredentials()
	if err != nil {
		return nil, err
	}
	return &aws.Config{
		Credentials: cred,
		Region:      aws.String(c.Credentials.Region),
	}, nil
}
