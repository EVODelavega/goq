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
	AccessKey string
	SecretKey string
	Token     string
	Region    string
}

type Config struct {
	AccountID       string
	CreateQueue     bool
	BatchSize       int64
	WaitTimeSeconds int64
	QueueName       string
	QueueUrl        *string
	TopicArn        *string
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
