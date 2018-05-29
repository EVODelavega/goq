package aws

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
