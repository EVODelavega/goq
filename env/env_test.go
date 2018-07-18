package env

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test that we can inintate all the conmfig properties including inner propertines
func TestNestedVariablesAreSet(t *testing.T) {

	// Given:
	os.Setenv("AWS_ACCESS_KEY", "my-testing-access-key")
	os.Setenv("AWS_SECRET_KEY", "my-testing-secret-key")

	// And:
	os.Setenv("AWS_ACCOUNT_ID", "my-testing-account-id")
	os.Setenv("AWS_QUEUE_NAME", "my-testing-queue-name")

	// and:
	defer os.Clearenv()

	// When:
	config, err := Get()

	// Then:
	assert.Nil(t, err)
	assert.NotNil(t, config)

	// and
	assert.Equal(t, "my-testing-account-id", config.AccountID)
	assert.Equal(t, "my-testing-queue-name", config.QueueName)

	// and nested fields as well:
	assert.NotNil(t, config.Credentials)
	assert.Equal(t, "my-testing-access-key", config.Credentials.AccessKey)
	assert.Equal(t, "my-testing-secret-key", config.Credentials.SecretKey)

	// and optional fields are omitted:
	assert.Nil(t, config.QueueUrl)
	assert.Nil(t, config.TopicArn)
}

// some properties are optional and they are stored as pointers to string, this test checks that they work properly
func TestOptionalVariablesAreSet(t *testing.T) {

	// Given:
	os.Setenv("AWS_QUEUE_URL", "http://test.queue")
	os.Setenv("AWS_TOPIC_ARN", "test-arn")

	// and:
	defer os.Clearenv()

	// When:
	config, err := Get()

	// Then:
	assert.Nil(t, err)
	assert.NotNil(t, config)

	// and:
	if assert.NotNil(t, config.QueueUrl) {
		assert.Equal(t, "http://test.queue", *config.QueueUrl)
	}

	if assert.NotNil(t, config.TopicArn) {
		assert.Equal(t, "test-arn", *config.TopicArn)
	}
}
