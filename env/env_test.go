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
}
