package env

import (
	"github.com/EVODelavega/goq/aws"

	"github.com/caarlos0/env"
)

// Get parses the variable environment and creates the config object
func Get() (*aws.Config, error) {

	credentials := &aws.Credentials{}
	if err := env.Parse(credentials); err != nil {
		return nil, err
	}

	config := &aws.Config{}
	if err := env.Parse(config); err != nil {
		return nil, err
	}

	config.Credentials = credentials

	return config, nil
}
