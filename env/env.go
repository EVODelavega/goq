package env

import (
	"reflect"

	"github.com/EVODelavega/goq/aws"

	"github.com/caarlos0/env"
)

var ptrToStringType = reflect.PtrTo(reflect.TypeOf(""))

// Get parses the variable environment and creates the config object
func Get() (*aws.Config, error) {

	credentials := &aws.Credentials{}
	if err := env.Parse(credentials); err != nil {
		return nil, err
	}

	config := &aws.Config{}

	// custom parser for the pointers of string
	customParsers := env.CustomParsers{
		ptrToStringType: fnGetStringPointer,
	}

	if err := env.ParseWithFuncs(config, customParsers); err != nil {
		return nil, err
	}

	config.Credentials = credentials

	return config, nil
}

// Optional properties are presented as pointers allowing to have nil's when value were not provided.
// This simple function returns pointer only in case when string value is not empty
func fnGetStringPointer(v string) (interface{}, error) {
	if len(v) > 0 {
		return &v, nil
	}
	return nil, nil
}
