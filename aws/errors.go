package aws

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type batchError struct {
	code      string
	message   string
	awsString string
	goString  string
}

func newBatchError(fail *sqs.BatchResultErrorEntry) error {
	return batchError{
		code:      *fail.Code,
		message:   *fail.Message,
		awsString: fail.String(),
		goString:  fail.GoString(),
	}
}

// Error - make this a valid error type
func (b batchError) Error() string {
	return b.awsString
}

func (b batchError) GoString() string {
	return b.goString
}

// Formatter - just for +v formatting, really
func (b batchError) Format(f fmt.State, c rune) {
	switch c {
	case 'v':
		var str string
		if f.Flag('+') {
			str = fmt.Sprintf("%s - %s (%s)", b.code, b.message, b.goString)
		} else {
			str = fmt.Sprintf("%s - %s", b.code, b.message)
		}
		_, _ = f.Write([]byte(str))
	case 's':
		_, _ = f.Write([]byte(b.awsString))
	default:
		_, _ = f.Write([]byte(b.message))
	}
}
