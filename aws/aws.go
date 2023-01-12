package aws

import (
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-msvc/errors"
	workflows "github.com/jansemmelink/j-workflow/workflows2"
	"github.com/stewelarend/logger"
)

var log = logger.New().WithLevel(logger.LevelDebug)

type AWS interface {
	S3() (workflows.Buckets, error)
	SQS(name string, receive bool) (workflows.EventStream, error)
}

func New() (AWS, error) {
	//setup AWS SDK session
	region := "us-east-1"
	var err error

	a := &myAws{}
	a.sess, err = session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create AWS session")
	}

	if _, err := a.sess.Config.Credentials.Get(); err != nil {
		return nil, errors.Wrapf(err, "failed to find AWS credentials")
	}
	a.eventStreamByQName = map[string]*sqsEventStream{}
	a.buckets = nil
	return a, nil
} //New()

type myAws struct {
	sync.Mutex
	sess               *session.Session
	sqs                *sqs.SQS
	s3                 *s3.S3
	eventStreamByQName map[string]*sqsEventStream
	buckets            *s3Buckets
}
