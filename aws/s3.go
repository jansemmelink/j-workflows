package aws

import (
	"encoding/json"
	"reflect"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/go-msvc/errors"
	workflows "github.com/jansemmelink/j-workflow/workflows2"
)

type Validator interface {
	Validate() error
}

func (a *myAws) S3() (workflows.Buckets, error) {
	a.Lock()
	defer a.Unlock()
	if a.s3 == nil {
		a.s3 = s3.New(a.sess)
	}

	if a.buckets != nil {
		return a.buckets, nil
	}

	a.buckets = &s3Buckets{
		sess:         a.sess,
		s3:           a.s3,
		bucketByName: map[string]*s3Bucket{},
	}
	return a.buckets, nil
}

type s3Buckets struct {
	sync.Mutex
	sess         *session.Session
	s3           *s3.S3
	bucketByName map[string]*s3Bucket
}

func (buckets *s3Buckets) List() ([]string, error) {
	buckets.Lock()
	defer buckets.Unlock()
	result, err := buckets.s3.ListBuckets(nil)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to access S3 buckets")
	}
	names := []string{}
	for _, b := range result.Buckets {
		//log.Debugf("available bucket[%d]: %s created on %s", i, aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
		names = append(names, aws.StringValue(b.Name))
	}
	return names, nil
} //s3Buckets.List()

func (buckets *s3Buckets) Bucket(name string) (workflows.Bucket, error) {
	buckets.Lock()
	defer buckets.Unlock()

	if existing, ok := buckets.bucketByName[name]; ok {
		return existing, nil
	}

	_, err := buckets.s3.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create bucket(%s)", name)
	}

	err = buckets.s3.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed while waiting for create bucket(%s) to complete", name)
	}
	log.Debugf("created bucket(%s)", name)

	bucket := &s3Bucket{
		sess: buckets.sess,
		s3:   buckets.s3,
		name: name,
	}
	buckets.bucketByName[name] = bucket
	return bucket, nil
}

type s3Bucket struct {
	sess *session.Session
	s3   *s3.S3
	name string
}

func (bucket s3Bucket) List(bucketName string) ([]interface{}, error) {
	resp, err := bucket.s3.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
	if err != nil {
		return nil, errors.Wrapf(err, "unable to list items in bucket %q, %v", bucketName, err)
	}
	for _, item := range resp.Contents {
		log.Debugf("bucket(%s).key(%s) modified:%s, size:%v, class:%v",
			bucketName,
			*item.Key,
			*item.LastModified,
			*item.Size,
			*item.StorageClass)
	}
	return nil, errors.Errorf("NYI")
}

func (bucket *s3Bucket) Get(bucketName string, objectKey string, tmpl interface{}) (interface{}, error) {
	output, err := bucket.s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get(%s,%s)", bucketName, objectKey)
	}
	if output.Body == nil {
		return nil, errors.Errorf("no body")
	}
	if tmpl == nil {
		//parse into any type
		var value interface{}
		if err := json.NewDecoder(output.Body).Decode(&value); err != nil {
			return nil, errors.Errorf("failed to decode JSON body")
		}
		return value, nil
	}

	//parse into tmpl type
	newPtrValue := reflect.New(reflect.TypeOf(tmpl))
	if err := json.NewDecoder(output.Body).Decode(newPtrValue.Interface()); err != nil {
		return nil, errors.Errorf("failed to decode JSON body")
	}

	if validator, ok := newPtrValue.Interface().(Validator); ok {
		if err := validator.Validate(); err != nil {
			return nil, errors.Wrapf(err, "got invalid object")
		}
	}
	return newPtrValue.Elem().Interface(), nil
} //s3ObjectStore.Get()

func (bucket *s3Bucket) Set(bucketName, objectKey string, value interface{}) error {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return errors.Errorf("failed to encode JSON")
	}

	if _, err := bucket.s3.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader(string(jsonValue)),
	}); err != nil {
		return errors.Wrapf(err, "failed to put object")
	}
	return nil
} //s3ObjectStore.Set()

func (bucket *s3Bucket) Del(bucketName string, objectKey string) error {
	if _, err := bucket.s3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}); err != nil {
		return errors.Wrapf(err, "failed to del object")
	}
	return nil
} //s3ObjectStore.Del()
