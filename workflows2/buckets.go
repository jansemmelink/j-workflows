package workflows

type Buckets interface {
	Bucket(name string) (Bucket, error)
}

type Bucket interface {
	Get(bucketName string, objectKey string, tmpl interface{}) (interface{}, error)
	Set(bucketName, objectKey string, value interface{}) error
	Del(bucketName string, objectKey string) error
}
