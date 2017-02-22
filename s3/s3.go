package s3

import (
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/qor/oss"
)

// Client S3 storage
type Client struct {
	*s3.S3
	Config Config
}

// Config S3 client config
type Config struct {
	AccessID     string
	AccessKey    string
	Region       string
	Bucket       string
	SessionToken string
	ACL          string
}

func EC2RoleAwsConfig(config Config) *aws.Config {
	ec2m := ec2metadata.New(session.New(), &aws.Config{
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
		Endpoint:   aws.String("http://169.254.169.254/latest"),
	})

	cr := credentials.NewCredentials(&ec2rolecreds.EC2RoleProvider{
		Client: ec2m,
	})

	return &aws.Config{
		Region:      &config.Region,
		Credentials: cr,
	}
}

// New initialize S3 storage
func New(config Config) *Client {
	client := &Client{Config: config}

	if config.ACL == "" {
		config.ACL = s3.BucketCannedACLPublicRead
	}

	if config.AccessID == "" && config.AccessKey == "" {
		client.S3 = s3.New(session.New(), EC2RoleAwsConfig(config))
	} else {
		creds := credentials.NewStaticCredentials(config.AccessID, config.AccessKey, config.SessionToken)
		if _, err := creds.Get(); err == nil {
			client.S3 = s3.New(session.New(), &aws.Config{
				Region:      &config.Region,
				Credentials: creds,
			})
		}
	}

	return client
}

// Get receive file with given path
func (client Client) Get(path string) (file *os.File, err error) {
	getResponse, err := client.S3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(client.Config.Bucket),
		Key:    aws.String(path),
	})

	if err == nil {
		if file, err = ioutil.TempFile("/tmp", "s3"); err == nil {
			_, err = io.Copy(file, getResponse.Body)
			file.Seek(0, 0)
		}
	}

	return file, err
}

// Put store a reader into given path
func (client Client) Put(path string, reader io.ReadSeeker) (*oss.Object, error) {
	params := &s3.PutObjectInput{
		Bucket: aws.String(client.Config.Bucket), // required
		Key:    aws.String(path),                 // required
		ACL:    aws.String(client.Config.ACL),
		Body:   reader,
	}

	_, err := client.S3.PutObject(params)

	now := time.Now()
	return &oss.Object{
		Path:             toRelativePath(path),
		Name:             filepath.Base(path),
		LastModified:     &now,
		StorageInterface: client,
	}, err
}

// Delete delete file
func (client Client) Delete(path string) error {
	_, err := client.S3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(client.Config.Bucket),
		Key:    aws.String(path),
	})
	return err
}

// List list all objects under current path
func (client Client) List(path string) ([]*oss.Object, error) {
	var objects []*oss.Object

	listObjectsResponse, err := client.S3.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(client.Config.Bucket),
		Prefix: aws.String(path),
	})

	if err == nil {
		for _, content := range listObjectsResponse.Contents {
			objects = append(objects, &oss.Object{
				Path:             toRelativePath(*content.Key),
				Name:             filepath.Base(*content.Key),
				LastModified:     content.LastModified,
				StorageInterface: client,
			})
		}
	}

	return objects, err
}

func toRelativePath(path string) string {
	return "/" + strings.TrimPrefix(path, "/")
}