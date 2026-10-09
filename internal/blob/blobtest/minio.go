// Package blobtest owns local S3-compatible fixture setup. Production store
// consumers do not create buckets or acquire administrative object-store APIs.
package blobtest

import (
	"context"
	"errors"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/tetral-ai/tetral/internal/blob"
)

func CreateBucket(ctx context.Context, client *http.Client, cfg blob.Config) error {
	admin := s3.New(s3.Options{Region: cfg.Region, Credentials: credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""), BaseEndpoint: aws.String(cfg.Endpoint), UsePathStyle: true, HTTPClient: client})
	if _, err := admin.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)}); err != nil {
		return errors.New("local object-store fixture bucket creation failed")
	}
	return nil
}

// DeleteBucketContents removes an isolated test bucket and its objects.
func DeleteBucketContents(ctx context.Context, client *http.Client, cfg blob.Config) error {
	admin := s3.New(s3.Options{Region: cfg.Region, Credentials: credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""), BaseEndpoint: aws.String(cfg.Endpoint), UsePathStyle: true, HTTPClient: client})
	pages := s3.NewListObjectsV2Paginator(admin, &s3.ListObjectsV2Input{Bucket: aws.String(cfg.Bucket)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return errors.New("local object-store fixture listing failed")
		}
		for _, object := range page.Contents {
			if _, err := admin.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(cfg.Bucket), Key: object.Key}); err != nil {
				return errors.New("local object-store fixture object deletion failed")
			}
		}
	}
	if _, err := admin.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(cfg.Bucket)}); err != nil {
		return errors.New("local object-store fixture bucket deletion failed")
	}
	return nil
}
