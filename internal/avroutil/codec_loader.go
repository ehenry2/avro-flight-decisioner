package avroutil

import (
	"context"
	"fmt"
	"github.com/ReneKroon/ttlcache/v2"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/linkedin/goavro/v2"
	"io/ioutil"
	"log"
)

type S3Client interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type AvroCodecLoader interface {
	LoadCodec(string) (*goavro.Codec, error)
}

type S3AvroCodecLoader struct {
	cache ttlcache.SimpleCache
	storageClient S3Client
	bucketName string
	objectPrefix string
}

func (l *S3AvroCodecLoader) getSchemaFromCache(cloudEventName string) (string, bool) {
	val, err := l.cache.Get(cloudEventName)
	if err != nil {
		if err == ttlcache.ErrNotFound {
			log.Printf("cache miss retrieving schema: event: %s", cloudEventName)
			return "", false
		} else {
			log.Printf("error getting schema from cache: event: %s, reason: %s", cloudEventName, err)
			return "", false
		}
	}
	s, ok := val.(string)
	if !ok {
		log.Printf("could not convert retrieved item to string: event: %s", cloudEventName)
	}
	return s, ok
}

func (l *S3AvroCodecLoader) loadSchemaFromS3(cloudEventName string) (string, error) {
	key := fmt.Sprintf("%s/%s.json", l.objectPrefix, cloudEventName)
	input := &s3.GetObjectInput{
		Bucket: &l.bucketName,
		Key: &key,
	}
	output, err := l.storageClient.GetObject(context.Background(), input)
	if err != nil {
		return "", err
	}
	b, err := ioutil.ReadAll(output.Body)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (l *S3AvroCodecLoader) LoadCodec(cloudEventName string) (*goavro.Codec, error) {
	s, found := l.getSchemaFromCache(cloudEventName)
	if !found {
		s, err := l.loadSchemaFromS3(cloudEventName)
		if err != nil {
			return nil, err
		}
		// set it in the cache.
		err = l.cache.Set(cloudEventName, s)
		if err != nil {
			log.Printf("error setting key in cache: key: %s, reason %s", cloudEventName, err)
		}
		return goavro.NewCodec(s)
	}
	return goavro.NewCodec(s)
}

func NewS3AvroCodecLoader(cache ttlcache.SimpleCache, storageClient S3Client,
	bucketName, objectPrefix string) *S3AvroCodecLoader {
	return &S3AvroCodecLoader{cache, storageClient, bucketName, objectPrefix}
}
