package avroutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/ReneKroon/ttlcache/v2"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ehenry2/avro-flight-decisioner/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"io"
	"time"

	"testing"
)


func TestS3AvroCodecLoader_LoadCodec(t *testing.T) {
	// configuration
	eventName := "test.custom"
	bucketName := "fake-test-bucket"
	objectPrefix := "fake-prefix"
	schemaKey := fmt.Sprintf("%s/%s.json", objectPrefix, eventName)
	schema := `{"type": "record", "name": "ultra_risk_version_6_19", "fields": [{"name": "col0", "type": "double"}, {"name": "col1", "type": "string"}]}`
	getObjOut := &s3.GetObjectOutput{
			Body: io.NopCloser(
				bytes.NewBufferString(schema)),
	}
	record := make(map[string]interface{})
	col0Value := 0.8
	col1Value := "test"
	record["col0"] = col0Value
	record["col1"] = col1Value

	// expectations
	expectedS3Input := &s3.GetObjectInput{
		Bucket: &bucketName,
		Key: &schemaKey,
	}
	b := []byte{154, 153, 153, 153, 153, 153, 233, 63, 8, 116, 101, 115, 116}

	// set up mocks.
	ctrl := gomock.NewController(t)
	mockS3Client := mocks.NewMockS3Client(ctrl)
	mockS3Client.EXPECT().
		GetObject(gomock.Eq(context.Background()), gomock.Eq(expectedS3Input)).
		Return(getObjOut, nil)

	// create the codec loader struct.
	loader := S3AvroCodecLoader{
		cache: ttlcache.NewCache(),
		storageClient: mockS3Client,
		bucketName: bucketName,
		objectPrefix: objectPrefix,
	}

	// run the test.
	codec, err := loader.LoadCodec(eventName)
	assert.Nil(t, err, "there should be no error when creating codec")

	// verify by encoding a record.
	encoded, err := codec.BinaryFromNative(nil, record)
	assert.Nil(t, err, "there should be no error when encoding a test record to avro")
	assert.Equal(t, b, encoded, "encoding to binary should have the correct value")
}

func TestS3AvroCodecLoader_LoadCodec_in_cache(t *testing.T) {
	// configuration
	eventName := "test.custom"
	bucketName := "fake-test-bucket"
	objectPrefix := "fake-prefix"
	schema := `{"type": "record", "name": "ultra_risk_version_6_19", "fields": [{"name": "col0", "type": "double"}, {"name": "col1", "type": "string"}]}`

	record := make(map[string]interface{})
	col0Value := 0.8
	col1Value := "test"
	record["col0"] = col0Value
	record["col1"] = col1Value
	b := []byte{154, 153, 153, 153, 153, 153, 233, 63, 8, 116, 101, 115, 116}

	// set up mocks.
	ctrl := gomock.NewController(t)
	mockS3Client := mocks.NewMockS3Client(ctrl)

	// create cache
	cache := ttlcache.NewCache()
	_ = cache.Set(eventName, schema)

	// create the codec loader struct.
	loader := S3AvroCodecLoader{
		cache: cache,
		storageClient: mockS3Client,
		bucketName: bucketName,
		objectPrefix: objectPrefix,
	}

	// run the test.
	codec, err := loader.LoadCodec(eventName)
	assert.Nil(t, err, "there should be no error when creating codec")

	// verify by encoding a record.
	encoded, err := codec.BinaryFromNative(nil, record)
	assert.Nil(t, err, "there should be no error when encoding a test record to avro")
	assert.Equal(t, b, encoded, "encoding to binary should have the correct value")
}

func TestS3AvroCodecLoader_getSchemaFromCache_in_cache(t *testing.T) {
	// configuration
	bucketName := "fake-test-bucket"
	objectPrefix := "fake-prefix"
	eventName := "custom.miss_event"
	schema := `{"type": "record", "name": "ultra_risk_version_6_19", "fields": [{"name": "col0", "type": "double"}, {"name": "col1", "type": "string"}]}`

	// set up mocks.
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockS3Client(ctrl)

	// set up cache.
	cache := ttlcache.NewCache()
	_ = cache.SetTTL(10 * time.Minute)
	_ = cache.Set(eventName, schema)

	// create loader
	loader := NewS3AvroCodecLoader(cache, mockClient, bucketName, objectPrefix)

	// run the test.
	out, ok := loader.getSchemaFromCache(eventName)
	assert.Equal(t, schema, out, "loaded schema should match expected")
	assert.True(t, ok, "cache value should be present")

}

func TestS3AvroCodecLoader_getSchemaFromCache_in_cache_not_a_string(t *testing.T) {
	// configuration
	bucketName := "fake-test-bucket"
	objectPrefix := "fake-prefix"
	eventName := "custom.miss_event"
	schema := []byte{100}

	// set up mocks.
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockS3Client(ctrl)

	// set up cache.
	cache := ttlcache.NewCache()
	_ = cache.SetTTL(10 * time.Minute)
	_ = cache.Set(eventName, schema)

	// create loader
	loader := NewS3AvroCodecLoader(cache, mockClient, bucketName, objectPrefix)

	// run the test.
	out, ok := loader.getSchemaFromCache(eventName)
	assert.Equal(t, "", out, "loaded schema should be bytes")
	assert.False(t, ok, "retrieving schema should not have completed successfully")
}

func TestS3AvroCodecLoader_getSchemaFromCache_cache_miss(t *testing.T) {
	// configuration
	bucketName := "fake-test-bucket"
	objectPrefix := "fake-prefix"
	eventName := "custom.miss_event"

	// set up mocks.
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockS3Client(ctrl)

	// set up cache.
	cache := ttlcache.NewCache()

	// create loader
	loader := NewS3AvroCodecLoader(cache, mockClient, bucketName, objectPrefix)

	// run the test.
	out, ok := loader.getSchemaFromCache(eventName)
	assert.Equal(t, "", out, "schema should be a blank string")
	assert.False(t, ok, "should not be present in cache")
}

func TestS3AvroCodecLoader_getSchemaFromCache_cache_error(t *testing.T) {
	// configuration
	bucketName := "fake-test-bucket"
	objectPrefix := "fake-prefix"
	eventName := "custom.miss_event"
	unknownError := errors.New("unknown error")

	// set up mocks.
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockS3Client(ctrl)
	mockCache := mocks.NewMockSimpleCache(ctrl)
	mockCache.EXPECT().
		Get(gomock.Eq(eventName)).
		Return(nil, unknownError)

	// create loader
	loader := NewS3AvroCodecLoader(mockCache, mockClient, bucketName, objectPrefix)

	// run the test.
	out, ok := loader.getSchemaFromCache(eventName)
	assert.Equal(t, "", out, "schema should be a blank string")
	assert.False(t, ok, "should not be present in cache")
}

func TestS3AvroCodecLoader_loadSchemaFromS3(t *testing.T) {
	// configuration
	bucketName := "fake-test-bucket"
	objectPrefix := "fake-prefix"
	eventName := "custom.miss_event"
	schemaKey := fmt.Sprintf("%s/%s.json", objectPrefix, eventName)
	schema := `{"type": "record", "name": "ultra_risk_version_6_19", "fields": [{"name": "col0", "type": "double"}, {"name": "col1", "type": "string"}]}`
	getObjOut := &s3.GetObjectOutput{
		Body: io.NopCloser(
			bytes.NewBufferString(schema)),
	}

	// set up mocks.
	expectedS3Input := &s3.GetObjectInput{
		Bucket: &bucketName,
		Key: &schemaKey,
	}
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockS3Client(ctrl)
	mockClient.EXPECT().
		GetObject(gomock.Eq(context.Background()), gomock.Eq(expectedS3Input)).
		Return(getObjOut, nil)
	// set up cache.
	cache := ttlcache.NewCache()

	// create loader
	loader := NewS3AvroCodecLoader(cache, mockClient, bucketName, objectPrefix)
	result, err := loader.loadSchemaFromS3(eventName)
	assert.Nil(t, err, "there should be no error retrieving schema")
	assert.Equal(t, result, schema, "the schema should be the same as expected")
}

func TestS3AvroCodecLoader_loadSchemaFromS3_error(t *testing.T) {
	// configuration
	bucketName := "fake-test-bucket"
	objectPrefix := "fake-prefix"
	eventName := "custom.miss_event"
	schemaKey := fmt.Sprintf("%s/%s.json", objectPrefix, eventName)
	schema := ``
	getObjOut := &s3.GetObjectOutput{
		Body: io.NopCloser(
			bytes.NewBufferString(schema)),
	}

	// set up mocks.
	expectedS3Input := &s3.GetObjectInput{
		Bucket: &bucketName,
		Key: &schemaKey,
	}
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockS3Client(ctrl)
	mockClient.EXPECT().
		GetObject(gomock.Eq(context.Background()), gomock.Eq(expectedS3Input)).
		Return(getObjOut, errors.New("unexpected s3 error"))
	// set up cache.
	cache := ttlcache.NewCache()

	// create loader
	loader := NewS3AvroCodecLoader(cache, mockClient, bucketName, objectPrefix)
	result, err := loader.loadSchemaFromS3(eventName)
	assert.NotNil(t, err, "there should be an s3 error retrieving the schema")
	assert.Equal(t, result, schema, "the schema should be an empty string b/c of error")
}