// Code generated by MockGen. DO NOT EDIT.
// Source: internal/avroutil/loader.go

// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	reflect "reflect"

	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	gomock "github.com/golang/mock/gomock"
	v2 "github.com/linkedin/goavro/v2"
)

// MockS3Client is a mock of S3Client interface.
type MockS3Client struct {
	ctrl     *gomock.Controller
	recorder *MockS3ClientMockRecorder
}

// MockS3ClientMockRecorder is the mock recorder for MockS3Client.
type MockS3ClientMockRecorder struct {
	mock *MockS3Client
}

// NewMockS3Client creates a new mock instance.
func NewMockS3Client(ctrl *gomock.Controller) *MockS3Client {
	mock := &MockS3Client{ctrl: ctrl}
	mock.recorder = &MockS3ClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockS3Client) EXPECT() *MockS3ClientMockRecorder {
	return m.recorder
}

// GetObject mocks base method.
func (m *MockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, params}
	for _, a := range optFns {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetObject", varargs...)
	ret0, _ := ret[0].(*s3.GetObjectOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetObject indicates an expected call of GetObject.
func (mr *MockS3ClientMockRecorder) GetObject(ctx, params interface{}, optFns ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, params}, optFns...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetObject", reflect.TypeOf((*MockS3Client)(nil).GetObject), varargs...)
}

// MockAvroCodecLoader is a mock of AvroCodecLoader interface.
type MockAvroCodecLoader struct {
	ctrl     *gomock.Controller
	recorder *MockAvroCodecLoaderMockRecorder
}

// MockAvroCodecLoaderMockRecorder is the mock recorder for MockAvroCodecLoader.
type MockAvroCodecLoaderMockRecorder struct {
	mock *MockAvroCodecLoader
}

// NewMockAvroCodecLoader creates a new mock instance.
func NewMockAvroCodecLoader(ctrl *gomock.Controller) *MockAvroCodecLoader {
	mock := &MockAvroCodecLoader{ctrl: ctrl}
	mock.recorder = &MockAvroCodecLoaderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockAvroCodecLoader) EXPECT() *MockAvroCodecLoaderMockRecorder {
	return m.recorder
}

// LoadCodec mocks base method.
func (m *MockAvroCodecLoader) LoadCodec(arg0 string) (*v2.Codec, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LoadCodec", arg0)
	ret0, _ := ret[0].(*v2.Codec)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// LoadCodec indicates an expected call of LoadCodec.
func (mr *MockAvroCodecLoaderMockRecorder) LoadCodec(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LoadCodec", reflect.TypeOf((*MockAvroCodecLoader)(nil).LoadCodec), arg0)
}