package main

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/ehenry2/avro-flight-decisioner/mocks"
	"github.com/golang/mock/gomock"
	"github.com/linkedin/goavro/v2"
	"testing"
)


func Test_handleMessage(t *testing.T) {
	// test configuration
	avroSchema := `{"doc": "a risk model feature", "name": "ultra_risk_version_6_19", "type": "record", "fields": [{"name": "app_id", "type": "string"}, {"name": "bank_balance_30_days", "type": "double"}, {"name": "credit_score", "type": "int"}]}`
	eventType := "custom.fake-event"
	codec, _ := goavro.NewCodec(avroSchema)
	record := make(map[string]interface{})
	record["app_id"] = "1000"
	record["bank_balance_30_days"] = 50000.00
	record["credit_score"] = 800
	data, _ := codec.BinaryFromNative(nil, record)

	// set up mocks.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mocks.NewMockAvroCodecLoader(ctrl)
	m.EXPECT().
		LoadCodec(gomock.Eq(eventType)).
		Return(codec, nil)


	// create the cloud event
	e := cloudevents.NewEvent()
	e.SetID("abc123")
	e.SetSource("upstream")
	e.SetType(eventType)
	_ = e.SetData("application/octet-stream", data)

	// create the context
	ctx := context.WithValue(context.Background(), codecLoaderKey, m)

	// run the test
	HandleMessage(ctx, e)
	t.Fail()

}
