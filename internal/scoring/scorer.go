package scoring

import (
	"context"
	"github.com/apache/arrow/go/v7/arrow/flight"
	"github.com/apache/arrow/go/v7/arrow/ipc"
	"github.com/ehenry2/avro-flight-decisioner/internal/scoring/arrowconv"
)


type ModelScorer interface {
	ScoreModel(map[string]interface{}) (map[string]interface{}, error)
}

type ArrowFlightClient interface {
	flight.FlightServiceClient
}

type FlightModelScorer struct {
	uri string
	client ArrowFlightClient
	conv *arrowconv.ArrowConverter
}

func NewFlightModelScorer(uri string, client ArrowFlightClient, conv *arrowconv.ArrowConverter) *FlightModelScorer {
	return &FlightModelScorer{uri, client, conv}
}

func (s *FlightModelScorer) ScoreModel(features map[string]interface{}) (map[string]interface{}, error) {
	featuresRecord, err := s.conv.MapToArrow(features)
	if err != nil {
		return nil, err
	}
	defer featuresRecord.Release()

	dxc, err := s.client.DoExchange(context.Background())
	if err != nil {
		return nil, err
	}
	writer := flight.NewRecordWriter(dxc, ipc.WithSchema(featuresRecord.Schema()))
	writer.SetFlightDescriptor(&flight.FlightDescriptor{
		Type: flight.FlightDescriptor_PATH,
		Path: []string{""},
	})

	err = writer.Write(featuresRecord)
	if err != nil {
		return nil, err
	}
	defer writer.Close()

	reader, err := flight.NewRecordReader(dxc)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	outputRecord, err := reader.Read()
	if err != nil {
		return nil, err
	}
	outputRecord.Retain()

	return s.conv.ArrowToMap(outputRecord)
}
