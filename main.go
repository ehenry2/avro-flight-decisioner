package main

import (
	"context"
	"github.com/ReneKroon/ttlcache/v2"
	"github.com/apache/arrow/go/v7/arrow/flight"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/ehenry2/avro-flight-decisioner/internal/arrowconv"
	"github.com/ehenry2/avro-flight-decisioner/internal/avroutil"
	"github.com/ehenry2/avro-flight-decisioner/internal/scoring"
	"google.golang.org/grpc"
	"log"
	"time"
)

const (
	codecLoaderKey = "codecLoader"
	scorerKey = "flightScorer"
)


func HandleMessage(ctx context.Context, event cloudevents.Event) {
	log.Println("received message")
	start := time.Now()
	// pull the codec loader out of the context and load the avro codec.
	codecLoader := ctx.Value(codecLoaderKey)
	if codecLoader == nil {
		log.Fatalln("required codec loader not in context..exiting")
	}
	loader, ok := codecLoader.(avroutil.AvroCodecLoader)
	if !ok {
		log.Fatalln("codec loader in context is not a valid AvroCodecLoader")
	}
	codec, err := loader.LoadCodec(event.Type())
	if err != nil {
		log.Fatalf("error creating avro codec: %s", err)
	}

	// convert from avro to generic map
	datum, _, err := codec.NativeFromBinary(event.Data())
	if err != nil {
		log.Fatalf("error decoding from binary: %s", err)
	}
	data, ok := datum.(map[string]interface{})
	if !ok {
		log.Fatalln("could not convert datum to map")
	}

	// pull out the flight client
	flightScorer := ctx.Value(scorerKey)
	scorer, ok := flightScorer.(*scoring.FlightModelScorer)
	if !ok {
		log.Println("could not cast to flight client")
	}

	// run the scoring
	_, err = scorer.ScoreModel(data)
	if err != nil {
		log.Fatalf("error scoring: %s", err)
	}

	elapsed := time.Since(start)
	log.Printf("took %d milliseconds", elapsed.Milliseconds())
	// convert back to map
	log.Println("done")
}

func initSchemaLoader() avroutil.AvroCodecLoader {
	bucket := "dqhub-test"
	prefix := "not-a-prefix"
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	client := s3.NewFromConfig(cfg)
	cache := ttlcache.NewCache()
	err = cache.SetTTL(10 * time.Minute)
	if err != nil {
		log.Fatalln(err)
	}
	return avroutil.NewS3AvroCodecLoader(cache, client, bucket, prefix)
}

func getFlightClient() (flight.FlightServiceClient, error) {
	conn, err := grpc.Dial("127.0.0.1:9998", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	return flight.NewFlightServiceClient(conn), nil
}

func getScorer() *scoring.FlightModelScorer {
	flightClient, err := getFlightClient()
	if err != nil {
		log.Fatalf("failed to instantiate flight client: %s", err)
	}
	conv := arrowconv.NewArrowConverter(memory.NewGoAllocator())
	return scoring.NewFlightModelScorer(flightClient, conv)
}

func main() {
	scorer := getScorer()
	loader := initSchemaLoader()
	log.Println("starting cloud events client")
	c, err := cloudevents.NewClientHTTP()
	if err != nil {
		log.Fatalf("error starting cloudwatch client: %s", err)
	}
	log.Println("starting receiver")
	// initialize s3 client here and add to context
	ctx := context.WithValue(context.Background(), codecLoaderKey, loader)
	ctx = context.WithValue(ctx, scorerKey, scorer)
	log.Fatal(
		c.StartReceiver(ctx, HandleMessage))
}
