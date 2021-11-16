package main

import (
	"context"
	"github.com/ReneKroon/ttlcache/v2"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/ehenry2/avro-flight-decisioner/internal/avroutil"
	"log"
	"github.com/aws/aws-sdk-go-v2/config"
	"time"
)

const codecLoaderKey = "codecLoader"


func HandleMessage(ctx context.Context, event cloudevents.Event) {
	log.Println("received message")
	// pull the codec loader out of the context.
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
		log.Printf("error creating avro codec: %s", err)
	}
	datum, _, err := codec.NativeFromBinary(event.Data())
	if err != nil {
		log.Printf("error decoding from binary: %s", err)
	}
	log.Println(datum)
	log.Println("done")
}

func initSchemaLoader() avroutil.AvroCodecLoader {
	bucket := "not-a-real-bucket"
	prefix := "not-a-real-prefix"
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


func main() {
	loader := initSchemaLoader()
	log.Println("starting cloud events client")
	c, err := cloudevents.NewClientHTTP()
	if err != nil {
		log.Fatalf("error starting cloudwatch client: %s", err)
	}
	log.Println("starting receiver")
	// initialize s3 client here and add to context
	ctx := context.WithValue(context.Background(), codecLoaderKey, loader)
	log.Fatal(
		c.StartReceiver(ctx, HandleMessage))
}
