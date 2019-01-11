package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/cloudfoundry/sonde-go/events"
	consumer "github.com/harlow/kinesis-consumer"
	checkpointddb "github.com/harlow/kinesis-consumer/checkpoint/ddb"
	checkpointpg "github.com/harlow/kinesis-consumer/checkpoint/postgres"

	"github.com/vjeantet/grok"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/olivere/elastic"
	aws "github.com/olivere/elastic/aws/v4"
)

type kinesisToElastic struct {
	CheckpointProvider string
	App                string
	Stream             string
	Table              string
	ConnStr            string
	ESURL              string
	ESregion           string
	ESaccesskey        string
	ESsecretkey        string

	Grok *grok.Grok

	Mappings map[string]interface{}
}

func (a *kinesisToElastic) RunForever(parentCtx context.Context) error {
	var ck consumer.Checkpoint
	// setup checkpoint with either Postgres or DynamoDB
	switch a.CheckpointProvider {
	case "postgres":
		pgck, err := checkpointpg.New(a.App, a.Table, a.ConnStr)
		if err != nil {
			return err
		}
		defer pgck.Shutdown()
		ck = pgck
	case "dynamo":
		ddbck, err := checkpointddb.New(a.App, a.Table)
		if err != nil {
			return err
		}
		defer ddbck.Shutdown()
		ck = ddbck
	default:
		return errors.New("unknown checkpoint provider. Please use 'postgres' or 'dynamo'")
	}

	// consumer
	c, err := consumer.New(
		a.Stream,
		consumer.WithCheckpoint(ck),
		// consumer.WithCounter(counter),
	)
	if err != nil {
		return err
	}

	// use cancel func to signal shutdown
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// trap SIGINT, wait to trigger shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signals
		cancel()
	}()

	client, err := a.initElasticSearch(ctx)
	if err != nil {
		return err
	}
	defer client.Stop()

	var eventCounter int
	// scan stream
	return c.Scan(ctx, func(r *consumer.Record) consumer.ScanStatus {
		err := a.processRecord(ctx, client, r)
		if err != nil {
			log.Println(err)
		}
		eventCounter++
		if eventCounter%1000 == 0 {
			log.Printf("written %d records", eventCounter)
		}
		return consumer.ScanStatus{Error: err}
	})

}

func (a *kinesisToElastic) initElasticSearch(ctx context.Context) (*elastic.Client, error) {
	var opts []elastic.ClientOptionFunc

	if strings.Contains(a.ESURL, ".es.amazonaws.com") {
		signingClient := aws.NewV4SigningClient(credentials.NewStaticCredentials(
			a.ESaccesskey,
			a.ESsecretkey,
			"",
		), a.ESregion)

		opts = []elastic.ClientOptionFunc{
			elastic.SetURL(a.ESURL),
			elastic.SetSniff(false),
			elastic.SetHealthcheck(false),
			elastic.SetScheme("https"),
			elastic.SetHttpClient(signingClient),
		}

	} else {
		opts = []elastic.ClientOptionFunc{
			elastic.SetURL(a.ESURL),
			elastic.SetSniff(false),
			elastic.SetHealthcheck(false),
			elastic.SetScheme("http"),
		}
	}

	// Create an Elasticsearch client
	esClient, err := elastic.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := esClient.IndexExists("log-message").Do(ctx)
	if err != nil {
		esClient.Stop()
		return nil, err
	}
	if exists {
		// index exists, we are happy
		return esClient, nil
	}

	// Create a new index.
	_, err = esClient.CreateIndex("log-message").BodyJson(a.Mappings).Do(ctx)
	if err != nil {
		esClient.Stop()
		return nil, err
	}

	return esClient, nil
}

func (a *kinesisToElastic) processRecord(ctx context.Context, es *elastic.Client, r *consumer.Record) error {
	var newEvent events.Envelope

	err := newEvent.Unmarshal(r.Data)
	if err != nil {
		return err
	}

	if newEvent.GetEventType() != events.Envelope_LogMessage {
		return nil
	}

	matched := strings.Contains(string(newEvent.LogMessage.Message), "x_b3_traceid")

	if !matched {
		return nil
	}

	values, err := a.Grok.Parse("%{CFFIREHOSE}", string(newEvent.LogMessage.Message))
	if err != nil {
		return err
	}

	_, err = es.Index().
		Index("log-message").
		Type("log-message").
		BodyJson(values).
		Do(ctx)
	if err != nil {
		return err
	}
	// continue scanning
	return nil
}

func mustParseJSON(p string) map[string]interface{} {
	mappingFile, err := os.Open(p)
	if err != nil {
		panic(err)
	}
	defer mappingFile.Close()

	var mappingsJSON map[string]interface{}
	err = json.NewDecoder(mappingFile).Decode(&mappingsJSON)
	if err != nil {
		panic(err)
	}

	return mappingsJSON
}

func mustGrok(config *grok.Config) *grok.Grok {
	rv, err := grok.NewWithConfig(config)
	if err != nil {
		panic(err)
	}
	return rv
}

func mustEnv(name string) string {
	rv, ok := os.LookupEnv(name)
	if !ok {
		panic(fmt.Sprintf("must set %s in env", name))
	}
	return rv
}

func main() {
	err := (&kinesisToElastic{
		App:                mustEnv("APP_NAME"),
		CheckpointProvider: mustEnv("CK_PROVIDER"),
		Stream:             mustEnv("STREAM_NAME"),
		Table:              mustEnv("TABLE_NAME"),
		ESURL:              mustEnv("ES_URL"),

		ConnStr: os.Getenv("CONNECTION_STRING"),

		ESregion:    os.Getenv("ES_AWS_REGION"),
		ESaccesskey: os.Getenv("ES_AWS_ACCESS_KEY_ID"),
		ESsecretkey: os.Getenv("ES_AWS_SECRET_ACCESS_KEY"),

		Grok: mustGrok(&grok.Config{
			Patterns: map[string]string{
				"RTRTIME":    `%{YEAR}-%{MONTHNUM}-%{MONTHDAY}T%{TIME}+%{INT}`,
				"CFFIREHOSE": `%{HOSTNAME:rtr_hostname} - \[%{RTRTIME:rtr_time}\] "%{WORD:rtr_verb} %{URIPATHPARAM:rtr_path} %{PROG:rtr_http_spec}" %{BASE10NUM:rtr_status:int} %{BASE10NUM:rtr_request_bytes_received:int} %{BASE10NUM:rtr_body_bytes_sent:int} "%{GREEDYDATA:rtr_referer}" "%{GREEDYDATA:rtr_http_user_agent}" "%{IPORHOST:rtr_src_host}:%{POSINT:rtr_src_port:int}" "%{IPORHOST:rtr_dst_host}:%{POSINT:rtr_dst_port:int}" x_forwarded_for:"%{GREEDYDATA:rtr_x_forwarded_for}" x_forwarded_proto:"%{GREEDYDATA:rtr_x_forwarded_proto}" vcap_request_id:"%{NOTSPACE:rtr_vcap_request_id}" response_time:%{NUMBER:rtr_response_time_sec:float} app_id:"%{NOTSPACE:rtr_app_id}" app_index:"%{BASE10NUM:rtr_app_index:int}"`,
			},
		}),
		Mappings: mustParseJSON("index-mappings-logMessage.json"),
	}).RunForever(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}
