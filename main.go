package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/cloudfoundry/sonde-go/events"
	consumer "github.com/harlow/kinesis-consumer"
	checkpointddb "github.com/harlow/kinesis-consumer/checkpoint/ddb"
	checkpointpg "github.com/harlow/kinesis-consumer/checkpoint/postgres"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/vjeantet/grok"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/olivere/elastic"
	aws "github.com/olivere/elastic/aws/v4"
)

var (
	errorCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kinesis_to_elasticsearch_errors_count",
	})
	successCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kinesis_to_elasticsearch_sent_count",
	})
)

func init() {
	prometheus.MustRegister(errorCount)
	prometheus.MustRegister(successCount)
}

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
	MetricsListen      string
	ESIndices          []string

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

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(a.MetricsListen, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()

	// scan stream
	return c.Scan(ctx, func(r *consumer.Record) consumer.ScanStatus {
		err := a.processRecord(ctx, client, r)
		if err == nil {
			successCount.Inc()
		} else {
			errorCount.Inc()
			log.Println(err)
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

	for _, index := range a.ESIndices {
		exists, err := esClient.IndexExists(index).Do(ctx)
		if err != nil {
			esClient.Stop()
			return nil, err
		}
		if exists {
			// index exists, we are happy
			log.Printf("index exists %s", index)
		} else {
			// Create a new index.
			_, err = esClient.CreateIndex(index).Do(ctx)
			if err != nil {
				esClient.Stop()
				return nil, err
			}
			log.Printf("successfully created index %s", index)
		}
	}

	return esClient, nil
}

func (a *kinesisToElastic) processRecord(ctx context.Context, es *elastic.Client, r *consumer.Record) error {
	var newEvent events.Envelope
	var values map[string]string
	var esIndex string

	err := newEvent.Unmarshal(r.Data)
	if err != nil {
		return err
	}

	if newEvent.GetEventType() != events.Envelope_LogMessage {
		return nil
	}

	switch {
	case strings.HasPrefix(string(newEvent.LogMessage.GetSourceInstance()), "/var/log/"):
		values, err = a.Grok.Parse("%{GENERIC}", string(newEvent.LogMessage.Message))
		if err != nil {
			return err
		}
		esIndex = "linux_logs"
	case strings.Contains(string(newEvent.LogMessage.GetSourceInstance()), "/var/vcap/sys/log/gorouter/access.log"):
		values, err = a.Grok.Parse("%{ROUTERACCESS}", string(newEvent.LogMessage.Message))
		if err != nil {
			return err
		}
		esIndex = "gorouter_access"
	case strings.Contains(string(newEvent.LogMessage.GetSourceInstance()), "/var/vcap/sys/log/director/"):
		values, err = a.Grok.Parse("%{GENERIC}", string(newEvent.LogMessage.Message))
		if err != nil {
			return err
		}
		esIndex = "bosh_director"
	case strings.HasPrefix(string(newEvent.LogMessage.GetSourceInstance()), "/var/vcap/sys/log/"):
		values, err = a.Grok.Parse("%{GENERIC}", string(newEvent.LogMessage.Message))
		if err != nil {
			return err
		}
		esIndex = "var_vcap_sys_log"
	default:
		// log.Println(newEvent.LogMessage.GetSourceInstance())
		// log.Println(string(newEvent.LogMessage.Message))
		return nil
	}

	values["kinesis_time"] = r.ApproximateArrivalTimestamp.String()
	values["file_path"] = newEvent.LogMessage.GetSourceInstance()

	_, err = es.Index().
		Index(esIndex).
		Type(esIndex).
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

func envWithDefault(name, defaultValue string) string {
	rv, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
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
		ESIndices:   []string{"linux_logs", "gorouter_access", "bosh_director", "var_vcap_sys_log"},

		MetricsListen: envWithDefault("METRICS_LISTEN", ":8080"),

		Grok: mustGrok(&grok.Config{
			Patterns: map[string]string{
				"GENERIC":         `%{GREEDYDATA:log_event}`,
				"ROUTERTIME":      `%{YEAR}-%{MONTHNUM}-%{MONTHDAY}T%{TIME}+%{INT}`,
				"ROUTERACCESS":    `%{HOSTNAME:rtr_hostname} - \[%{ROUTERTIME:rtr_time}\] "%{WORD:rtr_verb} %{URIPATHPARAM:rtr_path} %{PROG:rtr_http_spec}" %{BASE10NUM:rtr_status:int} %{BASE10NUM:rtr_request_bytes_received:int} %{BASE10NUM:rtr_body_bytes_sent:int} "%{GREEDYDATA:rtr_referer}" "%{GREEDYDATA:rtr_http_user_agent}" "%{IPORHOST:rtr_src_host}:%{POSINT:rtr_src_port:int}" "%{IPORHOST:rtr_dst_host}:%{POSINT:rtr_dst_port:int}" x_forwarded_for:"%{GREEDYDATA:rtr_x_forwarded_for}" x_forwarded_proto:"%{GREEDYDATA:rtr_x_forwarded_proto}" vcap_request_id:"%{NOTSPACE:rtr_vcap_request_id}" response_time:%{NUMBER:rtr_response_time_sec:float} app_id:"%{NOTSPACE:rtr_app_id}" app_index:"%{BASE10NUM:rtr_app_index:int}" x_b3_traceid:"%{NOTSPACE:x_b3_traceid}" x_b3_spanid:"%{NOTSPACE:x_b3_spanid}" x_b3_parentspanid:"%{NOTSPACE:x_b3_parentspanid}"`,
				"BOSHTIME":        `%{MONTHDAY}\/%{MONTH}\/%{YEAR}:%{TIME} +%{INT}`,
				"BOSHDIRECTOROUT": `D, \[%{ROUTERTIME:director_time} .*\] %{GREEDYDATA:bosh_director_out}`,
				"BOSHDIRECTORERR": `%{IP:client_ip} - - \[%{BOSHTIME:director_time}\] %{GREEDYDATA:bosh_director_err}`,
				"LINUXMESSAGES":   `%{TIMESTAMP_ISO8601:os_time} %{GREEDYDATA:var_log_messages}`,
			},
		}),
		Mappings: mustParseJSON("index-mappings-logMessage.json"),
	}).RunForever(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}
