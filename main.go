package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cloudfoundry-community/firehose-to-syslog/caching"
	"github.com/cloudfoundry/sonde-go/events"
	consumer "github.com/harlow/kinesis-consumer"
	checkpointddb "github.com/harlow/kinesis-consumer/checkpoint/ddb"
	checkpointpg "github.com/harlow/kinesis-consumer/checkpoint/postgres"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/vjeantet/grok"

	realAws "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/olivere/elastic"
	aws "github.com/olivere/elastic/aws/v4"

	cfclient "github.com/cloudfoundry-community/go-cfclient"
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
	DaysToKeep         int
	BatchWorkers       int

	Grok *grok.Grok

	CFClients map[string]caching.CFSimpleClient

	indices   map[string]bool
	indicesMU sync.Mutex

	cfCachesMU sync.Mutex
	cfCaches   map[string]*caching.CacheLazyFill
}

func (a *kinesisToElastic) getCFCache(origin string) (*caching.CacheLazyFill, error) {
	a.cfCachesMU.Lock()
	defer a.cfCachesMU.Unlock()

	if a.cfCaches == nil {
		a.cfCaches = make(map[string]*caching.CacheLazyFill)
	}

	rv, ok := a.cfCaches[origin]
	if ok {
		return rv, nil
	}

	client, ok := a.CFClients[origin]
	if !ok {
		return nil, fmt.Errorf("origin %s not recognised", origin)
	}

	rv = caching.NewCacheLazyFill(client, &DynamoCacheStore{
		Client:    dynamodb.New(session.New(realAws.NewConfig())),
		Origin:    origin,
		TableName: a.Table,
	}, &caching.CacheLazyFillConfig{
		CacheInvalidateTTL: time.Hour * 6,
		IgnoreMissingApps:  true,
		StripAppSuffixes:   []string{"-venerable", "-blue", "-green"},
	})
	a.cfCaches[origin] = rv

	return rv, nil
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

	client, err := a.createElasticSearchClient(ctx)
	if err != nil {
		return err
	}
	defer client.Stop()

	a.indices = make(map[string]bool)

	// Run synchronously at startup to free up space in case we're full
	err = a.deleteOldIndices(ctx, client)
	if err != nil {
		return err
	}

	// and then in background forever more
	go func() {
		for {
			time.Sleep(time.Hour * 24)
			err := a.deleteOldIndices(ctx, client)
			if err != nil {
				log.Println("error deleting old indices, will try again tomorrow:", err)
			}
		}
	}()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(a.MetricsListen, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()

	bulkService, err := client.BulkProcessor().FlushInterval(time.Second * 15).Workers(a.BatchWorkers).After(func(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
		if err != nil {
			log.Println(err, response)
		}
	}).Do(ctx)
	if err != nil {
		return err
	}
	defer bulkService.Close()
	defer bulkService.Flush()

	// scan stream
	return c.Scan(ctx, func(r *consumer.Record) consumer.ScanStatus {
		err := a.processRecord(ctx, client, bulkService, r)
		if err == nil {
			successCount.Inc()
		} else {
			errorCount.Inc()
			log.Println(err)
		}
		return consumer.ScanStatus{Error: err}
	})

}

var indexNameRegex = regexp.MustCompile(`^[\d]{4}-[\d]{2}-[\d]{2}$`)

func (a *kinesisToElastic) deleteOldIndices(ctx context.Context, client *elastic.Client) error {
	cutoff := time.Now().Add(time.Hour * -24 * time.Duration(a.DaysToKeep)).Format("2006-01-02")

	log.Println("deleting indices that are older than", cutoff)

	indices, err := client.IndexNames()
	if err != nil {
		return err
	}

	for _, iname := range indices {
		if iname == ".kibana" {
			log.Println("keeping", iname)
			continue
		}

		if len(iname) >= len(cutoff) {
			indexNameSuffix := iname[len(iname)-len(cutoff):]
			if indexNameRegex.Match([]byte(indexNameSuffix)) {
				if indexNameSuffix >= cutoff {
					log.Println("keeping", iname)
					continue
				}
			}
		}

		log.Println("dropping", iname)
		_, err = client.DeleteIndex(iname).Do(ctx)
		if err != nil {
			return err
		}

		a.indicesMU.Lock()
		delete(a.indices, iname)
		a.indicesMU.Unlock()
	}

	return nil
}

func (a *kinesisToElastic) createElasticSearchClient(ctx context.Context) (*elastic.Client, error) {
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
	return esClient, nil
}

// not thread safe
func (a *kinesisToElastic) ensureIndexExists(ctx context.Context, es *elastic.Client, indexName string) error {
	a.indicesMU.Lock()
	defer a.indicesMU.Unlock()

	_, ok := a.indices[indexName]
	if ok {
		return nil
	}
	exists, err := es.IndexExists(indexName).Do(ctx)
	if err != nil {
		return err
	}
	if !exists {
		// Create a new index.
		_, err = es.CreateIndex(indexName).BodyJson(map[string]interface{}{
			"mappings": map[string]interface{}{
				"_doc": map[string]interface{}{
					"date_detection":    true,
					"numeric_detection": true,
					"dynamic":           true,
					"properties": map[string]interface{}{
						"timestamp": map[string]interface{}{
							"type":   "date",
							"format": "epoch_millis",
						},
					},
				},
			},
		}).Do(ctx)
		if err != nil {
			return err
		}
		log.Printf("successfully created index %s", indexName)
	}

	a.indices[indexName] = true
	return nil
}

func (a *kinesisToElastic) augmentWithAppInfo(values map[string]string, appGUID, env string) error {
	cs, err := a.getCFCache(env)
	if err != nil {
		return err
	}
	app, err := cs.GetApp(appGUID)
	if err != nil {
		return err
	}

	values["@cf.app"] = app.Name
	values["@cf.app_id"] = app.Guid
	values["@cf.space"] = app.SpaceName
	values["@cf.space_id"] = app.SpaceGuid
	values["@cf.org"] = app.OrgName
	values["@cf.org_id"] = app.OrgGuid

	return nil
}

func (a *kinesisToElastic) processRecord(ctx context.Context, es *elastic.Client, bs *elastic.BulkProcessor, r *consumer.Record) error {
	var newEvent events.Envelope
	var values map[string]string
	var esIndex string
	dateStamp := r.ApproximateArrivalTimestamp.Format("2006-01-02")

	err := newEvent.Unmarshal(r.Data)
	if err != nil {
		return err
	}

	if newEvent.GetEventType() != events.Envelope_LogMessage {
		return nil
	}

	switch {
	case strings.HasPrefix(string(newEvent.LogMessage.GetSourceInstance()), "/var/log/"):
		return nil
		// values, err = a.Grok.Parse("%{GENERIC}", string(newEvent.LogMessage.Message))
		// if err != nil {
		// 	return err
		// }
		// esIndex = "linux_logs-" + dateStamp
	case strings.Contains(string(newEvent.LogMessage.GetSourceInstance()), "/var/vcap/sys/log/gorouter/access.log"):
		return nil
		// values, err = a.Grok.Parse("%{ROUTERACCESS}", string(newEvent.LogMessage.Message))
		// if err != nil {
		// 	return err
		// }
		// esIndex = "gorouter_access-" + dateStamp
	case strings.Contains(string(newEvent.LogMessage.GetSourceInstance()), "/var/vcap/sys/log/director/"):
		return nil
		// values, err = a.Grok.Parse("%{GENERIC}", string(newEvent.LogMessage.Message))
		// if err != nil {
		// 	return err
		// }
		// esIndex = "bosh_director-" + dateStamp
	case strings.HasPrefix(string(newEvent.LogMessage.GetSourceInstance()), "/var/vcap/sys/log/"):
		return nil
		// values, err = a.Grok.Parse("%{GENERIC}", string(newEvent.LogMessage.Message))
		// if err != nil {
		// 	return err
		// }
		// esIndex = "var_vcap_sys_log-" + dateStamp
	case newEvent.GetTags()["source_id"] == "gorouter":
		values, err = a.Grok.Parse("%{GENERIC}", string(newEvent.LogMessage.Message))
		if err != nil {
			return err
		}
		esIndex = "gorouter-" + dateStamp
	case newEvent.GetLogMessage().GetSourceType() == "APP/PROC/WEB":
		values, err = a.Grok.Parse("%{GENERIC}", string(newEvent.LogMessage.Message))
		if err != nil {
			return err
		}
		// not really gorouter, but let's keep it simple on the other end
		esIndex = "gorouter-" + dateStamp
	default:
		// bb, _ := json.Marshal(newEvent)
		// log.Println(string(bb))
		return nil
	}

	switch {
	case newEvent.LogMessage.GetAppId() != "":
		err = a.augmentWithAppInfo(values, newEvent.LogMessage.GetAppId(), newEvent.GetOrigin())
		if err != nil {
			log.Println("ignoring:", err)
		}
	case values["rtr_app_id"] != "":
		err = a.augmentWithAppInfo(values, values["rtr_app_id"], newEvent.GetOrigin())
		if err != nil {
			log.Println("ignoring:", err)
		}
	default:
		// if we can't identify an app, then, for now, don't bother storing in ES
		// bb, _ := json.Marshal(newEvent)
		// log.Println(string(bb))
		return nil
	}

	// convert to generic map so that we can add integers / longs to it that ES won't think are strings
	genericVals := toGenericMap(values)

	// we store time in millis since epoch
	// we don't do nano, as this would likely overflow JSON number types (2^53 - 1 is roughly safest max)
	// store as string so that auto-date detection might pick it up
	genericVals["timestamp"] = r.ApproximateArrivalTimestamp.UnixNano() / 1000000
	genericVals["file_path"] = newEvent.LogMessage.GetSourceInstance()
	genericVals["@cf.env"] = newEvent.GetOrigin()

	err = a.ensureIndexExists(ctx, es, esIndex)
	if err != nil {
		return err
	}

	bs.Add(elastic.NewBulkIndexRequest().Index(esIndex).Type("_doc").Doc(genericVals))

	// continue scanning
	return nil
}

func toGenericMap(m map[string]string) map[string]interface{} {
	rv := make(map[string]interface{})
	for k, v := range m {
		rv[k] = v
	}
	return rv
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

func mustCreateSimpleClients(origins []string) map[string]caching.CFSimpleClient {
	rv := make(map[string]caching.CFSimpleClient)
	for _, o := range origins {
		if o != "" {
			cfClient, err := cfclient.NewClient(&cfclient.Config{
				ApiAddress:   fmt.Sprintf("https://api.system.%s", o),
				ClientID:     mustEnv(fmt.Sprintf("%s_CLIENT_ID", strings.ToUpper(strings.Replace(o, ".", "_", -1)))),
				ClientSecret: mustEnv(fmt.Sprintf("%s_CLIENT_SECRET", strings.ToUpper(strings.Replace(o, ".", "_", -1)))),
			})
			if err != nil {
				panic(err)
			}
			rv[o] = &caching.CFClientAdapter{
				CF: cfClient,
			}
		}
	}
	return rv
}

func mustInt(s string) int {
	rv, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
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

		MetricsListen: envWithDefault("METRICS_LISTEN", ":8080"),

		DaysToKeep:   mustInt(envWithDefault("DAYS_TO_KEEP", "3")),
		BatchWorkers: mustInt(envWithDefault("ES_BATCH_WORKERS", "6")), // perhaps 2x the number of shards is a good amount?

		CFClients: mustCreateSimpleClients(strings.Split(os.Getenv("ALLOWED_ORIGINS"), ",")),

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
	}).RunForever(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}
