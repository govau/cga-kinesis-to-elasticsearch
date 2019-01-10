# Kinesis to ElasticSearch
consume events from Kinesis data stream, apply grok patterns and write to ElasticSearch

## local development
Start the dependencies (local Postgres, ElasticSearch and Kibana)
```
docker-compose up
```


To read from kinesis and write to the local Elastic Search Cluster, set the following environment variables
```
export APP_NAME=cloudfoundry_firehose
export STREAM_NAME=cloudfoundry_firehose
export CONNECTION_STRING="postgres://postgres:postgres@localhost/kinesis?sslmode=disable"
export TABLE_NAME=kinesis_consumer
export AWS_REGION=ap-southeast-2
export AWS_SECRET_ACCESS_KEY=xxx
export AWS_ACCESS_KEY_ID=yyy
export ES_URL="http://localhost:9200"
```
Now run it
```
go run main.go
```


To read from kinesis and write to an AWS ElasticSearch Cluster, set the following environment variables
```
export APP_NAME=cloudfoundry_firehose
export STREAM_NAME=cloudfoundry_firehose
export CONNECTION_STRING="postgres://postgres:postgres@localhost/kinesis?sslmode=disable"
export TABLE_NAME=kinesis_consumer
export AWS_REGION=ap-southeast-2
export AWS_SECRET_ACCESS_KEY=xxx
export AWS_ACCESS_KEY_ID=yyy
export ES_AWS_REGION=ap-southeast-2
export ES_AWS_SECRET_ACCESS_KEY=aaa
export ES_AWS_ACCESS_KEY_ID=bbb
export ES_URL="https://search-abc-xyz-xxxxxxx.ap-southeast-2.es.amazonaws.com/"
```
Now run it
```
go run main.go

view logs at AWS Kibana endpoint
