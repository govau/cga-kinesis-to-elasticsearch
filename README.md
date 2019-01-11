# Kinesis to ElasticSearch
consume events from Kinesis data stream, apply grok patterns and write to ElasticSearch


### local development
Start the dependencies (local Postgres, ElasticSearch and Kibana)
```
docker-compose up
```

kinesis, use local postgres for checkpointing, and write to the local Elastic Search Cluster.
```
APP_NAME=cloudfoundry_firehose \
STREAM_NAME=cloudfoundry_firehose \
CK_PROVIDER="postgres" \
CONNECTION_STRING="postgres://postgres:postgres@localhost/kinesis?sslmode=disable" \
TABLE_NAME=kinesis_consumer \
AWS_REGION=ap-southeast-2 \
AWS_SECRET_ACCESS_KEY=xxx \
AWS_ACCESS_KEY_ID=yyy \
ES_URL="http://localhost:9200" \
go run main.go
```

### AWS ElasticSearch service and local postgres consumer checkpointing
Start local postgres to store checkpoint data
```
docker-compose up postgres
```
Read from kinesis and write to an AWS ElasticSearch Cluster.
```
APP_NAME=cloudfoundry_firehose \
STREAM_NAME=cloudfoundry_firehose \
CK_PROVIDER="postgres" \
CONNECTION_STRING="postgres://postgres:postgres@localhost/kinesis?sslmode=disable" \
TABLE_NAME=kinesis_consumer \
AWS_REGION=ap-southeast-2 \
AWS_SECRET_ACCESS_KEY=xxx \
AWS_ACCESS_KEY_ID=yyy \
ES_AWS_REGION=ap-southeast-2 \
ES_AWS_SECRET_ACCESS_KEY=aaa \
ES_AWS_ACCESS_KEY_ID=bbb \
ES_URL="https://search-abc-xyz-xxxxxxx.ap-southeast-2.es.amazonaws.com/" \
go run main.go
```
view logs at AWS Kibana endpoint


### AWS ElasticSearch service and AWS DynamoDB consumer checkpointing
To use DynamoDB instead of Postgres

```
APP_NAME=cloudfoundry_firehose \
STREAM_NAME=cloudfoundry_firehose \
CK_PROVIDER="dynamo" \
TABLE_NAME="kinesis-to-elastic-consumer" \
AWS_REGION=ap-southeast-2 \
AWS_SECRET_ACCESS_KEY=xxx \
AWS_ACCESS_KEY_ID=yyy \
ES_AWS_REGION=ap-southeast-2 \
ES_AWS_SECRET_ACCESS_KEY=aaa \
ES_AWS_ACCESS_KEY_ID=bbb \
ES_URL="https://search-abc-xyz-xxxxxxx.ap-southeast-2.es.amazonaws.com/" \
go run main.go
```
