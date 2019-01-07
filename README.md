# Kinesis to ElasticSearch
consume events from Kinesis data stream, apply grok patterns and write to ElasticSearch

## local development
Start the dependencies (local Postgres, ElasticSearch and Kibana)
```
docker-compose up
```


Set the AWS environment variables for the kinesis-to-elasticsearch consumer
```
export AWS_REGION=ap-southeast-2
export AWS_SECRET_ACCESS_KEY=xxx
export AWS_ACCESS_KEY_ID=yyy
export AWS_KINESIS_STREAM=cloudfoundry_firehose
```

Run it
```
go run main.go -app kinesis-to-elasticsearch -connection "postgres://postgres:postgres@localhost/kinesis?sslmode=disable" -table kinesis_consumer -stream $AWS_KINESIS_STREAM
```