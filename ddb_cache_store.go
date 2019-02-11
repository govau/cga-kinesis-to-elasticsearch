package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"sync"

	"github.com/cloudfoundry-community/firehose-to-syslog/caching"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type DynamoCacheStore struct {
	Origin    string
	TableName string

	Client dynamodbiface.DynamoDBAPI // dynamodb.New(session.New(aws.NewConfig()))

	cache   map[string][]byte
	cacheMU sync.Mutex
}

func (dcs *DynamoCacheStore) Open() error {
	return nil
}

func (dcs *DynamoCacheStore) Close() error {
	return nil
}

// Get looks up key, and decodes it into rv.
// Returns ErrKeyNotFound if not found
func (dcs *DynamoCacheStore) Get(key string, rv interface{}) error {
	dcs.cacheMU.Lock()
	defer dcs.cacheMU.Unlock()

	if dcs.cache == nil {
		dcs.cache = make(map[string][]byte)
	}
	bb, ok := dcs.cache[key]
	if ok {
		return gob.NewDecoder(bytes.NewReader(bb)).Decode(rv)
	}

	resp, err := dcs.Client.GetItem(&dynamodb.GetItemInput{
		TableName:      &dcs.TableName,
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"namespace": &dynamodb.AttributeValue{
				S: &dcs.Origin,
			},
			"shard_id": &dynamodb.AttributeValue{
				S: &key,
			},
		},
	})
	if err != nil {
		return err
	}

	if resp.Item == nil {
		return caching.ErrKeyNotFound
	}

	var i item
	err = dynamodbattribute.UnmarshalMap(resp.Item, &i)
	if err != nil {
		return err
	}

	err = json.Unmarshal([]byte(i.Value), rv)
	if err != nil {
		return err
	}

	bs := &bytes.Buffer{}
	err = gob.NewEncoder(bs).Encode(rv)
	if err != nil {
		return err
	}

	dcs.cache[key] = bs.Bytes()

	return nil
}

type item struct {
	Namespace string `json:"namespace"`
	ShardID   string `json:"shard_id"`
	Value     string `json:"value"`
}

// Set encodes the value and stores it
func (dcs *DynamoCacheStore) Set(key string, value interface{}) error {
	dcs.cacheMU.Lock()
	defer dcs.cacheMU.Unlock()

	if dcs.cache == nil {
		dcs.cache = make(map[string][]byte)
	}

	bb, err := json.Marshal(value)
	if err != nil {
		return err
	}
	item, err := dynamodbattribute.MarshalMap(item{
		Namespace: dcs.Origin,
		ShardID:   key,
		Value:     string(bb),
	})
	if err != nil {
		return err
	}

	_, err = dcs.Client.PutItem(&dynamodb.PutItemInput{
		TableName: &dcs.TableName,
		Item:      item,
	})
	if err != nil {
		return err
	}

	bs := &bytes.Buffer{}
	err = gob.NewEncoder(bs).Encode(value)
	if err != nil {
		return err
	}

	dcs.cache[key] = bs.Bytes()

	return nil
}
