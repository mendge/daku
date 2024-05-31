package estore

import (
	"context"
	"errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

const (
	EnvPath        = "/env"
	BaseConfigPath = "/config/base-dag.yaml"
	DagDir         = "/dags"
	LogDir         = "/logs"
	PipelineDir    = "/pipelines"
	AdminLogDir    = "/logs/admin"
	StatueDir      = "/status"
	ElectionDir    = "/elections"
	SuspendDir     = "/suspend"
	CronmapDir     = "/cronmap"
	HealthDir      = "/healths"

	Timeout = 5 * time.Second
)

var eclient *clientv3.Client

func NewCli() *clientv3.Client {
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://127.0.0.1:20000", "http://127.0.0.1:20002", "http://127.0.0.1:20004"},
		//Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: Timeout,
	})
	if err != nil {
		log.Fatal("Fail to connect etcd", err)
	}
	return client
}

func GetCli() *clientv3.Client {
	if eclient != nil {
		return eclient
	}
	return NewCli()
}

func GetValueOfKey(ctx context.Context, key string) (content []byte, err error) {
	cli := GetCli()
	resp, err := cli.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, errors.New("key does not exist")
	}
	return resp.Kvs[0].Value, nil
}

func GetSbuKeysOfKey(ctx context.Context, key string) (subKeys []string, err error) {
	cli := GetCli()
	resp, err := cli.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	for _, kv := range resp.Kvs {
		subKeys = append(subKeys, string(kv.Key))
	}
	return subKeys, nil
}

func PutValueOfKey(ctx context.Context, key string, value string) error {
	cli := GetCli()
	_, err := cli.Put(ctx, key, value)
	if err != nil {
		return err
	}
	return nil
}

func DeleteKey(ctx context.Context, key string) error {
	cli := GetCli()
	_, err := cli.Delete(ctx, key)
	return err
}

func DeleteKeyWithPrefix(ctx context.Context, key string) error {
	cli := GetCli()
	_, err := cli.Delete(ctx, key, clientv3.WithPrefix())
	return err
}
