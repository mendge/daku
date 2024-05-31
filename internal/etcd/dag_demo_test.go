package etcdstore

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"os"
	"testing"
	"time"
)

func TestEtcd(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:20000", "http://127.0.0.1:20002", "http://127.0.0.1:20004"},
		DialTimeout: 5 * time.Second,
	})
	defer cli.Close()
	if err != nil {
		fmt.Println("Fail to connect etcd.")
		os.Exit(-1)
	}
	fmt.Println("Connect to etcd success.")
	resp, _ := cli.Get(context.Background(), "name")
	name := resp.Kvs[0].Value
	fmt.Println(string(name))
}
