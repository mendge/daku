package etcdstore

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/client/v3"
	"sync"
	"time"
)

func GetLocalClient() (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	//defer cli.Close()
	if err != nil {
		fmt.Println("fail to connect")
	}
	fmt.Println("connect to etcd success")
	return cli, err
}

func NormalSase(cli *clientv3.Client) {
	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	// watch
	go func() {
		rch := cli.Watch(context.Background(), "admin") // <-chan WatchResponse
		for wresp := range rch {
			for _, ev := range wresp.Events {
				fmt.Printf("Type: %s Key:%s Value:%s\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
		}
		wg.Done()
	}()
	if err != nil {
		fmt.Printf("connect to etcd failed, err:%v\n", err)
		return
	}

	// put
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = cli.Put(ctx, "admin", "mendge")
	cancel()
	if err != nil {
		fmt.Printf("put to etcd failed, err:%v\n", err)
		return
	}

	// get
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, "admin")
	cancel()
	if err != nil {
		fmt.Printf("get from etcd failed, err:%v\n", err)
		return
	}
	for _, ev := range resp.Kvs {
		fmt.Printf("%s:%s\n", ev.Key, ev.Value)
	}
	wg.Wait()
}
