package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mendge/daku/internal/etcd/estore"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

var (
	timeout    = time.Millisecond * 3000
	ErrFree    = fmt.Errorf("target is free")
	ErrTimeout = fmt.Errorf("runtime pipeline target timeout")
)

type RuntimeClient struct {
	Path   string
	ctx    context.Context
	cancel context.CancelFunc
}

func NewRuntimeClient(path string) *RuntimeClient {
	client := &RuntimeClient{Path: path}
	client.ctx, client.cancel = context.WithCancel(context.Background())
	return client
}

func (c *RuntimeClient) Request(reqtype string) (resp string, err error) {
	cli := estore.GetCli()
	if !estore.FileExist(c.Path) {
		return "", ErrFree
	}

	watchCh := cli.Watch(c.ctx, c.Path)
	// 发送请求事件
	var event = Event{Type: reqtype, Data: ""}
	c.Write(&event)
	// 监听响应事件
	for res := range watchCh {
		for _, ev := range res.Events {
			if ev.Type == clientv3.EventTypePut {
				err := json.Unmarshal(ev.Kv.Value, &event)
				if err != nil {
					log.Println("@@@ client failed to unmarshal event: ", err)
				}
				if event.Type == Response {
					return event.Data, nil
				}
			}
		}
	}
	return "", err
}

func (c *RuntimeClient) Write(event *Event) {
	body, err := json.Marshal(event)
	err = estore.MutexWrite(c.ctx, c.Path, string(body))
	if err != nil {
		log.Printf("@@@ client write runtime info to /pipelines failed: %v", err)
	}
}
