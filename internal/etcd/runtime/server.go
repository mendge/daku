package runtime

import (
	"context"
	"encoding/json"
	"github.com/mendge/daku/internal/etcd/estore"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
)

const (
	ReqStatus = "status"
	ReqStop   = "stop"
	RespErr   = "error"
	Response  = "response"
)

type Event struct {
	Type string `json:"type"`
	Data string `json:"data"`
}

type RuntimeServer struct {
	Path        string
	HandlerFunc EventHandler

	ctx    context.Context
	cancel context.CancelFunc
}
type EventHandler func(server *RuntimeServer, event *Event)

func NewRuntimeServer(path string, handlerFunc EventHandler) *RuntimeServer {
	server := &RuntimeServer{
		Path:        path,
		HandlerFunc: handlerFunc,
	}
	server.ctx, server.cancel = context.WithCancel(context.Background())
	return server
}

func (s *RuntimeServer) Serve() {
	cli := estore.GetCli()
	_ = estore.PutValueOfKey(s.ctx, s.Path, "")

	watchCh := cli.Watch(s.ctx, s.Path)
	for res := range watchCh {
		for _, ev := range res.Events {
			if ev.Type == clientv3.EventTypePut {
				var event Event
				err := json.Unmarshal(ev.Kv.Value, &event)
				if err != nil {
					continue
				}
				s.HandlerFunc(s, &event)
			}
		}
	}
}

func (s *RuntimeServer) Write(event *Event) {
	body, err := json.Marshal(event)
	err = estore.MutexWrite(s.ctx, s.Path, string(body))
	if err != nil {
		log.Println("@@@ sever write runtime info to /pipelines failed: ", err)
	}
}

func (s *RuntimeServer) Shutdown() error {
	err := estore.DeleteKey(context.Background(), s.Path)
	s.cancel()
	return err
}
