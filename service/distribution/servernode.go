package distribution

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/mendge/daku/internal/dag"
	"github.com/mendge/daku/internal/etcd/estore"
	"github.com/mendge/daku/internal/utils"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"log"
	"path"
	"time"
)

const (
	LeaseTTL = 30 // 租约时间，以秒为单位 	// 节点名称，实际使用时可以根据需要更改
)

var serverNode *ServerNode

func GetServerNode() *ServerNode {
	if serverNode != nil {
		return serverNode
	} else {
		var err error
		serverNode, err = NewServerNode()
		if err != nil {
			log.Fatalf("Failed to create serverNode: %v", err)
		}
		return serverNode
	}
}

type Campaigner interface {
	Campaign(context.Context) error
	IsLeader(context.Context) bool
}

type ServerNode struct {
	ServerName string
	client     *clientv3.Client
	lease      clientv3.Lease
	session    *concurrency.Session
	election   *concurrency.Election
}

func NewServerNode() (*ServerNode, error) {
	cli := estore.NewCli()
	session, err := concurrency.NewSession(cli, concurrency.WithTTL(LeaseTTL))
	if err != nil {
		return nil, err
	}
	return &ServerNode{
		client:     cli,
		lease:      clientv3.NewLease(cli),
		session:    session,
		election:   concurrency.NewElection(session, estore.ElectionDir),
		ServerName: fmt.Sprintf("%v", uuid.New()),
	}, nil
}

func (sn *ServerNode) Campaign(ctx context.Context) {
	// 参与选举，如果选举成功，会定时续期
	log.Println(sn.ServerName, "  ", "start campaign")
	if err := sn.election.Campaign(ctx, sn.ServerName); err != nil {
		utils.LogErr("server node campaign", err)
	}
}

func (sn *ServerNode) isLeader(ctx context.Context) bool {
	select {
	case resp := <-sn.election.Observe(ctx):
		if len(resp.Kvs) > 0 && sn.ServerName == string(resp.Kvs[0].Value) {
			log.Println(sn.ServerName, "  ", sn.ServerName, " is Leader")
			return true
		}
		return false
	}
}

func (sn *ServerNode) KeepAlive(ctx context.Context) {
	for {
		// 申请一个30秒的租约
		leaseResp, err := sn.lease.Grant(ctx, LeaseTTL)
		if err != nil {
			utils.LogErr("keepalive fail to grant lease", err)
		}
		// 将节点名和当前时间作为key-value存储到etcd
		key := path.Join(estore.HealthDir, sn.ServerName)
		value := time.Now().Format(time.RFC3339)
		_, err = sn.client.Put(ctx, key, value, clientv3.WithLease(leaseResp.ID))
		log.Println(sn.ServerName, "  ", "renew lease")
		if err != nil {
			utils.LogErr("keepalive fail to renew lease", err)
		}
		// 等待租约快到期时再续约
		time.Sleep((LeaseTTL - 5) * time.Second)
	}
}

func (sn *ServerNode) WatchHealth(ctx context.Context, ch chan<- struct{}) {
	watchChan := sn.client.Watch(ctx, estore.HealthDir, clientv3.WithPrefix())
	ch <- struct{}{}
	for watchResp := range watchChan {
		// 只有leader才需要处理
		if sn.isLeader(ctx) {
			for _, event := range watchResp.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					if event.IsCreate() {
						sn.Shuffle(ctx)
						log.Println(sn.ServerName, "  ", "new health shuffle")
					}
					//log.Printf("检测到新增或更新事件: %s %s\n", event.Kv.Key, event.Kv.Value)
				case clientv3.EventTypeDelete:
					sn.Shuffle(ctx)
					log.Println(sn.ServerName, "  ", "remove health shuffle")
					//log.Printf("检测到删除事件: %s\n", event.Kv.Key)
				}
			}
		}
	}
}

func (sn *ServerNode) WatchDags(ctx context.Context) {
	watchChan := sn.client.Watch(ctx, estore.DagDir, clientv3.WithPrefix())
	var i int = 0
	for watchResp := range watchChan {
		// 只有leader才需要处理
		if sn.isLeader(ctx) {
			for _, event := range watchResp.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					// 检测变更的dag是定时dag
					dagPath := string(event.Kv.Key)
					cl := dag.Loader{}
					d, err := cl.LoadMetadata(dagPath)
					if err != nil {
						continue
					}
					if d.Schedule == nil && d.StopSchedule == nil && d.RestartSchedule == nil {
						continue
					}

					if event.IsModify() {
						// modify dag：由于在/cronmap下以定时dag找绑定关系复杂度和shuffle差不多，直接shuffle
						sn.Shuffle(ctx)
						log.Println(sn.ServerName, "  ", "modify scheduled dag shuffle")
					} else if event.IsCreate() {
						// create dag：创建操作比较频繁，如果每次增加都shuffle，复杂度会达到O(n*n)，故不采用shuffle
						serverNodes, _ := estore.GetFilesOfDir(estore.HealthDir)
						nodeName := path.Base(serverNodes[i])
						i++
						dstMap := path.Join(estore.CronmapDir, nodeName)
						b, _ := estore.GetContentOfFile(dstMap)
						var wfs []string
						_ = json.Unmarshal(b, &wfs)
						newWfs, _ := json.Marshal(append(wfs, dagPath))
						_ = estore.PutValueOfKey(ctx, dstMap, string(newWfs))
					}
				case clientv3.EventTypeDelete:
					sn.Shuffle(ctx)
					log.Println(sn.ServerName, "  ", "delete scheduled dag shuffle")
				}
			}
		}

	}
}

func (sn *ServerNode) WatchCronmap(ctx context.Context, sem chan<- struct{}, ch chan<- struct{}) {
	watchChan := sn.client.Watch(ctx, path.Join(estore.CronmapDir, sn.ServerName))
	ch <- struct{}{}
	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			switch event.Type {
			case clientv3.EventTypePut:
				sem <- struct{}{}
			case clientv3.EventTypeDelete:
				log.Println(sn.ServerName, "  ", "modify scheduled dag shuffle")
			}
		}
	}
}

func (sn *ServerNode) Shuffle(ctx context.Context) {
	// 获取健康的节点名
	assignments := make(map[string][]string)
	resp, _ := sn.client.Get(ctx, estore.HealthDir, clientv3.WithPrefix())
	for _, kv := range resp.Kvs {
		nodeName := path.Base(string(kv.Key))
		if _, exists := assignments[nodeName]; !exists {
			assignments[nodeName] = []string{}
		}
	}
	// 遍历有定时调度需求的工作流，将其均匀分配给所有节点
	resp, _ = sn.client.Get(ctx, estore.DagDir, clientv3.WithPrefix())
	cl := dag.Loader{}
	var serverNodes []string
	for k := range assignments {
		serverNodes = append(serverNodes, k)
	}
	total := len(serverNodes)
	var i int = 0
	for _, kv := range resp.Kvs {
		dagPath := string(kv.Key)
		if utils.MatchExtension(dagPath, dag.EXTENSIONS) {
			d, err := cl.LoadMetadata(dagPath)
			if err != nil {
				log.Println("@@@ shuffle, load dag wrong", err)
				continue
			}
			// only for scheduled workflow
			if d.Schedule == nil && d.StopSchedule == nil && d.RestartSchedule == nil {
				continue
			}
			// 均匀分配
			nodeName := serverNodes[i%total]
			i++
			assignments[nodeName] = append(assignments[nodeName], dagPath)
		}
	}
	// 提交工作流和节点的绑定
	_ = estore.DeleteKeyWithPrefix(ctx, estore.CronmapDir)
	for _, nodeName := range serverNodes {
		value, _ := json.Marshal(assignments[nodeName])
		_ = estore.PutValueOfKey(ctx, path.Join(estore.CronmapDir, nodeName), string(value))
	}
}
