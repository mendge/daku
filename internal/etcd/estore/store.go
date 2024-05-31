package estore

import (
	"context"
	"go.etcd.io/etcd/client/v3/concurrency"
	"log"
)

func MutexWrite(ctx context.Context, key string, value string) error {
	cli := GetCli()
	// 建立session
	session, err := concurrency.NewSession(cli)
	if err != nil {
		return err
	}
	defer session.Close()
	// 加锁
	m := concurrency.NewMutex(session, key)
	if err = m.Lock(ctx); err != nil {
		log.Printf("@@@ Failed to acquire lock: ", err)
		return err
	}
	// 解锁
	defer func() {
		if err = m.Unlock(ctx); err != nil {
			log.Printf("@@@ Failed to release lock: ", err)
		}
	}()
	err = PutValueOfKey(ctx, key, value)
	if err != nil {
		log.Printf("@@@ Failed to write event to etcd: %v", err)
		return err
	}
	return nil
}
