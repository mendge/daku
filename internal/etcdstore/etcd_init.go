package etcdstore

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/etcd/client/v3"
	"os"
	"path"
	"regexp"
	"strings"
	"time"
)

var ecli *clientv3.Client

var (
	ConfigPath     = "/config/system"
	EnvPath        = "/env"
	BaseConfigPath = "/config/base-dag.yaml"
	DagDir         = "/dags"
	LogDir         = "/logs"
	AdminLogDir    = "/logs/admin"
	StatueDir      = "/status"
	// RunTimeDagDir 代替 suspend
	RunTimeDagDir = "/runtime"
	SuspendDir    = "/suspend"
)

func GetCli() *clientv3.Client {
	var err error
	if ecli == nil {
		ecli, err = clientv3.New(clientv3.Config{
			Endpoints:   []string{"127.0.0.1:2379"},
			DialTimeout: 5 * time.Second,
		})
		//defer cli.Close()
		if err != nil {
			fmt.Println("Fail to connect etcd.")
			os.Exit(-1)
		}
		fmt.Println("Connect to etcd success.")
	}
	return ecli
}

func GetFilesOfDir(dir string) (files []string, err error) {
	files, err = getSbuKeysOfKey(dir)
	if err != nil {
		return nil, err
	}
	return files, nil
}

func GetContentOfFile(file string) (content []byte, err error) {
	content, err = getValueOfKey(file)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func GetEnvValue(key string) string {
	fullEnvKey := path.Join(EnvPath, key)
	value, _ := getValueOfKey(fullEnvKey)
	return string(value)
}

func FileExist(file string) bool {
	_, err := getValueOfKey(file)
	if err != nil {
		return false
	}
	return true
}

// Dir Or File
func StatExist(dir string) bool {
	keys, err := getSbuKeysOfKey(dir)
	if err != nil || keys == nil {
		return false
	}
	return true
}

func SaveFile(file string, content string) error {
	//fmt.Println("etcd save" + file)
	//fmt.Println(content)
	return putValueOfKey(file, content)
}

func RemoveFile(file string) error {
	return deleteKey(file)
}

func RemoveDir(dir string) error {
	return deleteKeyWithPrefix(dir)
}

// pattern是指含有通配符的路径(必须是绝对路径), fixedPrefix是pattern中可以确定的前缀部分,用于etcd前缀查询(越长越好)
// 例: /aaa/bb*/filename, fixedPrefix最长为/aaa/bb, pattern为/aaa/bb*/filename
func GetFilesOfDirByPattern(fixedPrefix string, pattern string) (matchedFiles []string, err error) {
	files, err := GetFilesOfDir(fixedPrefix)
	if err != nil {
		return nil, err
	}

	pattern = strings.ReplaceAll(pattern, "*", ".*")
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if re.MatchString(file) {
			matchedFiles = append(matchedFiles, file)
		}
	}
	return matchedFiles, nil
}

func Rename(oldPath, newPath string) error {
	content, err := getValueOfKey(oldPath)
	if err != nil {
		return err
	}
	err = deleteKey(oldPath)
	if err != nil {
		return err
	}
	err = putValueOfKey(newPath, string(content))
	return err
}

//func getValuesOfKey(key string) (values [][]byte, err error) {
//	cli := GetCli()
//	resp, err := cli.Get(context.Background(), key, clientv3.WithPrefix())
//	if err != nil {
//		return nil, err
//	}
//	for _, kv := range resp.Kvs {
//		values = append(values, kv.Value)
//	}
//	return values, nil
//}

func getValueOfKey(key string) (content []byte, err error) {
	cli := GetCli()
	resp, err := cli.Get(context.Background(), key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, errors.New("key does not exist")
	}
	return resp.Kvs[0].Value, nil
}

func getSbuKeysOfKey(key string) (subKeys []string, err error) {
	cli := GetCli()
	resp, err := cli.Get(context.Background(), key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	for _, kv := range resp.Kvs {
		subKeys = append(subKeys, string(kv.Key))
	}
	return subKeys, nil
}

func putValueOfKey(key string, value string) error {
	cli := GetCli()
	_, err := cli.Put(context.Background(), key, value)
	if err != nil {
		return err
	}
	return nil
}

func deleteKey(key string) error {
	cli := GetCli()
	_, err := cli.Delete(context.Background(), key)
	return err
}

func deleteKeyWithPrefix(key string) error {
	cli := GetCli()
	_, err := cli.Delete(context.Background(), key, clientv3.WithPrefix())
	return err
}
