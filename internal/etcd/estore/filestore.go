package estore

import (
	"context"
	"path"
	"regexp"
	"strings"
)

func GetFilesOfDir(dir string) (files []string, err error) {
	files, err = GetSbuKeysOfKey(context.Background(), dir)
	if err != nil {
		return nil, err
	}
	return files, nil
}

func GetContentOfFile(file string) (content []byte, err error) {
	content, err = GetValueOfKey(context.Background(), file)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func GetEnvValue(key string) string {
	fullEnvKey := path.Join(EnvPath, key)
	value, _ := GetValueOfKey(context.Background(), fullEnvKey)
	return string(value)
}

func FileExist(file string) bool {
	_, err := GetValueOfKey(context.Background(), file)
	if err != nil {
		return false
	}
	return true
}

// Dir Or File
func StatExist(dir string) bool {
	keys, err := GetSbuKeysOfKey(context.Background(), dir)
	if err != nil || keys == nil {
		return false
	}
	return true
}

func SaveFile(file string, content string) error {
	//fmt.Println("etcd save" + file)
	//fmt.Println(content)
	return PutValueOfKey(context.Background(), file, content)
}

func RemoveFile(file string) error {
	return DeleteKey(context.Background(), file)
}

func RemoveDir(dir string) error {
	return DeleteKeyWithPrefix(context.Background(), dir)
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
	content, err := GetValueOfKey(context.Background(), oldPath)
	if err != nil {
		return err
	}
	err = DeleteKey(context.Background(), oldPath)
	if err != nil {
		return err
	}
	err = PutValueOfKey(context.Background(), newPath, string(content))
	return err
}
