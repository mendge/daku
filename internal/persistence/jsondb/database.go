package jsondb

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/mendge/daku/internal/etcd/estore"
	"github.com/mendge/daku/internal/persistence"
	"github.com/mendge/daku/internal/persistence/model"
	"github.com/mendge/daku/internal/utils"
	"log"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Store is the interface to store dags status in local.
// It stores status in JSON format in a directory as per each dagFile.
// Multiple JSON data can be stored in a single file and each data
// is separated by newline.
// When a data is updated, it appends a new line to the file.
// Only the latest data in a single file can be read.
// When Compact is called, it removes old data.
// Compact must be called only once per file.
type Store struct {
	dir     string
	dagsDir string
	writer  *writer
}

// New creates a new Store with default configuration.
func New(dir, dagsDir string) *Store {
	// dagsDir is used to calculate the directory that is compatible with the old version.
	return &Store{dir: dir, dagsDir: dagsDir}
}

func (store *Store) Update(dagFile, requestId string, st *model.Status) error {
	f, err := store.FindByRequestId(dagFile, requestId)
	if err != nil {
		return err
	}
	w := &writer{target: f.File}
	return w.write(st)
}

// mark
func (store *Store) Open(dagFile string, t time.Time, requestId string) error {
	w, _, err := store.newWriter(dagFile, t, requestId)
	if err != nil {
		return err
	}
	store.writer = w
	return nil
}

func (store *Store) Write(s *model.Status) error {
	return store.writer.write(s)
}

// modified: 压缩.dat文件的状态,仅仅保留最后一行
func (store *Store) Close() error {
	return store.Compact(store.writer.dagFile, store.writer.target)
}

// 读取.dat文件中最新的有效状态行, 解析成model.status对象返回 TOTEST
func ParseFile(fPath string) (*model.Status, error) {
	data, err := estore.GetContentOfFile(fPath)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	if err != nil {
		log.Printf("failed to open file. err: %v", err)
		return nil, err
	}
	var ret *model.Status
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 {
			var m *model.Status
			m, err = model.StatusFromJson(line)
			if err == nil {
				ret = m
				continue
			}
		}
	}
	return ret, nil
}

// NewWriter creates a new writer for a status.
func (store *Store) newWriter(dagFile string, t time.Time, requestId string) (*writer, string, error) {
	f, err := store.newFile(dagFile, t, requestId)
	if err != nil {
		return nil, "", err
	}
	w := &writer{target: f, dagFile: dagFile}
	return w, f, nil
}

// ReadStatusHist returns a list of status files.
func (store *Store) ReadStatusRecent(dagFile string, n int) []*model.StatusFile {
	ret := make([]*model.StatusFile, 0)
	files := store.latest(store.pattern(dagFile)+"*.dat", n)
	for _, file := range files {
		status, err := ParseFile(file)
		if err == nil {
			ret = append(ret, &model.StatusFile{
				File:   file,
				Status: status,
			})
		}
	}
	return ret
}

// ReadStatusToday returns a list of status files.
func (store *Store) ReadStatusToday(dagFile string) (*model.Status, error) {
	file, err := store.latestToday(dagFile, time.Now())
	if err != nil {
		return nil, err
	}
	return ParseFile(file)
}

// FindByRequestId finds a status file by requestId.
func (store *Store) FindByRequestId(dagFile string, requestId string) (*model.StatusFile, error) {
	if requestId == "" {
		return nil, fmt.Errorf("requestId is empty")
	}
	pattern := store.pattern(dagFile) + "*.dat"
	matches, err := estore.GetFilesOfDirByPattern(dagFile, pattern)
	if len(matches) > 0 || err == nil {
		sort.Slice(matches, func(i, j int) bool {
			return strings.Compare(matches[i], matches[j]) >= 0
		})
		for _, f := range matches {
			status, err := ParseFile(f)
			if err != nil {
				log.Printf("parsing failed %s : %s", f, err)
				continue
			}
			if status != nil && status.RequestId == requestId {
				return &model.StatusFile{
					File:   f,
					Status: status,
				}, nil
			}
		}
	}
	return nil, fmt.Errorf("%w : %s", persistence.ErrRequestIdNotFound, requestId)
}

// RemoveAll removes all files in a directory.
func (store *Store) RemoveAll(dagFile string) error {
	return store.RemoveOld(dagFile, 0)
}

// RemoveOld removes old files.
func (store *Store) RemoveOld(dagFile string, retentionDays int) error {
	pattern := store.pattern(dagFile) + "*.dat"
	var lastErr error = nil
	if retentionDays >= 0 {
		matches, _ := estore.GetFilesOfDirByPattern(dagFile, pattern)
		tBaseline := time.Now().AddDate(0, 0, -1*retentionDays)
		for _, fPath := range matches {
			tSave, err := time.Parse(tLayout, timestamp(fPath))
			if err == nil {
				if tSave.Before(tBaseline) {
					lastErr = estore.RemoveFile(fPath)
				}
			}
		}
	}
	return lastErr
}

// Compact creates a new file with only the latest data and removes old data.
func (store *Store) Compact(_, original string) error {
	status, err := ParseFile(original)
	if err != nil {
		return err
	}
	// 新建
	newFileName := fmt.Sprintf("%s_c.dat", utils.GetFileNameFromPath(original))
	fPath := path.Join(filepath.Dir(original), newFileName)
	w := &writer{target: fPath}
	if err := w.write(status); err != nil {
		if err := estore.RemoveFile(fPath); err != nil {
			log.Printf("failed to remove %s : %s", fPath, err.Error())
		}
		return err
	}
	// 删除
	return estore.RemoveFile(original)
}

func (store *Store) normalizeInternalName(name string) string {
	a := strings.TrimSuffix(name, ".yaml")
	a = strings.TrimSuffix(a, ".yml")
	a = path.Join(store.dagsDir, a)
	return fmt.Sprintf("%s.yaml", a)
}

func (store *Store) Rename(oldName, newName string) error {
	// This is needed to ensure backward compatibility.
	oDagPath := store.normalizeInternalName(oldName)
	nDagPath := store.normalizeInternalName(newName)
	// 具体的.dat文件存储路径, 比如 $data/dagName-md5(dagPath)/
	oldDir := store.directory(oDagPath, utils.GetFileNameFromPath(oDagPath))
	newDir := store.directory(nDagPath, utils.GetFileNameFromPath(nDagPath))
	if !estore.StatExist(oldDir) {
		// Nothing to do
		return nil
	}
	// 获取old dag的所有.dat文件,复制为new dag的.dat文件
	onp := store.pattern(oDagPath) + "*.dat"
	matches, err := estore.GetFilesOfDirByPattern(store.pattern(oDagPath), onp)
	if err != nil {
		return err
	}
	for _, m := range matches {
		oDatName := path.Base(m)
		nDatName := strings.Replace(oDatName, oldName, newName, 1)
		content, err := estore.GetContentOfFile(m)
		if err != nil {
			return err
		}
		err = estore.SaveFile(path.Join(newDir, nDatName), string(content))
		if err != nil {
			return err
		}
	}
	// 删除old dag的dat目录
	return estore.RemoveDir(oldDir)
}

// 根据dagPath 和 dagName,将dagPath进行md5哈西,返回一个目录 daguDataDir/DagName-<md5>
func (store *Store) directory(dagPath string, dagName string) string {
	h := md5.New()
	h.Write([]byte(dagPath))
	v := hex.EncodeToString(h.Sum(nil))
	return filepath.Join(store.dir, fmt.Sprintf("%s-%s", dagName, v))
}

// 获取格式化的文件路径
func (store *Store) newFile(dagFile string, t time.Time, requestId string) (string, error) {
	if dagFile == "" {
		return "", fmt.Errorf("dagFile is empty")
	}
	fileName := fmt.Sprintf("%s.%s.%s.dat", store.pattern(dagFile), t.Format("20060102.15:04:05.000"), utils.TruncString(requestId, 8))
	return fileName, nil
}

// 返回 daguDataDir/dageName-<md5>/dagName, 后续用于生成today的dat文件的通配字符串
func (store *Store) pattern(dagPath string) string {
	dagName := utils.GetFileNameFromPath(dagPath)
	dir := store.directory(dagPath, dagName)
	return filepath.Join(dir, dagName)
}

// TOTEST
func (store *Store) latestToday(dagFile string, now time.Time) (string, error) {
	var ret []string
	dagStatueDir := store.pattern(dagFile)
	today := now.Format("20060102")
	pattern := fmt.Sprintf("%s.%s*.*.dat", dagStatueDir, today)

	matches, err := estore.GetFilesOfDirByPattern(dagStatueDir, pattern)
	if err == nil || len(matches) > 0 {
		ret = filterLatest(matches, 1)
	} else {
		return "", persistence.ErrNoStatusDataToday
	}
	if len(ret) == 0 {
		return "", persistence.ErrNoStatusData
	}
	return ret[0], err
}

func (store *Store) latest(pattern string, n int) []string {
	matches, err := estore.GetFilesOfDirByPattern(store.dir, pattern)
	var ret = []string{}
	if err == nil || len(matches) >= 0 {
		ret = filterLatest(matches, n)
	}
	return ret
}

var rTimestamp = regexp.MustCompile(`2\d{7}.\d{2}:\d{2}:\d{2}`)
var tLayout = "20060102.15:04:05"

func filterLatest(files []string, n int) []string {
	if len(files) == 0 {
		return []string{}
	}
	sort.Slice(files, func(i, j int) bool {
		t1 := timestamp(files[i])
		t2 := timestamp(files[j])
		return t1 > t2
	})
	ret := make([]string, 0, n)
	for i := 0; i < n && i < len(files); i++ {
		ret = append(ret, files[i])
	}
	return ret
}

func timestamp(file string) string {
	return rTimestamp.FindString(file)
}
