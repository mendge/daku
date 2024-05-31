package local

import (
	"fmt"
	"github.com/mendge/daku/internal/dag"
	"github.com/mendge/daku/internal/etcd/estore"
	"github.com/mendge/daku/internal/grep"
	"github.com/mendge/daku/internal/persistence"
	"github.com/mendge/daku/internal/utils"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type dagStoreImpl struct {
	dir string
}

func NewDAGStore(dir string) persistence.DAGStore {
	return &dagStoreImpl{dir: dir}
}

func (d *dagStoreImpl) GetMetadata(name string) (*dag.DAG, error) {
	fPath := d.fileLocation(name)
	cl := dag.Loader{}
	dat, err := cl.LoadMetadata(fPath)
	if err != nil {
		return nil, err
	}
	return dat, nil
}

func (d *dagStoreImpl) GetDetails(name string) (*dag.DAG, error) {
	fPath := d.fileLocation(name)
	cl := dag.Loader{}
	dat, err := cl.LoadWithoutEval(fPath)
	if err != nil {
		return nil, err
	}
	return dat, nil
}

func (d *dagStoreImpl) GetSpec(name string) (string, error) {
	fPath := d.fileLocation(name)
	data, err := estore.GetContentOfFile(fPath)
	if err != nil {
		return "", fmt.Errorf("failed to read DAG spec file: %s", err)
	}
	return string(data), nil
}

func (d *dagStoreImpl) UpdateSpec(name string, spec []byte) error {
	// validation
	cl := dag.Loader{}
	_, err := cl.LoadData(spec)
	if err != nil {
		return err
	}

	fPath := d.fileLocation(name)
	if !estore.FileExist(fPath) {
		return fmt.Errorf("the DAG file %s does not exist", fPath)
	}

	err = estore.SaveFile(fPath, string(spec))
	if err != nil {
		return fmt.Errorf("failed to update DAG file: %s", err)
	}
	return nil
}

func (d *dagStoreImpl) Create(name string, spec []byte) (string, error) {
	fPath := d.fileLocation(name)
	if estore.FileExist(fPath) {
		return "", fmt.Errorf("the DAG file %s already exists", fPath)
	}
	return name, estore.SaveFile(fPath, string(spec))
}

func (d *dagStoreImpl) Delete(name string) error {
	fPath := d.fileLocation(name)
	err := estore.RemoveFile(fPath)
	if err != nil {
		return fmt.Errorf("failed to delete DAG file: %s", err)
	}
	return nil
}

func (d *dagStoreImpl) fileLocation(name string) string {
	if strings.Contains(name, "/") {
		// this is for backward compatibility
		return name
	}
	loc := path.Join(d.dir, name)
	return d.normalizeFilename(loc)
}

func (d *dagStoreImpl) normalizeFilename(file string) string {
	a := strings.TrimSuffix(file, ".yaml")
	a = strings.TrimSuffix(a, ".yml")
	return fmt.Sprintf("%s.yaml", a)
}

func (d *dagStoreImpl) List() (ret []*dag.DAG, errs []string, err error) {
	// check dir has files
	fis, err := estore.GetFilesOfDir(d.dir)
	if err != nil {
		errs = append(errs, err.Error())
		return
	}
	for _, fi := range fis {
		if checkExtension(fi) {
			dat, err := d.GetMetadata(fi)
			if err == nil {
				ret = append(ret, dat)
			} else {
				errs = append(errs, fmt.Sprintf("reading %s failed: %s", fi, err))
			}
		}
	}
	return ret, errs, nil
}

var extensions = []string{".yaml", ".yml"}

func checkExtension(file string) bool {
	ext := filepath.Ext(file)
	for _, e := range extensions {
		if e == ext {
			return true
		}
	}
	return false
}

func (d *dagStoreImpl) Grep(pattern string) (ret []*persistence.GrepResult, errs []string, err error) {
	fis, err := os.ReadDir(d.dir)
	dl := &dag.Loader{}
	opts := &grep.Options{
		IsRegexp: true,
		Before:   2,
		After:    2,
	}

	utils.LogErr("read DAGs directory", err)
	for _, fi := range fis {
		if utils.MatchExtension(fi.Name(), dag.EXTENSIONS) {
			file := filepath.Join(d.dir, fi.Name())
			dat, err := os.ReadFile(file)
			if err != nil {
				utils.LogErr("read DAG file", err)
				continue
			}
			m, err := grep.Grep(dat, fmt.Sprintf("(?i)%s", pattern), opts)
			if err != nil {
				errs = append(errs, fmt.Sprintf("grep %s failed: %s", fi.Name(), err))
				continue
			}
			d, err := dl.LoadMetadata(file)
			if err != nil {
				errs = append(errs, fmt.Sprintf("check %s failed: %s", fi.Name(), err))
				continue
			}
			ret = append(ret, &persistence.GrepResult{
				Name:    strings.TrimSuffix(fi.Name(), path.Ext(fi.Name())),
				DAG:     d,
				Matches: m,
			})
		}
	}
	return ret, errs, nil
}

func (d *dagStoreImpl) Load(name string) (*dag.DAG, error) {
	//TODO implement me
	panic("implement me")
}

func (d *dagStoreImpl) Rename(oldDAGPath, newDAGPath string) error {
	oldPath := d.fileLocation(oldDAGPath)
	newPath := d.fileLocation(newDAGPath)
	if err := estore.Rename(oldPath, newPath); err != nil {
		return err
	}
	return nil
}

// ensureDirExist 被删除,因为etcd put dir直接能够保证dir存在
