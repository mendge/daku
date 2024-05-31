package jsondb

import (
	"github.com/mendge/daku/internal/etcd/estore"
	"github.com/mendge/daku/internal/persistence/model"
	"github.com/mendge/daku/internal/utils"
	"regexp"
	"sync"
)

// modified： 移除不需要的变量
// Writer is the interface to write status to local file.
type writer struct {
	target  string
	dagFile string
	mu      sync.Mutex
}

// Writer appends the status to the etcd .dat file
func (w *writer) write(st *model.Status) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	jsonb, _ := st.ToJson()
	re, _ := regexp.Compile("\n\r")
	jsonb = re.ReplaceAll(jsonb, []byte(""))

	oldContent, _ := estore.GetContentOfFile(w.target)
	oldContent = append(oldContent, '\n')
	newContent := append(oldContent, jsonb...)

	err := estore.SaveFile(w.target, string(newContent))
	utils.LogErr("write status", err)

	return err
}
