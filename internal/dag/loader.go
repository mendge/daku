package dag

import (
	"bytes"
	"fmt"
	"github.com/imdario/mergo"
	"github.com/mendge/daku/internal/etcdstore"
	"github.com/mendge/daku/internal/utils"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/yaml.v2"
	"path/filepath"
	"reflect"
	"strings"
)

// Loader is a config loader.
type Loader struct {
	BaseConfig string
}

// Load loads config from file.
func (cl *Loader) Load(f, params string) (*DAG, error) {
	return cl.loadWithOptions(f, params, false, false, false)
}

// LoadWithoutEval loads config from file without evaluating env variables.
func (cl *Loader) LoadWithoutEval(f string) (*DAG, error) {
	return cl.loadWithOptions(f, "", false, true, true)
}

// LoadMetadata loads config from file and returns only the headline data.
func (cl *Loader) LoadMetadata(f string) (*DAG, error) {
	return cl.loadWithOptions(f, "", true, true, true)
}

// loadWithOptions loads the config file with the provided options.
func (cl *Loader) loadWithOptions(f, params string, loadMetadataOnly, skipEnvEval, skipEnvSetup bool) (*DAG, error) {
	return cl.loadDAG(f,
		&BuildDAGOptions{
			parameters:       params,
			loadMetadataOnly: loadMetadataOnly,
			skipEnvEval:      skipEnvEval,
			skipEnvSetup:     skipEnvSetup,
		},
	)
}

// LoadData loads config from given data.
func (cl *Loader) LoadData(data []byte) (*DAG, error) {
	fl := &fileLoader{}
	raw, err := fl.unmarshalData(data)
	if err != nil {
		return nil, err
	}
	cdl := &configDefinitionLoader{}
	def, err := cdl.decode(raw)
	if err != nil {
		return nil, err
	}
	b := &DAGBuilder{
		options: BuildDAGOptions{loadMetadataOnly: false, skipEnvEval: true, skipEnvSetup: true},
	}
	return b.buildFromDefinition(def, nil)
}

func (cl *Loader) loadBaseConfig(fpath string, opts *BuildDAGOptions) (*DAG, error) {
	if !etcdstore.FileExist(fpath) {
		return nil, nil
	}
	raw, err := cl.readFile(fpath)
	if err != nil {
		return nil, err
	}

	cdl := &configDefinitionLoader{}
	def, err := cdl.decode(raw)
	if err != nil {
		return nil, err
	}

	buildOpts := *opts
	buildOpts.loadMetadataOnly = false
	// TODO 环境变量从etcd载入
	buildOpts.defaultEnvs = utils.DefaultEnv()
	b := &DAGBuilder{
		options: buildOpts,
	}
	return b.buildFromDefinition(def, nil)
}

func (cl *Loader) loadDAG(fpath string, opts *BuildDAGOptions) (*DAG, error) {
	dst, err := cl.loadBaseConfigIfRequired(fpath, opts)
	if err != nil {
		return nil, err
	}

	raw, err := cl.readFile(fpath)
	if err != nil {
		return nil, err
	}

	cdl := &configDefinitionLoader{}

	def, err := cdl.decode(raw)
	if err != nil {
		return nil, err
	}

	b := DAGBuilder{options: *opts}
	c, err := b.buildFromDefinition(def, dst)

	if err != nil {
		return nil, err
	}

	err = cdl.merge(dst, c)
	if err != nil {
		return nil, err
	}

	dst.Location = fpath

	if !opts.skipEnvSetup {
		dst.setup()
	}

	return dst, nil
}

// loadBaseConfigIfRequired loads the base config if needed, based on the given options.
func (cl *Loader) loadBaseConfigIfRequired(file string, opts *BuildDAGOptions) (*DAG, error) {
	if !opts.loadMetadataOnly && cl.BaseConfig != "" {
		dag, err := cl.loadBaseConfig(cl.BaseConfig, opts)
		if err != nil {
			return nil, err
		}
		if dag != nil {
			return dag, nil
		}
	}
	return &DAG{Name: strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))}, nil
}

type mergeTransformer struct{}

var _ mergo.Transformers = (*mergeTransformer)(nil)

func (mt *mergeTransformer) Transformer(typ reflect.Type) func(dst, src reflect.Value) error {
	if typ == reflect.TypeOf(MailOn{}) {
		return func(dst, src reflect.Value) error {
			if dst.CanSet() {
				dst.Set(src)
			}
			return nil
		}
	}
	return nil
}

func (cl *Loader) readFile(file string) (config map[string]interface{}, err error) {
	fl := &fileLoader{}
	return fl.readFile(file)
}

// fileLoader is a helper struct to load and process configuration files.
type fileLoader struct{}

// readFile reads the contents of the file into a map.
func (fl *fileLoader) readFile(file string) (config map[string]interface{}, err error) {
	data, err := etcdstore.GetContentOfFile(file)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", file, err)
	}
	return fl.unmarshalData(data)
}

// unmarshalData unmarshals the data into a map.
func (fl *fileLoader) unmarshalData(data []byte) (map[string]interface{}, error) {
	var cm map[string]interface{}
	err := yaml.NewDecoder(bytes.NewReader(data)).Decode(&cm)
	return cm, err
}

// configDefinitionLoader is a helper struct to decode and merge configuration definitions.
type configDefinitionLoader struct{}

// decode decodes the configuration map into a configDefinition.
func (cdl *configDefinitionLoader) decode(cm map[string]interface{}) (*configDefinition, error) {
	c := &configDefinition{}
	md, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		ErrorUnused: true,
		Result:      c,
		TagName:     "",
	})
	err := md.Decode(cm)
	return c, err
}

// merge merges the source DAG into the destination DAG.
func (cdl *configDefinitionLoader) merge(dst, src *DAG) error {
	err := mergo.Merge(dst, src, mergo.WithOverride,
		mergo.WithTransformers(&mergeTransformer{}))
	return err
}

// prepareFilepath 被删除, 因为传入的etcd path已经是绝对路径
// load 被删除, 因为仅仅调用 readFile
// readFile 被修改, 改为从etcd中读入yaml
