package config

import (
	"fmt"
	"github.com/mendge/daku/internal/etcdstore"
	"github.com/mendge/daku/internal/utils"
	"github.com/spf13/viper"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Config struct {
	Host               string
	Port               int
	DAGs               string
	Command            string
	WorkDir            string
	IsBasicAuth        bool
	BasicAuthUsername  string
	BasicAuthPassword  string
	LogEncodingCharset string
	DefaultWorkDir     string
	TmpLogDir          string
	LogDir             string
	DataDir            string
	SuspendFlagsDir    string
	AdminLogsDir       string
	BaseConfig         string
	NavbarColor        string
	NavbarTitle        string
	Env                map[string]string
	TLS                *TLS
	IsAuthToken        bool
	AuthToken          string
}

type TLS struct {
	CertFile string
	KeyFile  string
}

var mu sync.Mutex
var instance *Config = nil

func Get() *Config {
	if instance == nil {
		if err := LoadConfig(); err != nil {
			panic(err)
		}
	}
	return instance
}

func LoadConfig() error {
	mu.Lock()
	defer mu.Unlock()

	viper.SetEnvPrefix("dagu")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	_ = viper.BindEnv("command", "DAGU_EXECUTABLE")
	_ = viper.BindEnv("dags", "DAGU_DAGS_DIR")
	_ = viper.BindEnv("workDir", "DAGU_WORK_DIR")
	_ = viper.BindEnv("isBasicAuth", "DAGU_IS_BASICAUTH")
	_ = viper.BindEnv("basicAuthUsername", "DAGU_BASICAUTH_USERNAME")
	_ = viper.BindEnv("basicAuthPassword", "DAGU_BASICAUTH_PASSWORD")
	_ = viper.BindEnv("logEncodingCharset", "DAGU_LOG_ENCODING_CHARSET")
	_ = viper.BindEnv("baseConfig", "DAGU_BASE_CONFIG")
	_ = viper.BindEnv("defaultWorkDir", "DAGU_Default_Work_Dir")
	_ = viper.BindEnv("tmpLogDir", "DAGU_TMP_LOG_DIR")
	_ = viper.BindEnv("logDir", "DAGU_LOG_DIR")
	_ = viper.BindEnv("dataDir", "DAGU_DATA_DIR")
	_ = viper.BindEnv("suspendFlagsDir", "DAGU_SUSPEND_FLAGS_DIR")
	_ = viper.BindEnv("adminLogsDir", "DAGU_ADMIN_LOG_DIR")
	_ = viper.BindEnv("navbarColor", "DAGU_NAVBAR_COLOR")
	_ = viper.BindEnv("navbarTitle", "DAGU_NAVBAR_TITLE")
	_ = viper.BindEnv("tls.certFile", "DAGU_CERT_FILE")
	_ = viper.BindEnv("tls.keyFile", "DAGU_KEY_FILE")
	_ = viper.BindEnv("isAuthToken", "DAGU_IS_AUTHTOKEN")
	_ = viper.BindEnv("authToken", "DAGU_AUTHTOKEN")
	command := "dagu"
	if ex, err := os.Executable(); err == nil {
		command = ex
	}

	viper.SetDefault("host", "127.0.0.1")
	viper.SetDefault("port", "8081")
	viper.SetDefault("command", command)
	viper.SetDefault("dags", etcdstore.DagDir)
	viper.SetDefault("workDir", "")
	viper.SetDefault("isBasicAuth", "0")
	viper.SetDefault("basicAuthUsername", "")
	viper.SetDefault("basicAuthPassword", "")
	viper.SetDefault("logEncodingCharset", "")
	viper.SetDefault("baseConfig", etcdstore.BaseConfigPath)
	viper.SetDefault("defaultWorkDir", "/tmp/dagu/workdir")
	viper.SetDefault("tmpLogDir", "/tmp/dagu/logs")
	viper.SetDefault("logDir", etcdstore.LogDir)
	viper.SetDefault("dataDir", etcdstore.StatueDir)
	viper.SetDefault("suspendFlagsDir", etcdstore.SuspendDir)
	viper.SetDefault("adminLogsDir", etcdstore.AdminLogDir)
	viper.SetDefault("navbarColor", "")
	viper.SetDefault("navbarTitle", "mendge's Daku")
	viper.SetDefault("isAuthToken", "0")
	viper.SetDefault("authToken", "0")

	viper.AutomaticEnv()

	// TODO 从etcd导入
	// _ = viper.ReadInConfig()

	cfg := &Config{}
	err := viper.Unmarshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to unmarshal cfg file: %w", err)
	}
	instance = cfg
	loadLegacyEnvs()
	loadEnvs()

	return nil
}

func (cfg *Config) GetAPIBaseURL() string {
	return "/api/v1"
}

func loadEnvs() {
	if instance.Env == nil {
		instance.Env = map[string]string{}
	}
	for k, v := range instance.Env {
		_ = os.Setenv(k, v)
	}
	for k, v := range utils.DefaultEnv() {
		if _, ok := instance.Env[k]; !ok {
			instance.Env[k] = v
		}
	}
}

func loadLegacyEnvs() {
	// For backward compatibility.
	instance.NavbarColor = getEnv("DAGU__ADMIN_NAVBAR_COLOR", instance.NavbarColor)
	instance.NavbarTitle = getEnv("DAGU__ADMIN_NAVBAR_TITLE", instance.NavbarTitle)
	instance.Port = getEnvI("DAGU__ADMIN_PORT", instance.Port)
	instance.Host = getEnv("DAGU__ADMIN_HOST", instance.Host)
	instance.DataDir = getEnv("DAGU__DATA", instance.DataDir)
	instance.LogDir = getEnv("DAGU__DATA", instance.LogDir)
	instance.SuspendFlagsDir = getEnv("DAGU__SUSPEND_FLAGS_DIR", instance.SuspendFlagsDir)
	instance.BaseConfig = getEnv("DAGU__SUSPEND_FLAGS_DIR", instance.BaseConfig)
	instance.AdminLogsDir = getEnv("DAGU__ADMIN_LOGS_DIR", instance.AdminLogsDir)
}

func getEnv(env, def string) string {
	v := etcdstore.GetEnvValue(env)
	if v == "" {
		return def
	}
	return v
}

func getEnvI(env string, def int) int {
	v := etcdstore.GetEnvValue(env)
	if v == "" {
		return def
	}
	i, _ := strconv.Atoi(v)
	return i
}
