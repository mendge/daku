package config

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/dagu-dev/dagu/internal/utils"
	"github.com/spf13/viper"
)

const (
	appHomeEnv     = "DAGU_HOME"
	appHomeDefault = ".dagu"
)

var (
	// dagu配置 全局 单实例
	instance *Config = nil
	mu       sync.Mutex
)

// dagu 配置项
type Config struct {
	Host string
	Port int
	DAGs string
	// ??
	Command            string
	WorkDir            string
	IsBasicAuth        bool
	BasicAuthUsername  string
	BasicAuthPassword  string
	LogEncodingCharset string
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

// 用于TLS加密协议配置
type TLS struct {
	CertFile string
	KeyFile  string
}

func Get() *Config {
	if instance == nil {
		home, _ := os.UserHomeDir()
		if err := LoadConfig(home); err != nil {
			panic(err)
		}
	}
	return instance
}

// 创建并配置 dagu全局配置
func LoadConfig(userHomeDir string) error {
	mu.Lock()
	defer mu.Unlock()

	appHome := appHomeDir(userHomeDir)

	viper.SetEnvPrefix("dagu")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	// 将环境变量 DAGU_xx 与配置项 "key" 绑定在一起。
	_ = viper.BindEnv("command", "DAGU_EXECUTABLE")
	_ = viper.BindEnv("dags", "DAGU_DAGS_DIR")
	_ = viper.BindEnv("workDir", "DAGU_WORK_DIR")
	_ = viper.BindEnv("isBasicAuth", "DAGU_IS_BASICAUTH")
	_ = viper.BindEnv("basicAuthUsername", "DAGU_BASICAUTH_USERNAME")
	_ = viper.BindEnv("basicAuthPassword", "DAGU_BASICAUTH_PASSWORD")
	_ = viper.BindEnv("logEncodingCharset", "DAGU_LOG_ENCODING_CHARSET")
	_ = viper.BindEnv("baseConfig", "DAGU_BASE_CONFIG")
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
	// 获取当前dagu运行时 的可执行文件路径
	if ex, err := os.Executable(); err == nil {
		command = ex
	}

	// 配置默认值(记得改正host为0.0.0.0)
	viper.SetDefault("host", "0.0.0.0")
	viper.SetDefault("port", "8080")
	viper.SetDefault("command", command)
	viper.SetDefault("dags", path.Join(appHome, "dags"))
	viper.SetDefault("workDir", "")
	viper.SetDefault("isBasicAuth", "0")
	viper.SetDefault("basicAuthUsername", "")
	viper.SetDefault("basicAuthPassword", "")
	viper.SetDefault("logEncodingCharset", "")
	viper.SetDefault("baseConfig", path.Join(appHome, "config.yaml"))
	viper.SetDefault("logDir", path.Join(appHome, "logs"))
	viper.SetDefault("dataDir", path.Join(appHome, "data"))
	viper.SetDefault("suspendFlagsDir", path.Join(appHome, "suspend"))
	viper.SetDefault("adminLogsDir", path.Join(appHome, "logs", "admin"))
	viper.SetDefault("navbarColor", "")
	viper.SetDefault("navbarTitle", "Dagu")
	viper.SetDefault("isAuthToken", "0")
	viper.SetDefault("authToken", "0")

	// 动态监控前面绑定的Env,变化就重新绑定
	viper.AutomaticEnv()
	// 尝试从当前工作目录中读取名为 config 的配置文件
	_ = viper.ReadInConfig()

	// 将 Viper 实例中的配置信息解析到这个结构体指针变量中
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

// /api/v1
func (cfg *Config) GetAPIBaseURL() string {
	return "/api/v1"
}

// 将全局配置的 env map 配置到系统环境变量 && 载入系统的$PATH和$HOME的环境变量到 env map
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

// 向老版本兼容 For backward compatibility.
func loadLegacyEnvs() {
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
	v := os.Getenv(env)
	if v == "" {
		return def
	}
	return v
}

func parseInt(s string) int {
	i, _ := strconv.Atoi(s)
	return i
}

func getEnvI(env string, def int) int {
	v := os.Getenv(env)
	if v == "" {
		return def
	}
	return parseInt(v)
}

// 先从环境变量获取dagu_home, 获取不到给定默认的 $HOME/.dagu
func appHomeDir(userHomeDir string) string {
	appDir := os.Getenv(appHomeEnv)
	if appDir == "" {
		return path.Join(userHomeDir, appHomeDefault)
	}
	return appDir
}
