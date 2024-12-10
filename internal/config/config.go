package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"text/template"
	"time"

	goapi "github.com/gdcorp-domains/fulfillment-go-api"
	registrarconfig "github.com/gdcorp-domains/fulfillment-registrar-config"
	rgclient "github.com/gdcorp-domains/fulfillment-rg-client"
)

type ConsumerConfig struct {
	MaxWorkers          int           `json:"maxWorkers"`
	MinWorkers          int           `json:"minWorkers"`
	MinPollInterval     Duration      `json:"minpollInterval"`
	PollBackoffInterval Duration      `json:"pollBackoffInterval"`
	MaxBatchSize        int           `json:"maxBatchSize"`
	ScaleInterval       Duration      `json:"scaleInterval"`
	ScaleThreshold      float64       `json:"scaleThreshold"`
	ScaleUpCoolDown     Duration      `json:"scaleUpCoolDown"`
	ScaleDownCoolDown   Duration      `json:"scaleDownCoolDown"`
	ScaleUpStep         int           `json:"scaleUpStep"`
	ScaleDownStep       int           `json:"scaleDownStep"`
	Queues              []QueueConfig `json:"queues"`
}

type BufferConfig struct {
	TotalCapacity  int     `json:"totalCapacity"`
	MaxMessageSize int64   `json:"maxMessageSize"`
	ScaleThreshold float64 `json:"scaleThreshold"`
}

type ProcessorConfig struct {
	MaxWorkers     int      `json:"maxWorkers"`
	MinWorkers     int      `json:"minWorkers"`
	ProcessTimeout Duration `json:"processTimeout"`
	ScaleInterval  Duration `json:"scaleInterval"`
	ScaleThreshold float64  `json:"scaleThreshold"`
}

type QueueConfig struct {
	Name     string  `json:"name"`
	URL      string  `json:"url"`
	Priority int     `json:"priority"`
	Weight   float64 `json:"weight"`
}

type Config struct {
	goapi.Config
	ActionAPIURL               string                 `json:"actionApiUrl"`
	RegistryContactsURL        string                 `json:"registryContactsUrl"`
	RegistryDomainsURL         string                 `json:"registryDomainsUrl"`
	RegistrarConfig            registrarconfig.Config `json:"registrarConfig"`
	SwitchBoardApplicationName string                 `json:"switchBoardApplicationName"`
	SQLDATAAPIURL              string                 `json:"sqldataAPIURL"`
	DBName                     string                 `json:"database"`
	ShopperAPIURL              string                 `json:"shopperApiUrl"`
	IntlContactsAPIURL         string                 `json:"internationalContactsApiUrl"`
	SwitchboardAPIURL          string                 `json:"switchboardApiUrl"`
	RegistryConfig             rgclient.Config        `json:"registryConfig"`
	MSMQURL                    string                 `json:"genericQueueEndpointUrl"`
	Consumer                   ConsumerConfig         `json:"consumer"`
	Buffer                     BufferConfig           `json:"buffer"`
	Processor                  ProcessorConfig        `json:"processor"`
}

// Duration wraps time.Duration for JSON unmarshaling
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid duration")
	}
	return nil
}

func validateConfig(cfg *Config) error {
	if cfg.Consumer.MaxWorkers < cfg.Consumer.MinWorkers {
		return fmt.Errorf("max workers must be greater than min workers")
	}

	if cfg.Consumer.MaxBatchSize <= 0 || cfg.Consumer.MaxBatchSize > 10 {
		return fmt.Errorf("batch size must be between 1 and 10")
	}

	if cfg.Buffer.MaxMessageSize <= 0 {
		return fmt.Errorf("max message size must be positive")
	}

	if len(cfg.Consumer.Queues) == 0 {
		return fmt.Errorf("at least one queue must be configured")
	}

	for _, queue := range cfg.Consumer.Queues {
		if queue.Priority < 1 || queue.Priority > 3 {
			return fmt.Errorf("queue priority must be between 1 and 3")
		}
		if queue.Weight <= 0 || queue.Weight > 1 {
			return fmt.Errorf("queue weight must be between 0 and 1")
		}
	}

	return nil
}

// LoadConfig loads and validates the configuration from a file
func (conf *Config) Load(configPath string) (err error) {
	var fileContents []byte
	fileContents, err = os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}
	envConfig := struct {
		Env    string
		Region string
		EnvDNS string
	}{
		Env:    os.Getenv("ENV"),
		Region: os.Getenv("AWS_REGION"),
		EnvDNS: os.Getenv("ENV"),
	}
	if envConfig.Env == "dev-private" {
		envConfig.EnvDNS = "dp"
	}

	var t *template.Template
	t, err = template.New("").Parse(string(fileContents))
	if err != nil {
		return err
	}

	buf := bytes.Buffer{}
	if err := t.Execute(&buf, envConfig); err != nil {
		return err
	}

	if err := json.Unmarshal(buf.Bytes(), conf); err != nil {
		return err
	}

	if err := validateConfig(conf); err != nil {
		return fmt.Errorf("failed to validate config: %w", err)
	}

	return nil
}
