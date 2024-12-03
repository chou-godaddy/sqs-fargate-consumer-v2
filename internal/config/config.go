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

type QueueConfig struct {
	URL          string   `json:"url"`          // URL of the queue
	Name         string   `json:"name"`         // Name of the queue
	Priority     int      `json:"priority"`     // 1-3, higher number = higher priority
	Weight       float64  `json:"weight"`       // Weight for queue selection (0-1)
	MaxBatchSize int      `json:"maxBatchSize"` // Maximum number of messages to poll at once
	PollInterval Duration `json:"pollInterval"` // Minimum time between polls
}

type ConsumerGroupConfig struct {
	MaxWorkers int           `json:"maxWorkers"` // Maximum number of worker consumers
	MinWorkers int           `json:"minWorkers"` // Minimum number of worker consumers
	BufferSize int           `json:"bufferSize"` // Size of the buffer channel
	Queues     []QueueConfig `json:"queues"`     // Queues to consume from
}

type ScalerConfig struct {
	MetricsWindow      Duration `json:"metricsWindow"`      // How long to keep metrics
	ScaleUpThreshold   float64  `json:"scaleUpThreshold"`   // Percentage (0-1)
	ScaleDownThreshold float64  `json:"scaleDownThreshold"` // Percentage (0-1)
	ScaleUpCooldown    Duration `json:"scaleUpCooldown"`    // Minimum time between scale ups
	ScaleDownCooldown  Duration `json:"scaleDownCooldown"`  // Minimum time between scale downs
	ScalingUpTicker    Duration `json:"scalingUpTicker"`    // How often to check for scaling up
	ScalingDownTicker  Duration `json:"scalingDownTicker"`  // How often to check for scaling down
}

type MetricsConfig struct {
	Namespace       string   `json:"namespace"`       // CloudWatch namespace
	Region          string   `json:"region"`          // AWS region
	PublishInterval Duration `json:"publishInterval"` // How often to publish metrics
	RetentionPeriod Duration `json:"retentionPeriod"` // How long to keep metrics
	MaxDataPoints   int      `json:"maxDataPoints"`   // Maximum number of data points to store
}

type BufferConfig struct {
	InitialSize           int     `json:"initialSize"`           // Initial buffer size
	MaxSize               int     `json:"maxSize"`               // Maximum buffer size
	HighPriorityPercent   float64 `json:"highPriorityPercent"`   // Percentage for high priority (0-1)
	MediumPriorityPercent float64 `json:"mediumPriorityPercent"` // Percentage for medium priority (0-1)
	LowPriorityPercent    float64 `json:"lowPriorityPercent"`    // Percentage for low priority (0-1)
	ScaleUpThreshold      float64 `json:"scaleUpThreshold"`      // Utilization threshold to scale up (0-1)
	ScaleDownThreshold    float64 `json:"scaleDownThreshold"`    // Utilization threshold to scale down (0-1)
	MaxMessageSize        int64   `json:"maxMessageSize"`        // Maximum size of a single message
	MemoryLimit           int64   `json:"memoryLimit"`           // Maximum total memory usage
	ScaleIncrement        float64 `json:"scaleIncrement"`        // How much to scale by (e.g., 1.2 = 20% increase)
	MaxOverflowCount      int64   `json:"maxOverflowCount"`      // Maximum number of overflows before scaling
}

type ProcessorConfig struct {
	MaxConcurrency     int      // Maximum number of concurrent message processors
	MinConcurrency     int      // Minimum number of concurrent processors to maintain
	ProcessTimeout     Duration // Maximum time to process a single message
	ScaleUpThreshold   float64  // Buffer utilization threshold to scale up processors (0-1)
	ScaleDownThreshold float64  // Buffer utilization threshold to scale down processors (0-1)
	ScaleInterval      Duration // How often to check scaling
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
	ConsumerGroupConfig        ConsumerGroupConfig    `json:"consumer"`
	MetricsConfig              MetricsConfig          `json:"metrics"`
	BufferConfig               BufferConfig           `json:"buffer"`
	ProcessorConfig            ProcessorConfig        `json:"processor"`
	ScalerConfig               ScalerConfig           `json:"scaler"`
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

// Validate performs validation of the configuration
func (c *Config) Validate() error {
	if c.ConsumerGroupConfig.MinWorkers <= 0 {
		return fmt.Errorf("minimum workers must be greater than 0")
	}
	if c.ConsumerGroupConfig.MaxWorkers < c.ConsumerGroupConfig.MinWorkers {
		return fmt.Errorf("maximum workers must be greater than or equal to minimum workers")
	}
	if c.BufferConfig.HighPriorityPercent+c.BufferConfig.MediumPriorityPercent+c.BufferConfig.LowPriorityPercent != 1.0 {
		return fmt.Errorf("buffer priority percentages must sum to 1.0")
	}
	// Add more validation
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
	return nil
}
