package config

import (
	"encoding/json"
	"fmt"
	goapi "github.com/gdcorp-domains/fulfillment-go-api"
	registrarconfig "github.com/gdcorp-domains/fulfillment-registrar-config"
	rgclient "github.com/gdcorp-domains/fulfillment-rg-client"
	"os"
	"time"
)

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

type QueueConfig struct {
	URL          string   `json:"url"`
	Name         string   `json:"name"`
	Priority     int      `json:"priority"` // 1-3, higher number = higher priority
	Weight       float64  `json:"weight"`   // Weight for queue selection (0-1)
	MaxBatchSize int      `json:"maxBatchSize"`
	PollInterval Duration `json:"pollInterval"`
}

type ConsumerGroupConfig struct {
	MaxWorkers         int           `json:"maxWorkers"`
	MinWorkers         int           `json:"minWorkers"`
	BufferSize         int           `json:"bufferSize"`
	ScaleUpThreshold   float64       `json:"scaleUpThreshold"`   // Percentage (0-1)
	ScaleDownThreshold float64       `json:"scaleDownThreshold"` // Percentage (0-1)
	MetricsWindow      Duration      `json:"metricsWindow"`
	Queues             []QueueConfig `json:"queues"`
	ScaleUpCooldown    Duration      `json:"scaleUpCooldown"`   // Minimum time between scale ups
	ScaleDownCooldown  Duration      `json:"scaleDownCooldown"` // Minimum time between scale downs
	ScalingUpTicker    Duration      `json:"scalingUpTicker"`
	ScalingDownTicker  Duration      `json:"scalingDownTicker"`
}

type MetricsConfig struct {
	Namespace       string   `json:"namespace"`
	Region          string   `json:"region"`
	PublishInterval Duration `json:"publishInterval"`
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
	MaxConcurrency     int           // Maximum number of concurrent message processors
	MinConcurrency     int           // Minimum number of concurrent processors to maintain
	ProcessTimeout     time.Duration // Maximum time to process a single message
	ScaleUpThreshold   float64       // Buffer utilization threshold to scale up processors (0-1)
	ScaleDownThreshold float64       // Buffer utilization threshold to scale down processors (0-1)
	ScaleInterval      time.Duration // How often to check scaling
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
	return nil
}

// LoadConfig loads and validates the configuration from a file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &config, nil
}
