package registrypool

import (
	"sqs-fargate-consumer-v2/internal/models"
	"sync"
)

type RegistryPoolManager struct {
	pools map[RegistryType]*RegistryPool
	mu    sync.RWMutex
}

type RegistryConfig struct {
	RatePerSecond int
}

func NewRegistryPoolManager(configs map[RegistryType]RegistryConfig) *RegistryPoolManager {
	pools := make(map[RegistryType]*RegistryPool)
	for regType, cfg := range configs {
		pools[regType] = NewRegistryPool(regType, cfg.RatePerSecond)
	}
	return &RegistryPoolManager{pools: pools}
}

func (m *RegistryPoolManager) GetPool(regType RegistryType) (*RegistryPool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pool, exists := m.pools[regType]
	return pool, exists
}

func (m *RegistryPoolManager) DetermineRegistryType(msg *models.Message) RegistryType {
	return RegistryVerisign
}
