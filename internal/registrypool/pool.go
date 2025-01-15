package registrypool

import (
	"context"
	"go.uber.org/ratelimit"
)

type RegistryType string

const (
	RegistryVerisign RegistryType = "verisign"
	// RegistryCentralNic RegistryType = "centralnic"
	// RegistryHexonet    RegistryType = "hexonet"
)

type RegistryPool struct {
	registryType RegistryType
	limiter      ratelimit.Limiter
}

func NewRegistryPool(registryType RegistryType, ratePerSec int) *RegistryPool {
	return &RegistryPool{
		registryType: registryType,
		limiter:      ratelimit.New(ratePerSec),
	}
}

func (p *RegistryPool) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		p.limiter.Take()
		return nil
	}
}
