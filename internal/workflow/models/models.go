package models

import (
	"github.com/gdcorp-domains/fulfillment-goapimodels/rules/agentservices"
	"github.com/gdcorp-domains/fulfillment-registrar-domains/models"
)

// DomainEventInput contains the inputs for domains workflows and will be stored in action record
type DomainEventInput struct {
	CustomerID              *string                     `json:"customerId" valid:"optional,omitempty"`
	LastFunctionGraph       *string                     `json:"lastFunctionGraph,omitempty"`
	RegistrarBackend        models.RegistrarBackendType `json:"registrarBackend"`
	AgentMessage            *agentservices.AgentMessage
	*UpdateLocksActionInput `json:"updateLocksInput,omitempty"`
}

// UpdateLocksActionInput is the input used for updatelocks action workflow
type UpdateLocksActionInput struct {
	DomainsToUpdate    []string
	Lock               bool
	LocksToAddOrRemove []models.DomainClientLock
	Registrar          string
}
