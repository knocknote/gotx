package spanner

import (
	"context"

	"cloud.google.com/go/spanner"
)

type ConnectionProvider interface {
	CurrentConnection(ctx context.Context) *spanner.Client
}

type TransactionProvider interface {
	CurrentTransaction(ctx context.Context) TxClient
}

type DefaultConnectionProvider struct {
	client *spanner.Client
}

func NewDefaultConnectionProvider() *DefaultTransactionProvider {
	return &DefaultTransactionProvider{}
}

func (p *DefaultConnectionProvider) CurrentConnection(_ context.Context) *spanner.Client {
	return p.client
}
