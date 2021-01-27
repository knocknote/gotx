package spanner

import (
	"context"

	"cloud.google.com/go/spanner"
)

type TransactionProvider interface {
	currentConnection(ctx context.Context) *spanner.Client
	CurrentTransaction(ctx context.Context) TxClient
}

type DefaultConnectionProvider struct {
	client *spanner.Client
}

func (p *DefaultConnectionProvider) currentConnection(_ context.Context) *spanner.Client {
	return p.client
}

func (p *DefaultConnectionProvider) CurrentTransaction(ctx context.Context) TxClient {
	transaction := ctx.Value(currentTransactionKey)
	if transaction == nil {
		return NewDefaultTxClient(p.currentConnection(ctx), nil, nil)
	}
	return transaction.(TxClient)
}
