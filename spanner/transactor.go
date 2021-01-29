package spanner

import (
	"context"
	"errors"

	"github.com/knocknote/gotx"

	"cloud.google.com/go/spanner"
)

type contextCurrentTransactionKey string

const currentTransactionKey contextCurrentTransactionKey = "current_spanner_transaction"

type DefaultClientProvider struct {
	connectionProvider ConnectionProvider
	client             *spanner.Client
}

func NewDefaultClientProvider(connectionProvider ConnectionProvider) *DefaultClientProvider {
	return &DefaultClientProvider{
		connectionProvider: connectionProvider,
	}
}

func (p *DefaultClientProvider) CurrentClient(ctx context.Context) Client {
	transaction := ctx.Value(currentTransactionKey)
	if transaction == nil {
		return NewDefaultTxClient(p.connectionProvider.CurrentConnection(ctx), nil, nil)
	}
	return transaction.(Client)
}

type Transactor struct {
	provider ConnectionProvider
}

func NewTransactor(provider ConnectionProvider) *Transactor {
	return &Transactor{
		provider: provider,
	}
}

func (t *Transactor) Required(ctx context.Context, fn gotx.DoInTransaction, options ...gotx.Option) error {
	if ctx.Value(currentTransactionKey) != nil {
		return fn(ctx)
	}
	return t.RequiresNew(ctx, fn, options...)
}

var rollbackOnly = errors.New("rollback only transaction")

func (t *Transactor) RequiresNew(ctx context.Context, fn gotx.DoInTransaction, options ...gotx.Option) error {

	spannerConfig := newConfig()
	for _, opt := range options {
		opt.Apply(&spannerConfig)
	}

	spannerClient := t.provider.CurrentConnection(ctx)
	if spannerConfig.ReadOnly {
		txn := spannerClient.ReadOnlyTransaction()

		// use stale read if needed
		if spannerConfig.VendorOption != nil {
			conf := spannerConfig.VendorOption.(*config)
			if conf.timestampBoundEnabled {
				txn = txn.WithTimestampBound(conf.timestampBound)
			}
		}
		defer txn.Close()
		executor := NewDefaultTxClient(spannerClient, nil, txn)
		return fn(context.WithValue(ctx, currentTransactionKey, executor))
	}
	_, err := spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		executor := NewDefaultTxClient(spannerClient, txn, nil)
		err := fn(context.WithValue(ctx, currentTransactionKey, executor))
		if err != nil {
			return err
		}
		if spannerConfig.RollbackOnly {
			return rollbackOnly
		}
		return nil
	})
	// rollback only transaction
	if err != nil && errors.Is(err, rollbackOnly) {
		return nil
	}
	return err
}
