package spanner

import (
	"context"
	"errors"

	"github.com/knocknote/gotx"

	"cloud.google.com/go/spanner"
)

type Transactor struct {
	provider TransactionProvider
}

func NewTransactor(provider TransactionProvider) *Transactor {
	return &Transactor{
		provider: provider,
	}
}

func (t *Transactor) Required(ctx context.Context, fn gotx.DoInTransaction, options ...gotx.Option) error {
	if hasTransaction(ctx) {
		return fn(ctx)
	}
	return t.requiresNew(ctx, fn, options...)
}

var rollbackOnly = errors.New("rollback only transaction")

func (t *Transactor) requiresNew(ctx context.Context, fn gotx.DoInTransaction, options ...gotx.Option) error {

	spannerConfig := newConfig()
	for _, opt := range options {
		opt.Apply(&spannerConfig)
	}

	spannerClient := t.provider.currentConnection(ctx)
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
		return fn(withCurrentTransaction(ctx, executor))
	}
	_, err := spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		executor := NewDefaultTxClient(spannerClient, txn, nil)
		err := fn(withCurrentTransaction(ctx, executor))
		if err != nil {
			return err
		}
		if spannerConfig.RollbackOnly {
			return rollbackOnly
		}
		return executor.Flush(ctx)
	})
	// rollback only transaction
	if err != nil && errors.Is(err, rollbackOnly) {
		return nil
	}
	return err
}
