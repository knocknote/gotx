package rdbms

import (
	"context"
	"database/sql"

	"github.com/knocknote/gotx"
)

type DefaultClientProvider struct {
	keyProvider        KeyProvider
	connectionProvider ConnectionProvider
}

var defaultKeyProvider = func(ctx context.Context) contextTransactionKey {
	return "current_rdb_transaction"
}

func NewDefaultClientProvider(connectionProvider ConnectionProvider) *DefaultClientProvider {
	return NewDefaultClientProviderWithConfig(connectionProvider, defaultKeyProvider)
}

func NewDefaultClientProviderWithConfig(connectionProvider ConnectionProvider, keyProvider KeyProvider) *DefaultClientProvider {
	return &DefaultClientProvider{
		keyProvider:        keyProvider,
		connectionProvider: connectionProvider,
	}
}

func (p *DefaultClientProvider) CurrentClient(ctx context.Context) Client {
	transaction := ctx.Value(p.keyProvider(ctx))
	if transaction == nil {
		return p.connectionProvider.CurrentConnection(ctx)
	}
	return transaction.(Client)
}

type contextTransactionKey string

type KeyProvider func(ctx context.Context) contextTransactionKey

type Transactor struct {
	keyProvider        KeyProvider
	connectionProvider ConnectionProvider
}

func NewTransactor(connectionProvider ConnectionProvider) *Transactor {
	return NewTransactorWithConfig(connectionProvider, defaultKeyProvider)
}

func NewTransactorWithConfig(connectionProvider ConnectionProvider, keyProvider KeyProvider) *Transactor {
	return &Transactor{
		keyProvider:        keyProvider,
		connectionProvider: connectionProvider,
	}
}

func (t *Transactor) Required(ctx context.Context, fn gotx.DoInTransaction, options ...gotx.Option) error {
	if ctx.Value(t.keyProvider(ctx)) != nil {
		return fn(ctx)
	}
	return t.RequiresNew(ctx, fn, options...)
}

func (t *Transactor) RequiresNew(ctx context.Context, fn gotx.DoInTransaction, options ...gotx.Option) (err error) {
	config := gotx.NewDefaultConfig()
	for _, opt := range options {
		opt.Apply(&config)
	}
	db := t.connectionProvider.CurrentConnection(ctx)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: config.ReadOnly,
	})
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		} else if err != nil {
			_ = tx.Rollback()
		} else {
			if config.RollbackOnly {
				_ = tx.Rollback()
			} else {
				err = tx.Commit()
			}
		}
	}()
	err = fn(context.WithValue(ctx, t.keyProvider(ctx), tx))
	return
}
