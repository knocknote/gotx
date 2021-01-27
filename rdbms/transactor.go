package rdbms

import (
	"context"
	"database/sql"

	"github.com/knocknote/gotx"
)

type contextCurrentTransactionKey string

const currentTransactionKey contextCurrentTransactionKey = "current_rdb_transaction"

type DefaultTransactionProvider struct {
	connectionProvider ConnectionProvider
}

func NewDefaultTransactionProvider(connectionProvider ConnectionProvider) *DefaultTransactionProvider {
	return &DefaultTransactionProvider{
		connectionProvider: connectionProvider,
	}
}

func (p *DefaultTransactionProvider) CurrentTransaction(ctx context.Context) Conn {
	transaction := ctx.Value(currentTransactionKey)
	if transaction == nil {
		return p.connectionProvider.CurrentConnection(ctx)
	}
	return transaction.(Conn)
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

func (t *Transactor) RequiresNew(ctx context.Context, fn gotx.DoInTransaction, options ...gotx.Option) (err error) {
	config := gotx.NewDefaultConfig()
	for _, opt := range options {
		opt.Apply(&config)
	}
	db := t.provider.CurrentConnection(ctx)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: config.RollbackOnly,
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
	err = fn(context.WithValue(ctx, currentTransactionKey, tx))
	return
}
