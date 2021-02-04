package rdbms

import (
	"context"
	"database/sql"

	"github.com/knocknote/gotx"
)

type Transactor struct {
	shardKeyProvider   ShardKeyProvider
	connectionProvider ConnectionProvider
}

func NewTransactor(connectionProvider ConnectionProvider) *Transactor {
	return NewShardingTransactor(connectionProvider, defaultShardKeyProvider)
}

func NewShardingTransactor(connectionProvider ConnectionProvider, shardKeyProvider ShardKeyProvider) *Transactor {
	return &Transactor{
		shardKeyProvider:   shardKeyProvider,
		connectionProvider: connectionProvider,
	}
}

func (t *Transactor) Required(ctx context.Context, fn gotx.DoInTransaction, options ...gotx.Option) error {
	if ctx.Value(contextKey(t.shardKeyProvider(ctx))) != nil {
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
	err = fn(context.WithValue(ctx, contextKey(t.shardKeyProvider(ctx)), tx))
	return
}
