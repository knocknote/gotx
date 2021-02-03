package rdbms

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/knocknote/gotx"
)

type contextTransactionKey string

type DefaultClientProvider struct {
	shardKeyProvider   ShardKeyProvider
	connectionProvider ConnectionProvider
}

var defaultShardKeyProvider = func(ctx context.Context) string {
	return "common"
}

func contextKey(shardKey string) contextTransactionKey {
	return contextTransactionKey(fmt.Sprintf("current_%s_tx", shardKey))
}

func NewDefaultClientProvider(connectionProvider ConnectionProvider) *DefaultClientProvider {
	return NewDefaultShardClientProvider(connectionProvider, defaultShardKeyProvider)
}

func NewDefaultShardClientProvider(connectionProvider ConnectionProvider, shardKeyProvider ShardKeyProvider) *DefaultClientProvider {
	return &DefaultClientProvider{
		shardKeyProvider:   shardKeyProvider,
		connectionProvider: connectionProvider,
	}
}

func (p *DefaultClientProvider) CurrentClient(ctx context.Context) Client {
	key := contextKey(p.shardKeyProvider(ctx))
	transaction := ctx.Value(key)
	if transaction == nil {
		return p.connectionProvider.CurrentConnection(ctx)
	}
	return transaction.(Client)
}

type Transactor struct {
	shardKeyProvider   ShardKeyProvider
	connectionProvider ConnectionProvider
}

func NewTransactor(connectionProvider ConnectionProvider) *Transactor {
	return NewShardTransactor(connectionProvider, defaultShardKeyProvider)
}

func NewShardTransactor(connectionProvider ConnectionProvider, shardKeyProvider ShardKeyProvider) *Transactor {
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
