package redis

import (
	"context"

	"github.com/knocknote/gotx"

	"github.com/go-redis/redis"
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

func (p *DefaultTransactionProvider) CurrentTransaction(ctx context.Context) (reader redis.Cmdable, writer redis.Cmdable) {
	client := p.connectionProvider.CurrentConnection(ctx)
	transaction := ctx.Value(currentTransactionKey)
	if transaction == nil {
		return client, client
	}
	return client, transaction.(redis.Cmdable)
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

func (t *Transactor) RequiresNew(ctx context.Context, fn gotx.DoInTransaction, options ...gotx.Option) error {
	config := gotx.NewDefaultConfig()
	for _, opt := range options {
		opt.Apply(&config)
	}
	//TODO support optimistic locking if needed.
	redisClient := t.provider.CurrentConnection(ctx)
	_, err := redisClient.TxPipelined(func(pipe redis.Pipeliner) error {
		err := fn(context.WithValue(ctx, currentTransactionKey, pipe))
		if err != nil || config.RollbackOnly {
			_ = pipe.Discard()
		}
		return err
	})
	return err
}
