package redis

import (
	"context"

	"github.com/knocknote/gotx"

	"github.com/go-redis/redis"
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

func (t *Transactor) RequiresNew(ctx context.Context, fn gotx.DoInTransaction, options ...gotx.Option) error {
	config := gotx.NewDefaultConfig()
	for _, opt := range options {
		opt.Apply(&config)
	}
	//TODO support optimistic locking if needed.
	redisClient := t.connectionProvider.CurrentConnection(ctx)
	_, err := redisClient.TxPipelined(func(pipe redis.Pipeliner) error {
		err := fn(context.WithValue(ctx, contextKey(t.shardKeyProvider(ctx)), pipe))
		if err != nil || config.RollbackOnly {
			_ = pipe.Discard()
		}
		return err
	})
	return err
}
