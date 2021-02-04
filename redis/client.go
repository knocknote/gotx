package redis

import (
	"context"
	"fmt"

	"github.com/go-redis/redis"
)

type ClientProvider interface {
	CurrentClient(ctx context.Context) (reader redis.Cmdable, writer redis.Cmdable)
}

var defaultShardKeyProvider = func(ctx context.Context) string {
	return "redis"
}

type ShardKeyProvider func(ctx context.Context) string

type contextTransactionKey string

func contextKey(shardKey string) contextTransactionKey {
	return contextTransactionKey(fmt.Sprintf("current_%s_tx", shardKey))
}

type DefaultClientProvider struct {
	shardKeyProvider   ShardKeyProvider
	connectionProvider ConnectionProvider
}

func NewDefaultClientProvider(connectionProvider ConnectionProvider) *DefaultClientProvider {
	return NewShardingDefaultClientProvider(connectionProvider, defaultShardKeyProvider)
}

func NewShardingDefaultClientProvider(connectionProvider ConnectionProvider, shardKeyProvider ShardKeyProvider) *DefaultClientProvider {
	return &DefaultClientProvider{
		shardKeyProvider:   shardKeyProvider,
		connectionProvider: connectionProvider,
	}
}

func (p *DefaultClientProvider) CurrentClient(ctx context.Context) (reader redis.Cmdable, writer redis.Cmdable) {
	client := p.connectionProvider.CurrentConnection(ctx)
	transaction := ctx.Value(contextKey(p.shardKeyProvider(ctx)))
	if transaction == nil {
		return client, client
	}
	return client, transaction.(redis.Cmdable)
}
