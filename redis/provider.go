package redis

import (
	"context"

	"github.com/go-redis/redis"
)

type ConnectionProvider interface {
	CurrentConnection(ctx context.Context) *redis.Client
}

type TransactionProvider interface {
	CurrentTransaction(ctx context.Context) (reader redis.Cmdable, writer redis.Cmdable)
}

// get redis client from field
type DefaultConnectionProvider struct {
	conn *redis.Client
}

func (p *DefaultConnectionProvider) CurrentConnection(_ context.Context) *redis.Client {
	return p.conn
}
