package redis

import (
	"context"

	"github.com/go-redis/redis"
)

type ConnectionProvider interface {
	CurrentConnection(ctx context.Context) *redis.Client
}

type ClientProvider interface {
	CurrentClient(ctx context.Context) (reader redis.Cmdable, writer redis.Cmdable)
}

// get redis client from field
type DefaultConnectionProvider struct {
	client *redis.Client
}

func NewDefaultConnectionProvider(client *redis.Client) *DefaultConnectionProvider {
	return &DefaultConnectionProvider{
		client: client,
	}
}

func (p *DefaultConnectionProvider) CurrentConnection(_ context.Context) *redis.Client {
	return p.client
}
