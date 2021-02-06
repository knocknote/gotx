package redis

import (
	"context"

	"github.com/knocknote/gotx"

	"github.com/go-redis/redis"
)

type ConnectionProvider interface {
	CurrentConnection(ctx context.Context) *redis.Client
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

// get db by hash slot
type ShardingConnectionProvider struct {
	db               []*redis.Client
	hashSlot         []uint32
	shardKeyProvider ShardKeyProvider
	maxSlot          uint32
}

func NewShardingConnectionProvider(db []*redis.Client, maxSlot uint32, shardKeyProvider ShardKeyProvider) *ShardingConnectionProvider {
	return &ShardingConnectionProvider{
		db:               db,
		shardKeyProvider: shardKeyProvider,
		hashSlot:         gotx.GetHashSlotRange(len(db), maxSlot),
		maxSlot:          maxSlot,
	}
}

func (p *ShardingConnectionProvider) CurrentConnection(ctx context.Context) *redis.Client {
	shardKey := p.shardKeyProvider(ctx)
	index := gotx.GetIndexByHash(p.hashSlot, []byte(shardKey), p.maxSlot)
	return p.db[index]
}
