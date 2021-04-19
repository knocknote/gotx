package gotx

import (
	"context"
	"fmt"

	"github.com/go-redis/redis"
)

type contextRedisTransactionKey string

func redisTransactionKey(shardKey string) contextRedisTransactionKey {
	return contextRedisTransactionKey(fmt.Sprintf("current_%s_tx", shardKey))
}

type redisShardKeyProvider func(ctx context.Context) string

var defaultRedisShardKeyProvider = func(ctx context.Context) string {
	return "redis"
}

// --------------------------------
// Connection
// --------------------------------
type RedisConnectionProvider interface {
	CurrentConnection(ctx context.Context) *redis.Client
}

// get redis client from field
type DefaultRedisConnectionProvider struct {
	client *redis.Client
}

func NewDefaultRedisConnectionProvider(client *redis.Client) RedisConnectionProvider {
	return &DefaultRedisConnectionProvider{
		client: client,
	}
}

func (p *DefaultRedisConnectionProvider) CurrentConnection(_ context.Context) *redis.Client {
	return p.client
}

// get db by hash slot
type ShardingRedisConnectionProvider struct {
	db               []*redis.Client
	hashSlot         []uint32
	shardKeyProvider redisShardKeyProvider
	maxSlot          uint32
}

func NewShardingRedisConnectionProvider(db []*redis.Client, maxSlot uint32, shardKeyProvider redisShardKeyProvider) *ShardingRedisConnectionProvider {
	return &ShardingRedisConnectionProvider{
		db:               db,
		shardKeyProvider: shardKeyProvider,
		hashSlot:         GetHashSlotRange(len(db), maxSlot),
		maxSlot:          maxSlot,
	}
}

func (p *ShardingRedisConnectionProvider) CurrentConnection(ctx context.Context) *redis.Client {
	shardKey := p.shardKeyProvider(ctx)
	index := GetIndexByHash(p.hashSlot, []byte(shardKey), p.maxSlot)
	return p.db[index]
}

// --------------------------------
// Client
// ---------------------------------
type RedisClientProvider interface {
	CurrentClient(ctx context.Context) (reader redis.Cmdable, writer redis.Cmdable)
}

type DefaultRedisClientProvider struct {
	shardKeyProvider   redisShardKeyProvider
	connectionProvider RedisConnectionProvider
}

func NewDefaultRedisClientProvider(connectionProvider RedisConnectionProvider) RedisClientProvider {
	return NewShardingRedisClientProvider(connectionProvider, defaultRedisShardKeyProvider)
}

func NewShardingRedisClientProvider(connectionProvider RedisConnectionProvider, shardKeyProvider redisShardKeyProvider) RedisClientProvider {
	return &DefaultRedisClientProvider{
		shardKeyProvider:   shardKeyProvider,
		connectionProvider: connectionProvider,
	}
}

func (p *DefaultRedisClientProvider) CurrentClient(ctx context.Context) (reader redis.Cmdable, writer redis.Cmdable) {
	client := p.connectionProvider.CurrentConnection(ctx)
	transaction := ctx.Value(redisTransactionKey(p.shardKeyProvider(ctx)))
	if transaction == nil {
		return client, client
	}
	return client, transaction.(redis.Cmdable)
}

// -----------------------------------
// Transactor
// -----------------------------------
type RedisTransactor struct {
	shardKeyProvider   redisShardKeyProvider
	connectionProvider RedisConnectionProvider
}

func NewRedisTransactor(connectionProvider RedisConnectionProvider) Transactor {
	return NewShardingRedisTransactor(connectionProvider, defaultRedisShardKeyProvider)
}

func NewShardingRedisTransactor(connectionProvider RedisConnectionProvider, shardKeyProvider redisShardKeyProvider) Transactor {
	return &RedisTransactor{
		shardKeyProvider:   shardKeyProvider,
		connectionProvider: connectionProvider,
	}
}

func (t *RedisTransactor) Required(ctx context.Context, fn DoInTransaction, options ...Option) error {
	if ctx.Value(redisTransactionKey(t.shardKeyProvider(ctx))) != nil {
		return fn(ctx)
	}
	return t.RequiresNew(ctx, fn, options...)
}

func (t *RedisTransactor) RequiresNew(ctx context.Context, fn DoInTransaction, options ...Option) error {
	config := NewDefaultConfig()
	for _, opt := range options {
		opt.Apply(&config)
	}
	//TODO support optimistic locking if needed.
	redisClient := t.connectionProvider.CurrentConnection(ctx)
	_, err := redisClient.TxPipelined(func(pipe redis.Pipeliner) error {
		err := fn(context.WithValue(ctx, redisTransactionKey(t.shardKeyProvider(ctx)), pipe))
		if err != nil || config.RollbackOnly {
			_ = pipe.Discard()
		}
		return err
	})
	return err
}
