package gotx

import (
	"context"
	"fmt"

	"github.com/knocknote/gotx"

	"github.com/go-redis/redis"
)

var defaultShardKeyProvider = func(ctx context.Context) string {
	return "redis"
}

type ShardKeyProvider func(ctx context.Context) string

type contextTransactionKey string

func contextKey(shardKey string) contextTransactionKey {
	return contextTransactionKey(fmt.Sprintf("current_%s_tx", shardKey))
}

// --------------------------------
// Connection
// --------------------------------
type ConnectionProvider interface {
	CurrentConnection(ctx context.Context) *redis.Client
}

// get redis client from field
type DefaultConnectionProvider struct {
	client *redis.Client
}

func NewDefaultConnectionProvider(client *redis.Client) ConnectionProvider {
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

func NewShardingConnectionProvider(db []*redis.Client, maxSlot uint32, shardKeyProvider ShardKeyProvider) ConnectionProvider {
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

// ------------------------------------
// Client
// ------------------------------------
type ClientProvider interface {
	CurrentClient(ctx context.Context) (reader redis.Cmdable, writer redis.Cmdable)
}

type DefaultClientProvider struct {
	shardKeyProvider   ShardKeyProvider
	connectionProvider ConnectionProvider
}

func NewDefaultClientProvider(connectionProvider ConnectionProvider) ClientProvider {
	return NewShardingDefaultClientProvider(connectionProvider, defaultShardKeyProvider)
}

func NewShardingDefaultClientProvider(connectionProvider ConnectionProvider, shardKeyProvider ShardKeyProvider) ClientProvider {
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

// ------------------------------------
// Transactor
// ------------------------------------
type Transactor struct {
	shardKeyProvider   ShardKeyProvider
	connectionProvider ConnectionProvider
}

func NewTransactor(connectionProvider ConnectionProvider) gotx.Transactor {
	return NewShardingTransactor(connectionProvider, defaultShardKeyProvider)
}

func NewShardingTransactor(connectionProvider ConnectionProvider, shardKeyProvider ShardKeyProvider) gotx.Transactor {
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
