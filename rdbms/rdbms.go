package gotx

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/knocknote/gotx"
)

type contextTransactionKey string

var defaultShardKeyProvider = func(ctx context.Context) string {
	return "rdbms"
}

func contextKey(shardKey string) contextTransactionKey {
	return contextTransactionKey(fmt.Sprintf("current_%s_tx", shardKey))
}

// --------------------------------
// Connection
// --------------------------------
type ConnectionProvider interface {
	CurrentConnection(ctx context.Context) Conn
}

// get db connection from field
type DefaultConnectionProvider struct {
	db Conn
}

func NewDefaultConnectionProvider(db Conn) ConnectionProvider {
	return &DefaultConnectionProvider{
		db: db,
	}
}

func (p *DefaultConnectionProvider) CurrentConnection(_ context.Context) Conn {
	return p.db
}

type ShardKeyProvider func(ctx context.Context) string

// get db by hash slot
type ShardingConnectionProvider struct {
	db               []*sql.DB
	hashSlot         []uint32
	shardKeyProvider ShardKeyProvider
	maxSlot          uint32
}

func NewShardingConnectionProvider(db []*sql.DB, maxSlot uint32, shardKeyProvider ShardKeyProvider) ConnectionProvider {
	return &ShardingConnectionProvider{
		db:               db,
		shardKeyProvider: shardKeyProvider,
		hashSlot:         gotx.GetHashSlotRange(len(db), maxSlot),
		maxSlot:          maxSlot,
	}
}

func (p *ShardingConnectionProvider) CurrentConnection(ctx context.Context) Conn {
	shardKey := p.shardKeyProvider(ctx)
	index := gotx.GetIndexByHash(p.hashSlot, []byte(shardKey), p.maxSlot)
	return p.db[index]
}

// ------------------------------------
// Client
// ------------------------------------

type Client interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type Conn interface {
	Client
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

type ClientProvider interface {
	CurrentClient(ctx context.Context) Client
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

func (p *DefaultClientProvider) CurrentClient(ctx context.Context) Client {
	key := contextKey(p.shardKeyProvider(ctx))
	transaction := ctx.Value(key)
	if transaction == nil {
		return p.connectionProvider.CurrentConnection(ctx)
	}
	return transaction.(Client)
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
