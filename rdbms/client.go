package rdbms

import (
	"context"
	"fmt"

	"database/sql"
)

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

type contextTransactionKey string

type DefaultClientProvider struct {
	shardKeyProvider   ShardKeyProvider
	connectionProvider ConnectionProvider
}

var defaultShardKeyProvider = func(ctx context.Context) string {
	return "rdbms"
}

func contextKey(shardKey string) contextTransactionKey {
	return contextTransactionKey(fmt.Sprintf("current_%s_tx", shardKey))
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

func (p *DefaultClientProvider) CurrentClient(ctx context.Context) Client {
	key := contextKey(p.shardKeyProvider(ctx))
	transaction := ctx.Value(key)
	if transaction == nil {
		return p.connectionProvider.CurrentConnection(ctx)
	}
	return transaction.(Client)
}
