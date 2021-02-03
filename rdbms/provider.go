package rdbms

import (
	"context"
)

type ConnectionProvider interface {
	CurrentConnection(ctx context.Context) Conn
}

type ClientProvider interface {
	CurrentClient(ctx context.Context) Client
}

// get db connection from field
type DefaultConnectionProvider struct {
	db Conn
}

func NewDefaultConnectionProvider(db Conn) *DefaultConnectionProvider {
	return &DefaultConnectionProvider{
		db: db,
	}
}

func (p *DefaultConnectionProvider) CurrentConnection(_ context.Context) Conn {
	return p.db
}

// get db connection from context
type contextCurrentConnectionKey string

type ContextualConnectionProvider struct {
	key contextCurrentConnectionKey
}

func NewContextualConnectionProvider() *ContextualConnectionProvider {
	return &ContextualConnectionProvider{
		key: "current_rdb_connection",
	}
}

func NewContextualConnectionProviderWithConfig(key contextCurrentConnectionKey) *ContextualConnectionProvider {
	return &ContextualConnectionProvider{
		key: key,
	}
}

func (p *ContextualConnectionProvider) CurrentConnection(ctx context.Context) Conn {
	return ctx.Value(p.key).(Conn)
}
