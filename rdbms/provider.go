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

const currentConnectionKey contextCurrentConnectionKey = "current_rdb_connection"

func WithCurrentConnection(ctx context.Context, con Conn) context.Context {
	return context.WithValue(ctx, currentConnectionKey, con)
}

type ContextualConnectionProvider struct {
}

func NewContextualConnectionProvider() *ContextualConnectionProvider {
	return &ContextualConnectionProvider{}
}

func (p *ContextualConnectionProvider) CurrentConnection(ctx context.Context) Conn {
	return ctx.Value(currentConnectionKey).(Conn)
}
