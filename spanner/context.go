package spanner

import (
	"golang.org/x/net/context"
)

type contextCurrentTransactionKey string

const currentTransactionKey contextCurrentTransactionKey = "current_spanner_transaction"

func withCurrentTransaction(parent context.Context, conn TxClient) context.Context {
	return context.WithValue(parent, currentTransactionKey, conn)
}

func hasTransaction(ctx context.Context) bool {
	return ctx.Value(currentTransactionKey) != nil
}
