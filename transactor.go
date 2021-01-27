package gotx

import (
	"context"
)

type DoInTransaction func(ctx context.Context) error

type Transactor interface {
	// Support a current transaction, create a new one if none exists.
	Required(ctx context.Context, fn DoInTransaction, options ...Option) error
	// Create a new transaction, and suspend the current transaction if one exists.
	RequiresNew(ctx context.Context, fn DoInTransaction, options ...Option) (err error)
}

type CompositeTransactor struct {
	transactors []Transactor
}

func NewCompositeTransactor(transactors ...Transactor) *CompositeTransactor {
	return &CompositeTransactor{
		transactors: transactors,
	}
}

func (t *CompositeTransactor) Required(ctx context.Context, fn DoInTransaction, options ...Option) error {
	composed := fn
	for _, transactor := range t.transactors {
		composed = t.composeRequired(transactor, composed, options...)
	}
	return composed(ctx)
}

func (t *CompositeTransactor) composeRequired(a Transactor, composed DoInTransaction, options ...Option) DoInTransaction {
	return func(ctx context.Context) error {
		return a.Required(ctx, func(ctx context.Context) error {
			return composed(ctx)
		}, options...)
	}
}

func (t *CompositeTransactor) RequiresNew(ctx context.Context, fn DoInTransaction, options ...Option) error {
	composed := fn
	for _, transactor := range t.transactors {
		composed = t.composeRequiresNew(transactor, composed, options...)
	}
	return composed(ctx)
}

func (t *CompositeTransactor) composeRequiresNew(a Transactor, composed DoInTransaction, options ...Option) DoInTransaction {
	return func(ctx context.Context) error {
		return a.RequiresNew(ctx, func(ctx context.Context) error {
			return composed(ctx)
		}, options...)
	}
}
