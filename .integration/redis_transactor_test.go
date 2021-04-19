package _integration

import (
	"context"
	"errors"
	"testing"

	"github.com/knocknote/gotx"
	gotxredis "github.com/knocknote/gotx/redis"

	"github.com/go-redis/redis"
)

func newTransactor() (gotx.Transactor, gotxredis.ClientProvider) {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	connectionProvider := gotxredis.NewDefaultConnectionProvider(client)
	clientProvider := gotxredis.NewDefaultClientProvider(connectionProvider)
	transactor := gotxredis.NewTransactor(connectionProvider)
	return transactor, clientProvider
}

func TestRedisCommit(t *testing.T) {

	ctx := context.Background()
	transactor, clientProvider := newTransactor()
	key := "test_key1"
	value := "test_value"
	err := transactor.Required(ctx, func(ctx context.Context) error {
		_, writer := clientProvider.CurrentClient(ctx)
		return writer.Set(key, value, -1).Err()
	})
	if err != nil {
		t.Error(err)
		return
	}
	reader, _ := clientProvider.CurrentClient(ctx)
	result, err := reader.Get(key).Result()
	if err != nil {
		t.Error(err)
		return
	}
	if result != value {
		t.Errorf("expected=%s, but actual=%s", value, result)
		return
	}
}

func TestReadInTransaction(t *testing.T) {

	ctx := context.Background()
	transactor, clientProvider := newTransactor()
	key := "test_key2"
	value := "test_value"
	_, w := clientProvider.CurrentClient(ctx)
	_ = w.Set(key, "aaa", -1).Err()
	err := transactor.Required(ctx, func(ctx context.Context) error {
		// reader can read value in transaction
		reader, writer := clientProvider.CurrentClient(ctx)
		res, err := reader.Get(key).Result()
		if err != nil {
			return err
		}
		if res != "aaa" {
			return errors.New("value must be aaa")
		}
		//expect that writer cannot get value in transaction because of pipeline.
		res, err = writer.Get(key).Result()
		if err != nil {
			return err
		}
		if res != "" {
			return errors.New("value must be ''")
		}
		return writer.Set(key, value, -1).Err()
	})
	if err != nil {
		t.Error(err)
		return
	}
	reader, _ := clientProvider.CurrentClient(ctx)
	result, err := reader.Get(key).Result()
	if err != nil {
		t.Error(err)
		return
	}
	if result != value {
		t.Errorf("expected=%s, but actual=%s", value, result)
		return
	}
}

func TestRedisRollbackOnError(t *testing.T) {

	ctx := context.Background()
	transactor, clientProvider := newTransactor()
	key := "test_key3"
	value := "test_value"
	err := transactor.Required(ctx, func(ctx context.Context) error {
		_, writer := clientProvider.CurrentClient(ctx)
		_ = writer.Set(key, value, -1).Err()
		return errors.New("error")
	})
	if err == nil {
		t.Error("should be error")
		return
	}
	reader, _ := clientProvider.CurrentClient(ctx)
	result, _ := reader.Get(key).Result()
	if result == value {
		t.Errorf("rollback expected")
		return
	}
}

func TestRedisRollbackOption(t *testing.T) {

	ctx := context.Background()
	transactor, clientProvider := newTransactor()
	key := "test_key4"
	value := "test_value"
	err := transactor.Required(ctx, func(ctx context.Context) error {
		_, writer := clientProvider.CurrentClient(ctx)
		return writer.Set(key, value, -1).Err()
	}, gotx.OptionRollbackOnly())

	// no error expected
	if err != nil {
		t.Error(err)
		return
	}
	reader, _ := clientProvider.CurrentClient(ctx)
	result, _ := reader.Get(key).Result()
	if result == value {
		t.Errorf("rollback expected")
		return
	}
}
