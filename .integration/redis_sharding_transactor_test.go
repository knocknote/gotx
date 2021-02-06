package _integration

import (
	"context"
	"errors"
	"testing"

	"github.com/knocknote/gotx"

	gotxredis "github.com/knocknote/gotx/redis"

	"github.com/go-redis/redis"
)

const testKey = "test_key1"
const testValue = "test_value"

func newShardingRedisConnection() (gotxredis.ConnectionProvider, []*redis.Client) {
	client1 := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	client2 := redis.NewClient(&redis.Options{
		Addr:     "localhost:6380",
		Password: "",
		DB:       0,
	})
	_ = client1.Del(testKey)
	_ = client2.Del(testKey)
	pools := []*redis.Client{client1, client2}
	connectionProvider := gotxredis.NewShardingConnectionProvider(pools, 16383, userShardKeyProvider)
	return connectionProvider, pools
}

func TestRedisShardingCommit(t *testing.T) {

	connectionProvider, pools := newShardingRedisConnection()
	ctx := context.WithValue(context.Background(), shardKeyUser, "user2")
	transactor := gotxredis.NewShardingTransactor(connectionProvider, userShardKeyProvider)
	clientProvider := gotxredis.NewShardingDefaultClientProvider(connectionProvider, userShardKeyProvider)
	err := transactor.Required(ctx, func(ctx context.Context) error {
		_, writer := clientProvider.CurrentClient(ctx)
		return writer.Set(testKey, testValue, -1).Err()
	})
	if err != nil {
		t.Error(err)
		return
	}
	expectedRedisResult(t, false, true, pools)
}

func TestRedisShardingRollbackOnError(t *testing.T) {

	connectionProvider, pools := newShardingRedisConnection()
	ctx := context.WithValue(context.Background(), shardKeyUser, "user2")
	transactor := gotxredis.NewShardingTransactor(connectionProvider, userShardKeyProvider)
	clientProvider := gotxredis.NewShardingDefaultClientProvider(connectionProvider, userShardKeyProvider)
	err := transactor.Required(ctx, func(ctx context.Context) error {
		_, writer := clientProvider.CurrentClient(ctx)
		_ = writer.Set(testKey, testValue, -1).Err()
		return errors.New("error")
	})
	if err == nil {
		t.Error(err)
		return
	}
	expectedRedisResult(t, false, false, pools)
}

func TestRedisShardingRollbackOnOption(t *testing.T) {

	connectionProvider, pools := newShardingRedisConnection()
	ctx := context.WithValue(context.Background(), shardKeyUser, "user2")
	transactor := gotxredis.NewShardingTransactor(connectionProvider, userShardKeyProvider)
	clientProvider := gotxredis.NewShardingDefaultClientProvider(connectionProvider, userShardKeyProvider)
	err := transactor.Required(ctx, func(ctx context.Context) error {
		_, writer := clientProvider.CurrentClient(ctx)
		return writer.Set(testKey, testValue, -1).Err()
	}, gotx.OptionRollbackOnly())
	if err != nil {
		t.Error(err)
		return
	}
	expectedRedisResult(t, false, false, pools)
}

func expectedRedisResult(t *testing.T, user1Exists bool, user2Exists bool, userCons []*redis.Client) {
	value, err := userCons[0].Get(testKey).Result()
	if user1Exists {
		if err != nil {
			t.Error(err)
			return
		}
		if value != testValue {
			t.Error("value must be test_value")
			return
		}
	} else {
		if err == nil {
			t.Error(err)
			return
		}
	}

	value, err = userCons[1].Get(testKey).Result()
	if user2Exists {
		if err != nil {
			t.Error(err)
		}
		if value != testValue {
			t.Error("value must be test_value")
		}
	} else {
		if err == nil {
			t.Error(err)
		}
	}

}
