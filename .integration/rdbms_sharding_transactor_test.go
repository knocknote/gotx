package _integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/knocknote/gotx"

	rdbms "github.com/knocknote/gotx/rdbms"

	_ "github.com/lib/pq"
)

type shardKey string

var shardKeyUser shardKey = "userID"
var shardKeyGuild shardKey = "guildID"

var userShardKeyProvider = func(ctx context.Context) string {
	return ctx.Value(shardKeyUser).(string)
}

var guildShardKeyProvider = func(ctx context.Context) string {
	return ctx.Value(shardKeyGuild).(string)
}

func newShardingConnection() (rdbms.ConnectionProvider, rdbms.ConnectionProvider, []*sql.DB, []*sql.DB) {
	userConnection1, _ := sql.Open("postgres", "postgres://postgres:password@localhost:5432/testdb?sslmode=disable")
	userConnection2, _ := sql.Open("postgres", "postgres://postgres:password@localhost:5433/testdb?sslmode=disable")
	guildConnection1, _ := sql.Open("postgres", "postgres://postgres:password@localhost:5432/testdb?sslmode=disable")
	guildConnection2, _ := sql.Open("postgres", "postgres://postgres:password@localhost:5433/testdb?sslmode=disable")

	userCons := []*sql.DB{userConnection1, userConnection2}
	guildCons := []*sql.DB{guildConnection1, guildConnection2}

	return rdbms.NewShardingConnectionProvider(userCons, 16383, userShardKeyProvider), rdbms.NewShardingConnectionProvider(guildCons, 16383, guildShardKeyProvider), userCons, guildCons
}

func createShardingTable(_ context.Context, cons []*sql.DB, name string) error {
	for _, connection := range cons {
		_, _ = connection.Exec(fmt.Sprintf("drop table %s", name))
		_, err := connection.Exec(fmt.Sprintf("create table %s ( id varchar(10))", name))
		if err != nil {
			return err
		}
	}
	return nil
}

func TestShardingCommit(t *testing.T) {

	ctx := context.WithValue(context.WithValue(context.Background(), shardKeyUser, "user1"), shardKeyGuild, "guild1")
	users, guilds, userCons, guildCons := newShardingConnection()
	userTransactor := rdbms.NewShardingTransactor(users, userShardKeyProvider)
	guildTransactor := rdbms.NewShardingTransactor(guilds, guildShardKeyProvider)
	userClientProvider := rdbms.NewShardingDefaultClientProvider(users, userShardKeyProvider)
	guildClientProvider := rdbms.NewShardingDefaultClientProvider(guilds, guildShardKeyProvider)
	if err := createShardingTable(ctx, userCons, "user1"); err != nil {
		t.Error(err)
		return
	}
	if err := createShardingTable(ctx, guildCons, "guild1"); err != nil {
		t.Error(err)
		return
	}
	transactor := gotx.NewCompositeTransactor(userTransactor, guildTransactor)

	err := transactor.Required(ctx, func(ctx context.Context) error {
		userClient := userClientProvider.CurrentClient(ctx)
		guildClient := guildClientProvider.CurrentClient(ctx)
		if _, err := userClient.Exec("INSERT into user1 values('user1')"); err != nil {
			return err
		}
		if _, err := guildClient.Exec("INSERT into guild1 values('guild1')"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Error(err)
		return
	}
	expected(t, false, true, false, true, userCons, guildCons)
}

func TestShardingRollbackOnError(t *testing.T) {

	ctx := context.WithValue(context.WithValue(context.Background(), shardKeyUser, "user1"), shardKeyGuild, "guild1")
	users, guilds, userCons, guildCons := newShardingConnection()
	userTransactor := rdbms.NewShardingTransactor(users, userShardKeyProvider)
	guildTransactor := rdbms.NewShardingTransactor(guilds, guildShardKeyProvider)
	userClientProvider := rdbms.NewShardingDefaultClientProvider(users, userShardKeyProvider)
	guildClientProvider := rdbms.NewShardingDefaultClientProvider(guilds, guildShardKeyProvider)
	if err := createShardingTable(ctx, userCons, "user1"); err != nil {
		t.Error(err)
		return
	}
	if err := createShardingTable(ctx, guildCons, "guild1"); err != nil {
		t.Error(err)
		return
	}
	transactor := gotx.NewCompositeTransactor(userTransactor, guildTransactor)

	err := transactor.Required(ctx, func(ctx context.Context) error {
		userClient := userClientProvider.CurrentClient(ctx)
		guildClient := guildClientProvider.CurrentClient(ctx)
		if _, err := userClient.Exec("INSERT into user1 values('user1')"); err != nil {
			return err
		}
		if _, err := guildClient.Exec("INSERT into guild1 values('guild1')"); err != nil {
			return err
		}
		return errors.New("error")
	})
	if err == nil {
		t.Error("must error")
		return
	}
	expected(t, false, false, false, false, userCons, guildCons)
}

func TestShardingRollbackOption(t *testing.T) {

	ctx := context.WithValue(context.WithValue(context.Background(), shardKeyUser, "user1"), shardKeyGuild, "guild1")
	users, guilds, userCons, guildCons := newShardingConnection()
	userTransactor := rdbms.NewShardingTransactor(users, userShardKeyProvider)
	guildTransactor := rdbms.NewShardingTransactor(guilds, guildShardKeyProvider)
	userClientProvider := rdbms.NewShardingDefaultClientProvider(users, userShardKeyProvider)
	guildClientProvider := rdbms.NewShardingDefaultClientProvider(guilds, guildShardKeyProvider)
	if err := createShardingTable(ctx, userCons, "user1"); err != nil {
		t.Error(err)
		return
	}
	if err := createShardingTable(ctx, guildCons, "guild1"); err != nil {
		t.Error(err)
		return
	}
	transactor := gotx.NewCompositeTransactor(userTransactor, guildTransactor)

	err := transactor.Required(ctx, func(ctx context.Context) error {
		userClient := userClientProvider.CurrentClient(ctx)
		guildClient := guildClientProvider.CurrentClient(ctx)
		if _, err := userClient.Exec("INSERT into user1 values('user1')"); err != nil {
			return err
		}
		if _, err := guildClient.Exec("INSERT into guild1 values('guild1')"); err != nil {
			return err
		}
		return nil
	}, gotx.OptionRollbackOnly())
	if err != nil {
		t.Error(err)
		return
	}
	expected(t, false, false, false, false, userCons, guildCons)
}

func TestShardingReadOnlyOption(t *testing.T) {

	ctx := context.WithValue(context.WithValue(context.Background(), shardKeyUser, "user1"), shardKeyGuild, "guild1")
	users, guilds, userCons, guildCons := newShardingConnection()
	userTransactor := rdbms.NewShardingTransactor(users, userShardKeyProvider)
	guildTransactor := rdbms.NewShardingTransactor(guilds, guildShardKeyProvider)
	userClientProvider := rdbms.NewShardingDefaultClientProvider(users, userShardKeyProvider)
	guildClientProvider := rdbms.NewShardingDefaultClientProvider(guilds, guildShardKeyProvider)
	if err := createShardingTable(ctx, userCons, "user1"); err != nil {
		t.Error(err)
		return
	}
	if err := createShardingTable(ctx, guildCons, "guild1"); err != nil {
		t.Error(err)
		return
	}
	transactor := gotx.NewCompositeTransactor(userTransactor, guildTransactor)

	err := transactor.Required(ctx, func(ctx context.Context) error {
		userClient := userClientProvider.CurrentClient(ctx)
		guildClient := guildClientProvider.CurrentClient(ctx)
		if _, err := userClient.Exec("INSERT into user1 values('user1')"); err != nil {
			return err
		}
		if _, err := guildClient.Exec("INSERT into guild1 values('guild1')"); err != nil {
			return err
		}
		return nil
	}, gotx.OptionReadOnly())

	if err == nil {
		t.Error("must error")
		return
	}
	expected(t, false, false, false, false, userCons, guildCons)
}

func expected(t *testing.T, user1Exists bool, user2Exists bool, guild1Exists bool, guild2Exists bool, userCons []*sql.DB, guildCons []*sql.DB) {
	rows1, err := userCons[0].Query("SELECT * FROM user1")
	if err != nil {
		t.Error(err)
		return
	}
	rows2, err := userCons[1].Query("SELECT * FROM user1")
	if err != nil {
		t.Error(err)
		return
	}
	rows3, err := guildCons[0].Query("SELECT * FROM guild1")
	if err != nil {
		t.Error(err)
		return
	}
	rows4, err := guildCons[1].Query("SELECT * FROM guild1")
	if err != nil {
		t.Error(err)
		return
	}
	if user1Exists != rows1.Next() {
		t.Errorf("row must exists")
		return
	}
	if user2Exists != rows2.Next() {
		t.Errorf("row must exists")
		return
	}
	if guild1Exists != rows3.Next() {
		t.Errorf("row must exists")
		return
	}
	if guild2Exists != rows4.Next() {
		t.Errorf("row must exists")
		return
	}
}
