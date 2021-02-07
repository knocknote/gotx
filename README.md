# gotx

Go transaction library inspired by Spring Framework that brings you can handle transactions without being aware of the difference in data sources such as spanner, redis or rdbmds

![CI](https://github.com/knocknote/gotx/workflows/CI/badge.svg?branch=develop)

## Motivation

In the architecture of web applications, differences in data sources are absorbed by layers such as Repository and Dao.
However, transactions straddle Repository and Dao.  
I created this library from the desire to do simple coding by providing a `Transactor` that absorbs the difference in transaction behavior between data sources such as redis and spanner.

## Features
* Transaction Abstraction Layer  
  <img src="./.img/feature1.png" width="360px"/>
  
* Database Sharding  
  <img src="./.img/feature2.png" width="360px"/>
  
* Write-Through  
  <img src="./.img/feature3.png" width="360px"/>

## Installation

```sh
go get github.com/knocknote/gotx@develop 
```

The standard library only supports RDBMS.  
Install additional libraries depending on the data source you want to use.

| DataSource | Command |
|--------|--------|
| Google Cloud Spanner | go get github.com/knocknote/gotx/spanner@develop |
| Redis | go get github.com/knocknote/gotx/redis@develop |

## API

* gotx has three core interfaces `Transactor`, `ConnectionProvider`, `ClientProvider`.
* You can create any transaction behavior by implementing these interfaces.

### Transactor 
* It provides various methods shown in [Transaction propagation of Spring Framework](https://docs.spring.io/spring-framework/docs/current/javadoc-api/org/springframework/transaction/annotation/Propagation.html).
* Currently, only the following methods are supported.

| Method | Description |
|--------|----------|
| Required| Support a current transaction, create a new one if none exists. |
| RequiresNew | Create a new transaction, and suspend the current transaction if one exists. |

* Each method has the following options.

| Option | Description |
|--------|----------|
| ReadOnly | This option makes it a read-only transaction. |
| RollbackOnly | This option ensures that the transaction rolls back even if it succeeds. Mainly used in test classes. |

### ConnectionProvider
* A strategy to get raw connections such as `*spanner.Client` and `*sql.DB`.
* You can create any ConnectionProvider by implementing the following method.

| Method | Description |
|--------|----------|
| CurrentConnection | returns raw connections like spanner.Client or sql.DB |

### ClientProvider
* Provides client classes for executing queries of rdbms or spanner .
* You can create any ClientProvider by implementing the following method.

| Method | Description |
|--------|----------|
| CurrentClient | returns api for executing query.|

## Usage

Here is the sample UseCase class.
* Case 1: use read-write transaction
* Case 2: use readonly transaction
* Case 3: no transaction(just call repository method)

```go
type MyUseCase struct {
  transactor gotx.Transactor
  repository Reppsitory
}

func (u *MyUseCase) Do(ctx context.Context) error {

  // Case 1
  tx1Err := u.transactor.Required(ctx, func(ctx context.Context) error {
    // do in transaction
    res, err := u.repository.FindByID(ctx, "A")
    if err != nil {
      return err
    }
    model.value += 100
    return u.repository.Update(ctx, model)
  })

  // Case 2
  var res model.Model
  tx2Err := u.transactor.Required(ctx, func(ctx context.Context) error {
    // do in readonly transaction 
    var err error
    res, err = u.repository.FindByID(ctx, "A")
    return err
  }, gotx.OptionReadOnly())

  // Case 3
  // no transaction     
  model, err := u.repository.FindByID(ctx, "A")
}
```

### RDBMS

Here is the sample with using PostgreSQL for datasource.

```go
import (
  "context"
  "database/sql"

  gotx "github.com/knocknote/gotx/rdbms"

  _ "github.com/lib/pq"
)

func DependencyInjection() {
  connection, _ := sql.Open("postgres", "postgres://postgres:password@localhost/testdb?sslmode=disable")
  connectionProvider := gotx.NewDefaultConnectionProvider(connection)
  clientProvider := gotx.NewDefaultClientProvider(connectionProvider)
  transactor := gotx.NewTransactor(connectionProvider)

  repository := NewRDBRepository(clientProvider)
  // MyUseCase code is independent on RDBMS transaction behavior.
  usecase := NewMyUseCase(transactor, repository)
}

type RDBRepository struct {
  clientProvider gotx.ClientProvider
}

// Repository is unaware of transactions
func (r *RDBRepository) FindByID(ctx context.Context, userID string) (*model.Model, error) {
  //Case 1 client is `sql.Tx`
  //Case 2 client is `sql.Tx(readonly)` 
  //Case 3 client is `sql.DB or interface provided by gotx.ConnectionProvider `
  client := r.clientProvider.CurrentClient(ctx)

  // use ORM like sqlboiler
  return model.FindModel(ctx, client, userID)
}
```

### Google Cloud Spanner

* Here is the sample with using Google Cloud Spanner for datasource.
* Cloud Spanner does not support nested transactions. So don't use `transactor.RequiresNew`.

```go
import (
  "context"

  "cloud.google.com/go/spanner"
  gotx "github.com/knocknote/gotx/spanner"

  _ "github.com/lib/pq"
)

func DependencyInjection() {
  connection, _ := spanner.NewClient(context.Background(), "projects/local-project/instances/test-instance/databases/test-database")
  connectionProvider := gotx.NewDefaultConnectionProvider(connection)
  clientProvider := gotx.NewDefaultClientProvider(connectionProvider)
  transactor := gotx.NewTransactor(connectionProvider)
  
  repository := NewSpannerRepository(clientProvider)
  // MyUseCase code is independent on Spanner transaction behavior.
  useCase := NewMyUseCase(transactor, repository)
}

type SpannerRepository struct {
  clientProvider gotx.ClientProvider
}

// Repository is unaware of transactions
func (r *SpannerRepository) FindByID(ctx context.Context, userID string) (*model.Model, error)  {
  //Case 1 reader is `spanner.ReadWriteTransaction` 
  //Case 2 reader is `spanner.ReadOnlyTransaction` 
  //Case 3 reader is `spanner.ReadOnlyTransaction` (spanner.Client.Single())
  reader := r.clientProvider.CurrentClient(ctx).Reader(ctx)

  // use ORM like https://github.com/cloudspannerecosystem/yo
  return model.FindModel(ctx, reader, userID)
}

func (r *SpannerRepository) Update(ctx context.Context, target *model.Model) error  {
  client := r.clientProvider.CurrentClient(ctx)
  //Case 1 use `spanner.Client.BufferWrite(mutations)`
  //Case 2 returns error
  //Case 3 use `spanner.Client.Apply(ctx,mutations)`
  return clieny.ApplyOrBufferWrite(ctx,target.Update(ctx))
}
```

### Redis

Here is the sample with using Redis for datasource.

```go
import (
  "context"
  
  "github.com/go-redis/redis"

  gotx "github.com/knocknote/gotx/redis"
)

func DependencyInjection() {
  connection := redis.NewClient(&redis.Options{
    Addr:     "localhost:6379",
    Password: "",
    DB:       0,
   })
  connectionProvider := gotx.NewDefaultConnectionProvider(connection)
  clientProvider := gotx.NewDefaultClientProvider(connectionProvider)
  transactor := gotx.NewTransactor(connectionProvider)

  repository := NewRedisRepository(clientProvider)
  // MyUseCase code is independent on Redis transaction behavior.
  useCase := NewMyUseCase(transactor, repository)
}

type RedisRepository struct {
  clientProvider gotx.ClientProvider
}

// Repository is unaware of transactions
func (r *RedisRepository) FindByID(ctx context.Context, userID string) (*model.Model, error)  {
  //Case 1 reader is `redis.Client` and writer is `redis.Pipeliner` 
  //Case 2 reader is `redis.Client` and writer is `redis.Pipeliner` read only option is unsupported 
  //Case 3 reader and writer is `redis.Client`
  reader, writer := r.clientProvider.CurrentClient(ctx)

  // calling `writer.Get` returns empty result in `redis.TxPipelined` so use reader to use get item.
  val, err := reader.Get(userID).Result()
  var model &Model
  //TODO unmarshal val
  return model, err
}
```

### Database Sharding
* Select specified connection from []*sql.DB by the sharding key.
* Use `ShardingConnectionProvider` to get the sql.DB determined by the hash slot.

```go
import (
  "context"
  "database/sql"

  gotx "github.com/knocknote/gotx/rdbms"
)

type shardKey string

var shardKeyUser shardKey = "userID"

func DependencyInjection() {
  var userCons []*sql.DB // create sql.DB for each sharded database
  userShardKeyProvider := func(ctx context.Context) string {
    return ctx.Value(shardKeyUser).(string)
  }
  // use hash-slot
  userConnectionProvider := gotx.NewShardingConnectionProvider(userCons, 127, userShardKeyProvider)
  userTransactor := gotx.NewShardingTransactor(userConnectionProvider, userShardKeyProvider)
  userClientProvider := gotx.NewShardingDefaultClientProvider(userConnectionProvider, userShardKeyProvider)

  repository := NewSpannerRepository(userClientProvider)
  usecase := NewMyUseCase(userTransactor, repository)
}

func (u *UseCase) Do(ctx context.Context) error{
  userID := "test"
  return u.transactor.Required(context.WithValue(ctx, shardKeyUser, userID), func(ctx context.Context){
    // in target shard transaction scope 
    return u.repostiory.Update(ctx, model)
  })
}
```

#### Multiple Database Sharding
* Select specified connection from multiple []*sql.DB by the sharding key.
* Use `CompositeTransactor` to handle multiple transactions transparently with UseCase.
* This is not a distributed transaction like XA.

```go
import (
  "context"
  "database/sql"

  "github.com/knocknote/gotx"
  gotxrdbms "github.com/knocknote/gotx/rdbms"
)

type shardKey string

var shardKeyUser shardKey = "userID"
var shardKeyGuild shardKey = "guildID"

func DependencyInjection() {
  // user shard connections
  var userCons []*sql.DB // create sql.DB for each sharded database
  userShardKeyProvider := func(ctx context.Context) string {
    return ctx.Value(shardKeyUser).(string)
  }
  userConnectionProvider := gotxrdbms.NewShardingConnectionProvider(userCons, 127, userShardKeyProvider)
  userTransactor := gotxrdbms.NewShardingTransactor(userConnectionProvider, userShardKeyProvider)
  userClientProvider := gotxrdbms.NewShardingDefaultClientProvider(userConnectionProvider, userShardKeyProvider)

  // guild shard connections
  var guildCons []*sql.DB // create sql.DB for each sharded database
  guildShardKeyProvider := func(ctx context.Context) string {
    return ctx.Value(shardKeyGuild).(string)
  }
  guildConnectionProvider := gotxrdbms.NewShardingConnectionProvider(guildCons, 127, guildShardKeyProvider)
  guildTransactor := gotxrdbms.NewShardingTransactor(guildConnectionProvider, guildShardKeyProvider)
  guildClientProvider := gotxrdbms.NewShardingDefaultClientProvider(guildConnectionProvider, guildShardKeyProvider)

  // composite transaction
  userRepository := NewUserRepository(userClientProvider)
  guildRepository := NewGuildRepository(guildClientProvider)
  transactor := gotxrdbms.NewCompositeTransactor(userTransactor, guildTransactor)
  usecase := NewMyUseCase(transactor, userRepository,guildRepository)
}

func (u *UseCase) Do(ctx context.Context) error {
	
  childCtx := context.WithValue(context.WithValue(ctx, shardKeyUser, "userID"), shardKeyGuild, "guildID")
  
  return u.transactor.Required(childCtx, func(ctx context.Context) error {
    // in target shard transaction scope. ( one guild Tx and one user Tx begins. )
    if err := u.guildRepostiory.Update(ctx, guildModel); err != nil {
      return err
    }
    return u.userRepostiory.Update(ctx, userModel)
  }
} 
```

### Force rollback during test

You can always roll back the test DB only for unit tests without changing the production code.

```go
func Test_Success(t *testing.T) {

  // construct test target 
  transactor := gotx.NewTransactor(...)
  useCase := NewUserCase(transactor,repository)
 
  // execute test in rollback-only transaction
  err := transactor.Required(ctx, func(ctx context.Context) error { 
    model, err := useCase.Do(ctx)
    if err != nil {
    	return err
    }
    // assert updated data in database or return value
  }, gotx.OptionRollbackOnly()) // <- rollback only option
  
  if err != nil {
    t.Error(err)
  }
}

```