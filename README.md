# gotx

Go transaction library inspired by Spring Framework that brings you can handle transactions without being aware of the difference in data sources such as spanner, redis or rdbmds

![CI](https://github.com/knocknote/gotx/workflows/CI/badge.svg?branch=develop)

## Motivation

In the architecture of web applications, differences in data sources are absorbed by layers such as Repository and Dao.
However, transactions straddle Repository and Dao.  
I created this library from the desire to do simple coding by providing a `Transactor` that absorbs the difference in transaction behavior between data sources such as redis and spanner.

## Installation

This library is currently in alpha version.

```sh
go get github.com/knocknote/gotx@develop 
```

Install additional libraries depending on the data source you want to use.

### Google Cloud Spanner

```sh
go get github.com/knocknote/gotx/spanner@develop 
```

### Redis

```sh
go get github.com/knocknote/gotx/redis@develop 
```

## API

* It provides various methods shown in [Transaction propagation of Spring Framework](https://docs.spring.io/spring-framework/docs/current/javadoc-api/org/springframework/transaction/annotation/Propagation.html).
* Currently, only the following methods are supported.
* You can create any Transactor by implementing Transactor method.

| Method | Description |
|--------|----------|
| Required | Support a current transaction, create a new one if none exists. |
| RequiresNew | Create a new transaction, and suspend the current transaction if one exists. |

## Usage

Here is the sample usecase.

```go
type MyUseCase struct {
    transactor gotx.Transactor
    repository Reppsitory
}

func (u *MyUseCase) Do(ctx context.Context) error {
    
    // case 1
    tx1Err := u.transactor.Required(ctx, func(ctx context.Context) error {
        // do in spanner.ReadWriterTransaction
        res, err := u.repository.FindByID(ctx, "A") 
        if err != nil {
            return err 
        }   
        model.value += 100
        return u.repository.Update(ctx, model) 
    })
    
    // case 2
    var res model.Model 
    tx2Err := u.transactor.Required(ctx, func(ctx context.Context) error {
        // do in spanner.ReadOnlyTransaction
        var err error
        res, err = u.repository.FindByID(ctx, "A") 
        return err
    }, gotx.OptionReadOnly())
    
    // case 3
    // no transaction     
    model, err := u.repository.FindByID(ctx, "A") 
}
```

### RDBMS

```go
import (
    "database/sql"
    
    "github.com/knocknote/gotx"
    gotxrdbms "github.com/knocknote/gotx/rdbms"

    _ "github.com/lib/pq"
)

func DependencyInjection() {
    connection, err := sql.Open("postgres", "postgres://postgres:password@localhost/testdb?sslmode=disable")
    connectionProvider = gotxrdbms.NewDefaultConnectionProvider(connection)
    clientProvider := gotxrdbms.NewDefaultClientProvider(connectionProvider)
    repository := &RDBRepository{clientProvider}
    
    transactor := gotxrdbms.NewTransactor(connectionProvider)
    useCase := &MyUseCase{transactor, repository}
}

type RDBRepository struct {
    clientProvider gotxrdbms.ClientProvider
}

// Repository is unaware of transactions
func (r *RDBRepository) FindByID(ctx context.Context, userID string) (*model.Model, error) {
    //Case 1 client is `sql.Tx`
    //Case 2 client is `sql.Tx(readonly)` 
    //Case 3 client is `sql.DB or interface provided by gotxrdbms.ConnectionProvider `
    client := r.clientProvider.CurrentClient(ctx)
        
    // use ORM like sqlboiler
    return model.FindModel(ctx, client, userID)
}
```

### Google Cloud Spanner

```go
import (
    "cloud.google.com/go/spanner"
    
    "github.com/knocknote/gotx"
    gotxspanner "github.com/knocknote/gotx/spanner"
)

func DependencyInjection() {
    connection, _:= spanner.NewClient(context.Background(),"projects/local-project/instances/test-instance/databases/test-database")
    connectionProvider = gotxspanner.NewDefaultConnectionProvider(connection)
    clientProvider := gotxspanner.NewDefaultClientProvider(connectionProvider)
    repository := &SpannerRepository{clientProvider}
    
    transactor := gotxspanner.NewTransactor(connectionProvider)
    useCase := &MyUseCase{transactor, repository}
}

type SpannerRepository struct {
    clientProvider gotxspanner.ClientProvider
}

// Repository is unaware of transactions
func (r *SpannerRepository) FindByID(ctx context.Context, userID string) (*model.Model, error)  {
    //Case 1 reader is `spanner.ReadWriteTransaction` 
    //Case 2 reader is `spanner.ReadOnlyTransaction` 
    //Case 3 reader is `spanner.ReadOnlyTransaction` (spanner.Client.Single())
    reader := r.clientProvider.CurrentClient(ctx).Reader()
        
    // use ORM like https://github.com/cloudspannerecosystem/yo
    return model.FindModel(ctx, reader, userID)
}
```

### Redis

```go
import (
    "github.com/go-redis/redis"
    
    "github.com/knocknote/gotx"
    gotxredis "github.com/knocknote/gotx/redis"
)

func DependencyInjection() {
    connection := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "",
        DB:       0,
    })
    connectionProvider = gotxredis.NewDefaultConnectionProvider(connection)
    clientProvider := gotxredis.NewDefaultClientProvider(connectionProvider)
    repository := &RedisRepository{clientProvider}
    
    transactor := gotxredis.NewTransactor(connectionProvider)
    useCase := &MyUseCase{transactor, repository}
}

type RedisRepository struct {
    clientProvider gotxredis.ClientProvider
}

// Repository is unaware of transactions
func (r *RedisRepository) FindByID(ctx context.Context, userID string) (*model.Model, error)  {
    //Case 1 reader is `redis.Client` and writer is `redis.Pipeliner` 
    //Case 2 reader is `redis.Client` and writer is `redis.Pipeliner` read only option is unsupported 
    //Case 3 reader and writer is `redis.Client`
    reader, writer := r.clientProvider.CurrentClient(ctx).Reader()
    
    // calling `writer.Get` returns empty result in `redis.TxPipelined` so use reader to use get item.
    val, err := reader.Get(userID).Result()
    var model &Model
    //TODO unmarshal val
    return model, err
}
```

### Composite Transaction

* Handle multiple transactions transparently with UseCase.
* This is not a distributed transaction like XA.

```go
func DependencyInjection() {
   
    redisClient := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "",
        DB:       0,
    })
    redisConnectionProvider = gotxredis.NewDefaultConnectionProvider(redisClient)
    redisClientProvider := gotxredis.NewDefaultClientProvider(redisConnectionProvider)
    
    dbConnection, err := sql.Open("postgres", "postgres://postgres:password@localhost/testdb?sslmode=disable")
    dbConnectionProvider = gotxrdbms.NewDefaultConnectionProvider(dbConnection)
    dbClientProvider := gotxrdbms.NewDefaultClientProvider(dbConnectionProvider)
    
    dbTransactor := gotxrdbms.NewTransactor(dbConnectionProvider)    
    redisTransactor := gotxredis.NewTransactor(redisConnectionProvider)
       
    compositeTransactor := gotx.NewCompositeTransactor(redisTransactor, dbTransactor)
    useCase := &MyUseCase{transactor, ...}
}

func (u *UseCase) Do(ctx context.Context){
    // redis transaction is executed in database transaction.
    // database connection fails if redis transaction fails.
    // attention! After the redis transaction ends, if the DB commit fails, redis and DB will not match
    err := u.transactor.Required(ctx, ... 
```

