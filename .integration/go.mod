module github.com/knocknote/gotx/.integration

go 1.15

require (
	cloud.google.com/go/spanner v1.17.0
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/knocknote/gotx v0.0.0
	github.com/lib/pq v1.10.0
	google.golang.org/genproto v0.0.0-20210416161957-9910b6c460de
	google.golang.org/grpc v1.37.0
)

replace github.com/knocknote/gotx => ../
