module github.com/knocknote/gotx/.integration

go 1.15

require (
	cloud.google.com/go/spanner v1.13.0
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/knocknote/gotx v0.0.0-20210131103456-c185c00dec73
	github.com/knocknote/gotx/redis v0.0.0-20210131104022-fe798c6c495a
	github.com/knocknote/gotx/spanner v0.0.0-20210131104022-fe798c6c495a
	github.com/lib/pq v1.9.0
	github.com/onsi/ginkgo v1.14.2 // indirect
	github.com/onsi/gomega v1.10.4 // indirect
	google.golang.org/genproto v0.0.0-20210113195801-ae06605f4595
	google.golang.org/grpc v1.34.1
)
