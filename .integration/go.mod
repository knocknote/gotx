module github.com/knocknote/gotx/.integration

go 1.15

require (
	cloud.google.com/go/spanner v1.17.0
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/knocknote/gotx v0.0.0
	github.com/lib/pq v1.9.0
	github.com/onsi/ginkgo v1.14.2 // indirect
	github.com/onsi/gomega v1.10.4 // indirect
	google.golang.org/genproto v0.0.0-20210331142528-b7513248f0ba
	google.golang.org/grpc v1.36.1
)

replace github.com/knocknote/gotx => ../
