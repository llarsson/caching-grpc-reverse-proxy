module github.com/llarsson/caching-grpc-reverse-proxy

go 1.13

require (
	github.com/golang/protobuf v1.3.2
	github.com/hashicorp/terraform v0.12.19 // indirect
	github.com/llarsson/grpc-caching-interceptors v0.0.0-20200110085858-0fdf7f498b7e
	github.com/patrickmn/go-cache v2.1.0+incompatible
	go.opencensus.io v0.22.2
	google.golang.org/grpc v1.26.0
)
