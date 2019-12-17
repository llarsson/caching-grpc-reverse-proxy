package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/hashicorp/terraform/helper/hashcode"
	pb "github.com/llarsson/caching-grpc-reverse-proxy/hipstershop"
	"github.com/patrickmn/go-cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	port                   = flag.Int("port", 1234, "Port to listen to")
	productCatalogUpstream = flag.String("productCatalogUpstream", "productcatalogservice:3550", "Host:port address of the Product Catalog Service")
	servicePrefixes        map[string]string
	responseCache          *cache.Cache
)

type productCatalogProxy struct {
	client pb.ProductCatalogServiceClient
}

func (p *productCatalogProxy) ListProducts(ctx context.Context, req *pb.Empty) (*pb.ListProductsResponse, error) {
	hash := hashcode.Strings([]string{"ListProducts", req.String()})

	if value, found := responseCache.Get(hash); found {
		cachedResponse := value.(*pb.ListProductsResponse)
		grpc.SendHeader(ctx, metadata.Pairs("x-cache", "hit"))
		log.Printf("Using cached response for call to ListProducts(%s)", req)
		return cachedResponse, nil
	} else {
		var header metadata.MD
		response, err := p.client.ListProducts(ctx, req, grpc.Header(&header))
		if err != nil {
			log.Printf("Failed to call upstream ListProducts")
			return nil, err
		}

		expiration, err := cacheExpiration(header.Get("cache-control"))
		if expiration > 0 {
			responseCache.Set(hash, response, time.Duration(expiration)*time.Second)
			log.Printf("Storing response for %d seconds", expiration)
		}

		grpc.SendHeader(ctx, metadata.Pairs("x-cache", "miss"))
		log.Printf("Fetched upstream response for call to ListProducts(%s)", req)
		return response, nil
	}
}

func cacheExpiration(cacheHeaders []string) (int, error) {
	for _, header := range cacheHeaders {
		for _, value := range strings.Split(header, ",") {
			value = strings.Trim(value, " ")
			if strings.HasPrefix(value, "max-age") {
				duration := strings.Split(value, "max-age=")[1]
				return strconv.Atoi(duration)
			}
		}
	}
	return -1, errors.New("No cache expiration set for the given object")
}

func (p *productCatalogProxy) GetProduct(ctx context.Context, req *pb.GetProductRequest) (*pb.Product, error) {
	hash := hashcode.Strings([]string{"GetProduct", req.String()})

	if value, found := responseCache.Get(hash); found {
		cachedResponse := value.(*pb.Product)
		grpc.SendHeader(ctx, metadata.Pairs("x-cache", "hit"))
		log.Printf("Using cached response for call to GetProduct(%s)", req)
		return cachedResponse, nil
	} else {
		var header metadata.MD
		response, err := p.client.GetProduct(ctx, req, grpc.Header(&header))
		if err != nil {
			log.Printf("Failed to call upstream GetProduct")
			return nil, err
		}

		expiration, err := cacheExpiration(header.Get("cache-control"))
		if expiration > 0 {
			responseCache.Set(hash, response, time.Duration(expiration)*time.Second)
			log.Printf("Storing response for %d seconds", expiration)
		}

		grpc.SendHeader(ctx, metadata.Pairs("x-cache", "miss"))
		log.Printf("Fetched upstream response for call to GetProduct(%s)", req)
		return response, nil
	}
}

func (p *productCatalogProxy) SearchProducts(ctx context.Context, req *pb.SearchProductsRequest) (*pb.SearchProductsResponse, error) {
	hash := hashcode.Strings([]string{"SearchProducts", req.String()})

	if value, found := responseCache.Get(hash); found {
		cachedResponse := value.(*pb.SearchProductsResponse)
		grpc.SendHeader(ctx, metadata.Pairs("x-cache", "hit"))
		log.Printf("Using cached response for call to SearchProducts(%s)", req)
		return cachedResponse, nil
	} else {
		var header metadata.MD
		response, err := p.client.SearchProducts(ctx, req, grpc.Header(&header))
		if err != nil {
			log.Printf("Failed to call upstream SearchProducts")
			return nil, err
		}

		expiration, err := cacheExpiration(header.Get("cache-control"))
		if expiration > 0 {
			responseCache.Set(hash, response, time.Duration(expiration)*time.Second)
			log.Printf("Storing response for %d seconds", expiration)
		}

		grpc.SendHeader(ctx, metadata.Pairs("x-cache", "miss"))
		log.Printf("Fetched upstream response for call to SearchProducts(%s)", req)
		return response, nil
	}
}

func main() {
	var err error
	*productCatalogUpstream = os.Getenv("PRODUCT_CATALOG_SERVICE_ADDR")
	*port, err = strconv.Atoi(os.Getenv("PROXY_LISTEN_PORT"))
	if err != nil {
		log.Fatalf("PROXY_LISTEN_PORT cannot be parsed as integer")
	}

	flag.Parse()

	servicePrefixes = make(map[string]string)
	servicePrefixes["/hipstershop.ProductCatalogService/"] = *productCatalogUpstream

	responseCache = cache.New(10*time.Second, 60*time.Second)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()

	productCatalogUpstreamConn, err := grpc.Dial(*productCatalogUpstream, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Cannot connect to upstream %s : %v", *productCatalogUpstream, err)
	}
	defer productCatalogUpstreamConn.Close()
	pb.RegisterProductCatalogServiceServer(grpcServer, &productCatalogProxy{client: pb.NewProductCatalogServiceClient(productCatalogUpstreamConn)})

	grpcServer.Serve(lis)
}
