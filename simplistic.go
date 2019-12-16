package main

import (
	"fmt"
	"flag"
	"log"
	"context"
	"net"
	"google.golang.org/grpc"
	pb "github.com/llarsson/caching-grpc-reverse-proxy/hipstershop"
	"google.golang.org/grpc/metadata"
	"github.com/patrickmn/go-cache"
	"time"
	"github.com/hashicorp/terraform/helper/hashcode"
	"strings"
	"strconv"
	"errors"
)

var (
	port = flag.Int("port", 1234, "Port to listen to")
	productCatalogUpstream = flag.String("productCatalogUpstream", "productcatalogservice:3550", "Host:port address of the Product Catalog Service")
	servicePrefixes map[string]string
	responseCache *cache.Cache
)

type productCatalogProxy struct {
	fallback pb.UnimplementedProductCatalogServiceServer
	client pb.ProductCatalogServiceClient
}

func (p *productCatalogProxy) ListProducts(ctx context.Context, req *pb.Empty) (*pb.ListProductsResponse, error) {
	// keeping this around for future reference, if it would ever be needed
	//md, _ := metadata.FromIncomingContext(ctx)

	//	outMd := md.Copy()
	//
	//	outCtx, _ := context.WithCancel(ctx)
	//	outCtx = metadata.NewOutgoingContext(outCtx, outMd)

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
			responseCache.Set(hash, response, time.Duration(expiration) * time.Second)
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
	response, err := p.client.GetProduct(context.Background(), req)
	if err != nil {
		log.Printf("Failed to call upstream GetProduct")
		return nil, err
	}
	return response, err
}

func (p *productCatalogProxy) SearchProducts(ctx context.Context, req *pb.SearchProductsRequest) (*pb.SearchProductsResponse, error) {
	response, err := p.client.SearchProducts(context.Background(), req)
	if err != nil {
		log.Printf("Failed to call upstream SearchProduct")
		return nil, err
	}
	return response, err
}

func main() {
	flag.Parse()

	servicePrefixes = make(map[string]string)
	servicePrefixes["/hipstershop.ProductCatalogService/"] = *productCatalogUpstream

	responseCache = cache.New(10 * time.Second, 60 * time.Second)

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
