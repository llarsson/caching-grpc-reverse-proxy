package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	pb "github.com/llarsson/caching-grpc-reverse-proxy/hipstershop"
	"github.com/patrickmn/go-cache"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"google.golang.org/grpc"
)

const (
	productCatalogServiceAddrKey = "PRODUCT_CATALOG_SERVICE_ADDR"
	currencyServiceAddrKey       = "CURRENCY_SERVICE_ADDR"
	cartServiceAddrKey           = "CART_SERVICE_ADDR"
	recommendationServiceAddrKey = "RECOMMENDATION_SERVICE_ADDR"
	shippingServiceAddrKey       = "SHIPPING_SERVICE_ADDR"
	checkoutServiceAddrKey       = "CHECKOUT_SERVICE_ADDR"
	adServiceAddrKey             = "AD_SERVICE_ADDR"
)

func main() {
	port, err := strconv.Atoi(os.Getenv("PROXY_LISTEN_PORT"))
	if err != nil {
		log.Fatalf("PROXY_LISTEN_PORT cannot be parsed as integer")
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	if err := view.Register(ocgrpc.DefaultServerViews...); err != nil {
		log.Fatalf("Failed to register ocgrpc server views: %v", err)
	}

	if err := view.Register(ocgrpc.DefaultClientViews...); err != nil {
		log.Fatalf("Failed to register ocgrpc client views: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.StatsHandler(&ocgrpc.ServerHandler{}))

	serviceAddrKeys := []string{productCatalogServiceAddrKey, currencyServiceAddrKey,
		cartServiceAddrKey, recommendationServiceAddrKey, shippingServiceAddrKey,
		checkoutServiceAddrKey, adServiceAddrKey}

	for _, serviceAddrKey := range serviceAddrKeys {
		upstreamAddr, ok := os.LookupEnv(serviceAddrKey)
		if !ok {
			continue
		}

		conn, err := grpc.Dial(upstreamAddr, grpc.WithInsecure(), grpc.WithStatsHandler(new(ocgrpc.ClientHandler)))
		if err != nil {
			log.Fatalf("Cannot connect to upstream %s : %v", serviceAddrKey, err)
		}
		defer conn.Close()

		switch serviceAddrKey {
		case productCatalogServiceAddrKey:
			{
				proxy := pb.ProductCatalogServiceCachingProxy{Client: pb.NewProductCatalogServiceClient(conn), Cache: *cache.New(10*time.Second, 60*time.Second)}
				pb.RegisterProductCatalogServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Product Catalog Service calls to %s", upstreamAddr)
			}
		case currencyServiceAddrKey:
			{
				proxy := pb.CurrencyServiceCachingProxy{Client: pb.NewCurrencyServiceClient(conn), Cache: *cache.New(10*time.Second, 60*time.Second)}
				pb.RegisterCurrencyServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Currency Service calls to %s", upstreamAddr)
			}
		case cartServiceAddrKey:
			{
				proxy := pb.CartServiceCachingProxy{Client: pb.NewCartServiceClient(conn), Cache: *cache.New(10*time.Second, 60*time.Second)}
				pb.RegisterCartServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Cart Service calls to %s", upstreamAddr)
			}
		case recommendationServiceAddrKey:
			{
				proxy := pb.RecommendationServiceCachingProxy{Client: pb.NewRecommendationServiceClient(conn), Cache: *cache.New(10*time.Second, 60*time.Second)}
				pb.RegisterRecommendationServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Recommendation Service calls to %s", upstreamAddr)
			}
		case shippingServiceAddrKey:
			{
				proxy := pb.RecommendationServiceCachingProxy{Client: pb.NewRecommendationServiceClient(conn), Cache: *cache.New(10*time.Second, 60*time.Second)}
				pb.RegisterRecommendationServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Shipping Service calls to %s", upstreamAddr)
			}
		case checkoutServiceAddrKey:
			{
				proxy := pb.CheckoutServiceCachingProxy{Client: pb.NewCheckoutServiceClient(conn), Cache: *cache.New(10*time.Second, 60*time.Second)}
				pb.RegisterCheckoutServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Checkout Service calls to %s", upstreamAddr)
			}
		case adServiceAddrKey:
			{
				proxy := pb.AdServiceCachingProxy{Client: pb.NewAdServiceClient(conn), Cache: *cache.New(10*time.Second, 60*time.Second)}
				pb.RegisterAdServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Ad Service calls to %s", upstreamAddr)
			}
		default:
			{
				log.Fatalf("This should never happen")
			}
		}

	}

	grpcServer.Serve(lis)
}
