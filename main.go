package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	pb "github.com/llarsson/caching-grpc-reverse-proxy/hipstershop"
	interceptors "github.com/llarsson/grpc-caching-interceptors/client"
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
	paymentSerivceAddrKey        = "PAYMENT_SERVICE_ADDR"
	emailServiceAddrKey          = "EMAIL_SERVICE_ADDR"
	csvFileName     = "data.csv"
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

	cachingInterceptor := interceptors.InmemoryCachingInterceptor{Cache: *cache.New(10*time.Second, 60*time.Second)}

	csvFile, err := os.Create(csvFileName)
	if err != nil {
		log.Fatalf("Could not open CSV file (%s) for writing", csvFileName)
	}
	defer csvFile.Close()

	grpcServer := grpc.NewServer(grpc.StatsHandler(&ocgrpc.ServerHandler{}), grpc.UnaryInterceptor(cachingInterceptor.UnaryServerInterceptor(log.New(csvFile, "", 0))))

	serviceAddrKeys := []string{productCatalogServiceAddrKey, currencyServiceAddrKey,
		cartServiceAddrKey, recommendationServiceAddrKey, shippingServiceAddrKey,
		checkoutServiceAddrKey, adServiceAddrKey, paymentSerivceAddrKey, emailServiceAddrKey}

	for _, serviceAddrKey := range serviceAddrKeys {
		upstreamAddr, ok := os.LookupEnv(serviceAddrKey)
		if !ok {
			continue
		}

		conn, err := grpc.Dial(upstreamAddr, grpc.WithUnaryInterceptor(cachingInterceptor.UnaryClientInterceptor()), grpc.WithInsecure(), grpc.WithStatsHandler(new(ocgrpc.ClientHandler)))
		if err != nil {
			log.Fatalf("Cannot connect to upstream %s : %v", serviceAddrKey, err)
		}
		defer conn.Close()

		switch serviceAddrKey {
		case productCatalogServiceAddrKey:
			{
				proxy := pb.ProductCatalogServiceProxy{Client: pb.NewProductCatalogServiceClient(conn)}
				pb.RegisterProductCatalogServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Product Catalog Service calls to %s", upstreamAddr)
			}
		case currencyServiceAddrKey:
			{
				proxy := pb.CurrencyServiceProxy{Client: pb.NewCurrencyServiceClient(conn)}
				pb.RegisterCurrencyServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Currency Service calls to %s", upstreamAddr)
			}
		case cartServiceAddrKey:
			{
				proxy := pb.CartServiceProxy{Client: pb.NewCartServiceClient(conn)}
				pb.RegisterCartServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Cart Service calls to %s", upstreamAddr)
			}
		case recommendationServiceAddrKey:
			{
				proxy := pb.RecommendationServiceProxy{Client: pb.NewRecommendationServiceClient(conn)}
				pb.RegisterRecommendationServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Recommendation Service calls to %s", upstreamAddr)
			}
		case shippingServiceAddrKey:
			{
				proxy := pb.ShippingServiceProxy{Client: pb.NewShippingServiceClient(conn)}
				pb.RegisterShippingServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Shipping Service calls to %s", upstreamAddr)
			}
		case checkoutServiceAddrKey:
			{
				proxy := pb.CheckoutServiceProxy{Client: pb.NewCheckoutServiceClient(conn)}
				pb.RegisterCheckoutServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Checkout Service calls to %s", upstreamAddr)
			}
		case adServiceAddrKey:
			{
				proxy := pb.AdServiceProxy{Client: pb.NewAdServiceClient(conn)}
				pb.RegisterAdServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Ad Service calls to %s", upstreamAddr)
			}
		case paymentSerivceAddrKey:
			{
				proxy := pb.PaymentServiceProxy{Client: pb.NewPaymentServiceClient(conn)}
				pb.RegisterPaymentServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Payment Service calls to %s", upstreamAddr)
			}
		case emailServiceAddrKey:
			{
				proxy := pb.EmailServiceProxy{Client: pb.NewEmailServiceClient(conn)}
				pb.RegisterEmailServiceServer(grpcServer, &proxy)
				log.Printf("Proxying Email Service calls to %s", upstreamAddr)
			}
		default:
			{
				log.Fatalf("This should never happen")
			}
		}

	}

	grpcServer.Serve(lis)
}
