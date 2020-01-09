package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/terraform/helper/hashcode"
	pb "github.com/llarsson/caching-grpc-reverse-proxy/hipstershop"
	"github.com/llarsson/protobuf/proto"
	"github.com/patrickmn/go-cache"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
)

// A CachingInterceptor will intercept
type CachingInterceptor interface {
	CreateUnaryServerInterceptor() grpc.UnaryServerInterceptor
	CreateUnaryClientInterceptor() grpc.UnaryClientInterceptor
}

// InmemoryCachingInterceptor is the
type InmemoryCachingInterceptor struct {
	Cache cache.Cache
}

// CreateUnaryServerInterceptor creates the actual grpc.UnaryClientInterceptor
func (interceptor *InmemoryCachingInterceptor) CreateUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		reqMessage := req.(proto.Message)
		hash := hashcode.Strings([]string{info.FullMethod, reqMessage.String()})

		if value, found := interceptor.Cache.Get(hash); found {
			grpc.SendHeader(ctx, metadata.Pairs("x-cache", "hit"))
			log.Printf("Using cached response for call to %s(%s)", info.FullMethod, req)
			return value, nil
		}

		resp, err := handler(ctx, req)
		if err != nil {
			log.Printf("Failed to call upstream %s", info.FullMethod)
			return nil, err
		}

		return resp, nil
	}
}

// CreateUnaryClientInterceptor creates the actual grpc.UnaryClientInterceptor
func (interceptor *InmemoryCachingInterceptor) CreateUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		reqMessage := req.(proto.Message)
		hash := hashcode.Strings([]string{method, reqMessage.String()})

		var header metadata.MD
		opts = append(opts, grpc.Header(&header))
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			log.Printf("Error calling upstream: %v", err)
			return err
		}

		expiration, err := cacheExpiration(header.Get("cache-control"))
		if expiration > 0 {
			interceptor.Cache.Set(hash, reply, time.Duration(expiration)*time.Second)
			log.Printf("Storing response for %d seconds", expiration)
		}

		grpc.SendHeader(ctx, metadata.Pairs("x-cache", "miss"))
		log.Printf("Fetched upstream response for call to %s(%s)", method, req)
		return nil
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
	return -1, status.Errorf(codes.Internal, "No cache expiration set for the given object")
}

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

	cachingInterceptor := InmemoryCachingInterceptor{Cache: *cache.New(10*time.Second, 60*time.Second)}

	grpcServer := grpc.NewServer(grpc.StatsHandler(&ocgrpc.ServerHandler{}), grpc.UnaryInterceptor(cachingInterceptor.CreateUnaryServerInterceptor()))

	serviceAddrKeys := []string{productCatalogServiceAddrKey, currencyServiceAddrKey,
		cartServiceAddrKey, recommendationServiceAddrKey, shippingServiceAddrKey,
		checkoutServiceAddrKey, adServiceAddrKey, paymentSerivceAddrKey, emailServiceAddrKey}

	for _, serviceAddrKey := range serviceAddrKeys {
		upstreamAddr, ok := os.LookupEnv(serviceAddrKey)
		if !ok {
			continue
		}

		conn, err := grpc.Dial(upstreamAddr, grpc.WithUnaryInterceptor(cachingInterceptor.CreateUnaryClientInterceptor()), grpc.WithInsecure(), grpc.WithStatsHandler(new(ocgrpc.ClientHandler)))
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
