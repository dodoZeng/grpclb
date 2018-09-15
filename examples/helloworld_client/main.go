package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	kbalancer "github.com/dodoZeng/grpclb/balancer/ketama"
	pb "github.com/dodoZeng/grpclb/examples/helloworld"
	_ "github.com/dodoZeng/grpclb/resolver/consul"
)

const (
	consulAddress = "127.0.0.1:8500"
	service       = "helloworld.Greeter"
	defaultName   = "world"
)

func main() {
	// Set up a connection to the server.
	conn, err := grpc.Dial(
		fmt.Sprintf("consul:///%s/%s", consulAddress, service),
		//grpc.WithBlock(),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(
			grpc_retry.UnaryClientInterceptor(
				// 重试次数
				grpc_retry.WithMax(3),
				// 重试间隔
				grpc_retry.WithBackoff(grpc_retry.BackoffLinear(time.Duration(100)*time.Millisecond)),
				// 重试时间
				grpc_retry.WithPerRetryTimeout(time.Duration(200)*time.Millisecond),
				// 重试的返回值
				grpc_retry.WithCodes(codes.ResourceExhausted, codes.Unavailable, codes.DeadlineExceeded),
			),
		),
		grpc.WithBalancerName(kbalancer.Name),
		//grpc.WithBalancer(grpc.RoundRobin(grpclb.NewResolver(
		//	consulAddress, service,
		//))),
	)
	//conn, err := grpc.Dial(consulAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return
	}
	defer conn.Close()

	c := pb.NewGreeterClient(conn)

	// Contact the server and print out its response.
	name := defaultName
	if len(os.Args) > 1 {
		name = os.Args[1]
	}

	for i := 0; i < 10; i++ {

		go func(i int) {
			//ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1000)*time.Millisecond)
			//defer cancel()

			ts := time.Now().UnixNano()

			ctx := context.Background()
			keyValue := name
			r, err := c.SayHello(context.WithValue(ctx, kbalancer.DefaultKetamaKeyName, keyValue),
				&pb.HelloRequest{Name: name},
				grpc_retry.WithMax(3),
				grpc_retry.WithPerRetryTimeout(time.Duration(300)*time.Millisecond),
				grpc_retry.WithBackoff(grpc_retry.BackoffLinear(time.Duration(100)*time.Millisecond)),
				grpc_retry.WithCodes(codes.DeadlineExceeded, codes.Unavailable),
			)

			if err != nil {
				log.Printf("Greeting(%d): %v", i, err)
			} else {
				log.Printf("Greeting(%d): %s", i, r.Message)
			}

			log.Printf("Span(%d): %d ms", i, (time.Now().UnixNano()-ts)/int64(time.Millisecond))
		}(i)

		time.Sleep(time.Second * 1)
	}
}
