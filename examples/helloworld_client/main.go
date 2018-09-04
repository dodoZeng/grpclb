package main

import (
	"log"
	"os"
	"time"

	"github.com/dodoZeng/grpclb"
	"github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
)

const (
	consul_address = "127.0.0.1:8500"
	service        = "helloworld.Greeter"
	defaultName    = "world"
)

func main() {
	// Set up a connection to the server.
	conn, err := grpc.Dial(
		"",
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(
			grpc_retry.UnaryClientInterceptor(
				// 重试间隔时间
				grpc_retry.WithBackoff(grpc_retry.BackoffLinear(time.Duration(1)*time.Millisecond)),
				// 重试次数
				grpc_retry.WithMax(3),
				// 重试时间
				grpc_retry.WithPerRetryTimeout(time.Duration(5)*time.Millisecond),
				// 返回码为如下值时重试
				grpc_retry.WithCodes(codes.ResourceExhausted, codes.Unavailable, codes.DeadlineExceeded),
			),
		),
		grpc.WithBalancer(grpc.RoundRobin(grpclb.NewResolver(
			consul_address, service,
		))),
	)
	//conn, err := grpc.Dial(consul_address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return
	}
	defer conn.Close()

	// Contact the server and print out its response.
	c := pb.NewGreeterClient(conn)
	name := defaultName
	if len(os.Args) > 1 {
		name = os.Args[1]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		r, err := c.SayHello(ctx,
			&pb.HelloRequest{Name: name},
			// 这里可以再次设置重试次数，重试时间，重试返回码
			grpc_retry.WithMax(3),
			grpc_retry.WithPerRetryTimeout(time.Duration(5)*time.Millisecond),
			grpc_retry.WithCodes(codes.DeadlineExceeded),
		)
		if err != nil {
			log.Fatalf("Could not greet: %v", err)
			return
		}
		log.Printf("Greeting: %s", r.Message)
	}
}
