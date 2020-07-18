package main

import (
	proto "bladedb/proto"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
)

func main() {
	//change server address accordingly
	_IP := "0.0.0.0"
	_PORT := 9099
	address := fmt.Sprintf("%s:%d", _IP, _PORT)
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	client := proto.NewBladeDBClient(conn)
	defer conn.Close()

	//write
	key := "basic_test_key"
	value := "basic_test_value"
	res_1, err := client.Set(context.Background(), &proto.SetRequest{Key: key, Value: []byte(value)})
	if err == nil && res_1.Status {
		fmt.Println(fmt.Sprintf("Key: %s Value: %s written successfully", key, value))
	} else {
		panic(err)
	}

	//read
	res_2, err := client.Get(context.Background(), &proto.GetRequest{Key: key})
	if err != nil {
		panic(err)
	} else {
		fmt.Println(fmt.Sprintf("%s fetched for Key: %s", string(res_2.Value), res_2.Key))
	}

	//delete
	res_3, err := client.Del(context.Background(), &proto.DelRequest{Key: key})
	if err != nil {
		panic(err)
	} else {
		if res_3 != nil {
			fmt.Println(fmt.Sprintf("%s key deleted from db, value was %s", key, string(res_3.Value)))
		} else {
			panic(fmt.Sprintf("No such key %s found in db", key))
		}
	}

	//read - after delete
	_, err = client.Get(context.Background(), &proto.GetRequest{Key: key})
	errStatus, _ := status.FromError(err)
	if errStatus.Code() == codes.NotFound {
		fmt.Println("Keys not found after it deleted")
	} else {
		panic(err)
	}
}
