package server

import (
	"bladedb"
	proto "bladedb/proto"
	"context"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
)

type BladeDBServer struct {
}

var activeServer *grpc.Server

func (*BladeDBServer) Get(ctx context.Context, request *proto.GetRequest) (*proto.GetResponse, error) {
	value, err := bladedb.Get([]byte(request.Key))

	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while fetching value: %v", request.Key)
	}

	if value == nil {
		return nil, status.Errorf(codes.NotFound, "could not find: %v", request.Key)
	}

	response := &proto.GetResponse{
		Key:   request.Key,
		Value: value,
	}
	return response, nil
}

func (*BladeDBServer) Set(ctx context.Context, request *proto.SetRequest) (*proto.SetResponse, error) {
	err := bladedb.Put([]byte(request.Key), request.Value)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while writing key: %v, value: %v, error: %v",
			request.Key, request.Value, err)
	}

	response := &proto.SetResponse{
		Status: true,
	}
	return response, nil
}

func (*BladeDBServer) Del(ctx context.Context, request *proto.DelRequest) (*proto.DelResponse, error) {
	value, err := bladedb.Delete([]byte(request.Key))

	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while deleting key: %v error: %v", request.Key, err)
	}

	response := &proto.DelResponse{
		Value: value,
	}
	return response, nil
}

func StartServer(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return errors.Wrapf(err, "Error while setting up server: %s", address)
	}
	server := grpc.NewServer()
	proto.RegisterBladeDBServer(server, &BladeDBServer{})
	activeServer = server
	serverErr := server.Serve(lis)
	if serverErr != nil {
		return errors.Wrapf(err, "Error while setting up server: %v", serverErr)
	}
	return nil
}

func StopServer() {
	if activeServer != nil {
		activeServer.GracefulStop()
	}
}
