package main

import (
	"bladedb"
	"bladedb/server"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	fmt.Println("process-id ", os.Getpid())
	bladedb.Open()
	address := fmt.Sprintf("0.0.0.0:%d", bladedb.DefaultConstants.ClientListenPort)
	fmt.Println(fmt.Sprintf("starting bladedb server, listening on %s", address))
	go server.StartServer(address)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGUSR1) //sending this signal will cause db to stop accepting new requests,
	// flush all memtable and wait for all compaction to complete - will be replaced with cool cli tool like nodetool in cassandra
	for {
		sig := <-sigs
		fmt.Println(fmt.Sprintf("received signal: %v to drain and close bladedb server", sig))
		server.StopServer()
		bladedb.Drain()
		break
	}
}
