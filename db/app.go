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
	if len(os.Args) < 2 {
		panic("No config file passed, exiting db")
	}
	configPath := os.Args[1]
	bladedb.Open(configPath)
	fmt.Println(fmt.Sprintf("starting bladedb server, listening on %s", bladedb.ServerAddress))
	go server.StartServer(bladedb.ServerAddress)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGUSR1) //kill -SIGUSR1 pId
	//it will cause db to stop accepting new requests,
	// flush all memtable and wait for all compaction to complete - will be replaced with cool cli tool like nodetool in cassandra
	for {
		sig := <-sigs
		fmt.Println(fmt.Sprintf("received signal: %v to stop bladedb", sig))
		server.StopServer()
		bladedb.Drain()
		break
	}
}
