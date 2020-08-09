package main

import (
	"bladedb"
	"bladedb/server"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	configPath := flag.String("config", "", "db config file path")
	embeddedMode := flag.Bool("embed-mode", true, "db mode")
	flag.Parse()

	_, err := os.Stat(*configPath)
	if os.IsNotExist(err) {
		log.Fatalf(fmt.Sprintf("could not locate config file: %s", *configPath))
	}
	bladedb.Open(*configPath)
	if !*embeddedMode {
		log.Println("Starting blade in Server mode")
		go func() {
			log.Println(fmt.Sprintf("starting bladedb server, listening on %s", bladedb.ServerAddress))
			err := server.StartServer(bladedb.ServerAddress)
			if err != nil {
				log.Fatalf(fmt.Sprintf("error while staring blade db, error: %v", err))
			}
		}()
	} else {
		log.Println("Starting blade in Embedded mode")
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGUSR1) //kill -SIGUSR1 pId
	//it will cause db to stop accepting new requests,
	// flush all memtable and wait for all compaction to complete - will be replaced with cool cli tool like nodetool in cassandra
	for {
		sig := <-sigs
		log.Println(fmt.Sprintf("received signal: %v to stop bladedb", sig))
		server.StopServer()
		bladedb.Drain()
		break
	}
}
