// Command devredis 是本地开发/故障注入用的 miniredis（无需 Docker）。
package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/alicebob/miniredis/v2"
)

func main() {
	m := miniredis.NewMiniRedis()
	if err := m.StartAddr("127.0.0.1:6379"); err != nil {
		log.Fatal(err)
	}
	log.Printf("devredis (miniredis) on %s", m.Addr())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
}
