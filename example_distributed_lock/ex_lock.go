package main

import (
	"log"
	"time"

	"github.com/mywrap/retry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	log.Println("getting lock for mutex1")
	mutex1, err := retry.NewEtcdLock(cli, "/my_lock/")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("acquired lock for mutex1")
	var mutex2 *retry.EtcdLock
	m2Locked := make(chan struct{})
	go func() {
		log.Println("getting lock for mutex2")
		mutex2, err = retry.NewEtcdLock(cli, "/my_lock/")
		if err != nil {
			log.Fatal(err)
		}
		log.Println("acquired lock for mutex2")
		close(m2Locked)
	}()

	time.Sleep(100 * time.Millisecond)
	if true {
		mutex1.Unlock()
		log.Println("released lock for mutex1")
	} else {
		log.Println("should hang here if do not mutex1.Unlock()")
	}

	<-m2Locked
	_ = mutex2
	log.Println("main returned")

	// stdout:
	//
	// getting lock for mutex1
	// acquired lock for mutex1
	// getting lock for mutex2
	// released lock for mutex1
	// acquired lock for mutex2
	// main returned
}
