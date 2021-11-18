package main

import (
	"errors"
	"log"
	"math/rand"
	"time"

	"github.com/mywrap/retry"
	"go.etcd.io/etcd/v3/clientv3"
)

func main() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
	etcdConfig0 := clientv3.Config{
		Endpoints: []string{
			"192.168.99.100:2379",
			"192.168.99.101:2379",
			"192.168.99.102:2379"},
		DialTimeout: 5 * time.Second}
	etcdSto, err := retry.NewEtcdStorage(etcdConfig0, "/retrierTest3e5448c4")
	if err != nil {
		log.Fatalf("error NewEtcdStorage: %v", err)
	}
	log.Println("inited etcd storage")

	callUnreliableResource := func(inputs ...interface{}) error {
		log.Printf("about to callUnreliableResource")
		if rand.Intn(100) < 90 {
			return errors.New("resourceUnavailable")
		}
		return nil
	}
	cfg := &retry.Config{MaxAttempts: 15, DelayType: retry.ExpBackOffDelay,
		Delay: 100 * time.Millisecond, MaxJitter: 0 * time.Millisecond}
	retrier := retry.NewRetrier(callUnreliableResource, cfg, etcdSto)

	// checks all running jobs in storage, if a job lastAttempted too long time '
	// ago (maybe the runner crashed) then run the job
	go retrier.LoopTakeQueueJobs()

	// run a new job
	runResult, err := retrier.Do("callUnreliableResourceInput049")
	if err != nil {
		log.Fatalf("error retrier Do: %v", err)
	}
	log.Printf("errors of attempts: %#v", runResult.Errors)
}
