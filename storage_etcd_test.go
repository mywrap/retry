package retry

import (
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/mywrap/gofast"
	"go.etcd.io/etcd/v3/clientv3"
)

func TestEtcdStorage(t *testing.T) {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{
			"192.168.99.100:2379",
			"192.168.99.101:2379",
			"192.168.99.102:2379",
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("error etcd clientv3 New: %v", err)
	}
	defer etcdCli.Close()

	etcdSto, err := NewEtcdStorage(etcdCli, "/retrierTest")
	if err != nil {
		t.Fatalf("error NewEtcdStorage: %v", err)
	}

	cfg := &Config{MaxAttempts: 10,
		Delay: 50 * time.Millisecond, MaxJitter: 10 * time.Millisecond,
	}
	r := NewRetrier(jobCheckPayment, cfg, etcdSto)
	const nJobs = 2000
	wg := &sync.WaitGroup{}
	didJobs := make([]Job, nJobs)
	for i := 0; i < nJobs; i++ {
		txId := gofast.UUIDGenNoHyphen()
		wg.Add(1)
		go func(i int) {
			defer wg.Add(-1)
			t.Logf("about to Do jobId %v", txId)
			didJob, err := r.Do(JobId(txId), txId, time.Now().Format(time.RFC3339))
			if err != nil {
				t.Errorf("error retrier do: %v", err)
			}
			didJobs[i] = didJob
			// random do job again
			if rand.Intn(100) < 20 {
				_, err := r.Do(JobId(txId), txId, time.Now().Format(time.RFC3339))
				if !errors.Is(err, ErrJobRunningOrStopped) {
					t.Errorf("unexpected error retrier do: %v", err)
				}
			}
		}(i)
		// random stop job
		go func() {
			if rand.Intn(100) < 50 {
				time.Sleep(r.cfg.DelayType(1+rand.Intn(5), r.cfg))
				err := r.Stop(JobId(txId))
				if err != nil && err != ErrJobNotRunning {
					t.Errorf("error retrier stop: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	nNotDoneJobs := 0
	for _, job := range didJobs {
		if job.LastErr() != nil {
			nNotDoneJobs += 1
		}
	}
	t.Logf("nNotDoneJobs: %v", nNotDoneJobs)
	if nNotDoneJobs < nJobs/5 {
		t.Errorf("too small number of not done jobs: %v", nNotDoneJobs)
	}
}
