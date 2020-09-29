package retry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mywrap/gofast"
	"go.etcd.io/etcd/v3/clientv3"
	"go.etcd.io/etcd/v3/clientv3/concurrency"
)

var etcdConfig0 = clientv3.Config{
	Endpoints: []string{
		"192.168.99.100:2379",
		"192.168.99.101:2379",
		"192.168.99.102:2379",
	},
	DialTimeout: 5 * time.Second,
	//Username:    "root",
	//Password:    "123qwe",
}

func getInt64(cli *clientv3.Client, key string) int64 {
	resp, err := cli.Get(context.Background(), key)
	if err != nil {
		return 0
	}
	if len(resp.Kvs) < 1 {
		return 0
	}
	ret, _ := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	return ret
}

func TestEtcdLockSum(t *testing.T) {
	Log = log.New(os.Stdout, "", log.Lshortfile|log.Lmicroseconds)
	const nWorkers = 50
	const sharedSumKey = "/TestEtcdLockSum/sharedSum"
	const lockKey = "/TestEtcdLockSum/lock"
	const expectedSum = int64(500)
	cli0, err := clientv3.New(etcdConfig0)
	if err != nil {
		t.Fatalf("error etcd clientv3 New: %v", err)
	}
	ctx, cxl := context.WithTimeout(context.Background(), 1*time.Second)
	_, err = cli0.Put(ctx, sharedSumKey, "0")
	cxl()
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)
			cli1, err := clientv3.New(etcdConfig0)
			if err != nil {
				t.Fatal(err)
			}
			session, err := concurrency.NewSession(cli1, concurrency.WithTTL(5))
			if err != nil {
				t.Fatal(err)
			}
			mutex := concurrency.NewMutex(session, lockKey)
			for k := 0; k < int(expectedSum/nWorkers); k++ {
				if err := mutex.Lock(context.Background()); err != nil {
					t.Error(err)
				}
				sum := getInt64(cli1, sharedSumKey)
				sum += 1
				_, err = cli1.Put(context.Background(), sharedSumKey,
					strconv.FormatInt(sum, 10))
				if err != nil {
					t.Error(err)
				}
				if err := mutex.Unlock(context.Background()); err != nil {
					t.Error(err)
				}
			}

			session.Close()
			cli1.Close()
		}()
	}
	wg.Wait()
	if realSum := getInt64(cli0, sharedSumKey); realSum != expectedSum {
		t.Errorf("error realSum: %v, expectedSum: %v", realSum, expectedSum)
	}
}

func TestEtcdStorageNewRetrier(t *testing.T) {
	etcdSto, err := NewEtcdStorage(etcdConfig0, "/retrierTestNew"+gofast.GenUUID())
	if err != nil {
		t.Fatalf("error NewEtcdStorage: %v", err)
	}
	t.Logf("ret NewEtcdStorage: %v, %v", etcdSto, err)

	cfg := &Config{MaxAttempts: 10,
		Delay: 25 * time.Millisecond, MaxJitter: 5 * time.Millisecond,
	}
	const nJobs = 2000

	r := NewRetrier(jobCheckPayment, cfg, etcdSto)
	n, err := etcdSto.deleteAllKey()
	Log.Printf("etcdSto deleteAllKey: %v, %v\n", n, err)
	if err != nil {
		t.Error(err)
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < nJobs; i++ {
		txId := gofast.UUIDGenNoHyphen()
		wg.Add(1)
		go func(i int) {
			defer wg.Add(-1)
			_, err := r.Do(JobId(txId), txId, time.Now().Format(time.RFC3339Nano))
			if err != nil {
				t.Errorf("error retrier do: %v", err)
			}
			// random do job again
			if rand.Intn(100) < 20 {
				_, err := r.Do(JobId(txId), txId, time.Now().Format(time.RFC3339Nano))
				if !errors.Is(err, ErrDuplicateJob) {
					t.Errorf("error retrier do2: %v", err)
				}
			}
		}(i)
		// random stop job
		wg.Add(1)
		go func() {
			defer wg.Add(-1)
			if rand.Intn(100) < 50 {
				time.Sleep(r.cfg.DelayType(5+rand.Intn(5), r.cfg))
				err := r.StopJob(JobId(txId))
				//Log.Printf("manually stop ret: %v, %v\n", err, txId)
				if err != nil && err != ErrJobNotRunning {
					t.Errorf("error retrier stop: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	r.mutex.Lock()
	if r.nDoOKJobs+r.nStopOKJobs != nJobs {
		t.Errorf("error nDonedJobs: %v, nJobs: %v", r.nDoOKJobs+r.nStopOKJobs, nJobs)
	}
	Log.Printf("nManuallyStoppeds: %v\n", r.nStopOKJobs)
	if r.nStopOKJobs < 1 {
		t.Errorf("error small nManuallyStoppeds: %v, expected: %v",
			r.nStopOKJobs, nJobs/5)
	}
	r.mutex.Unlock()

	// view metric
	if etcdSto.metric != nil {
		for _, row := range etcdSto.metric.GetCurrentMetric() {
			t.Logf("metric row %#v", row)
		}
	}

	// printf '\ec'; etcdctl get --prefix /retrierTest/job
}

func FuncTestEtcdStorageResumeRetrier(t *testing.T, isExitWhenRunning bool) {
	t.Logf("begin FuncTestEtcdStorageResumeRetrier")
	etcdSto, err := NewEtcdStorage(etcdConfig0, "/retrierTestResume")
	if err != nil {
		t.Fatalf("error NewEtcdStorage: %v", err)
	}
	t.Logf("ret NewEtcdStorage: %v, %v", etcdSto, err)

	cfg := &Config{MaxAttempts: 10,
		Delay: 100 * time.Millisecond, MaxJitter: 5 * time.Millisecond,
	}
	const nJobs = 100
	r := NewRetrier(jobCheckPayment, cfg, etcdSto)

	go r.LoopTakeQueueJobs()
	t.Logf("in queue keys: %v", etcdSto.keyPfx+pfxIdxStatusNextTry+Queue)

	wg := &sync.WaitGroup{}
	for i := 0; i < nJobs; i++ {
		txId := gofast.UUIDGenNoHyphen()
		wg.Add(1)
		go func(i int) {
			defer wg.Add(-1)
			_, err := r.Do(JobId(txId), txId, time.Now().Format(time.RFC3339Nano))
			if err != nil {
				t.Errorf("error retrier do: %v", err)
			}
		}(i)
	}
	time.Sleep(r.cfg.DelayType(6+rand.Intn(4), r.cfg))
	if isExitWhenRunning {
		running, _ := r.Storage.ReadJobsRunning()
		t.Logf("end when running, nRunningJobs: %v", len(running))
		return
	}
	wg.Wait()
	r.mutex.Lock()
	time.Sleep(1 * time.Second)
	t.Logf("nDoOKJobs: %v", r.nDoOKJobs)
	if r.nDoErrJobs > 0 {
		t.Errorf("nDoErrJobs: %v", r.nDoErrJobs)
	}
	r.mutex.Unlock()
	t.Logf("end normally FuncTestEtcdStorageResumeRetrier")
}

func TestEtcdStorageResumeRetrier(t *testing.T) {
	FuncTestEtcdStorageResumeRetrier(t, true)
}

func TestEtcdStorageResumeRetrier2(t *testing.T) {
	FuncTestEtcdStorageResumeRetrier(t, false)
}

func TestEtcdStorage_Monitor(t *testing.T) {
	etcdSto, err := NewEtcdStorage(etcdConfig0, "/retrierTestMonitor")
	if err != nil {
		t.Fatalf("error NewEtcdStorage: %v", err)
	}
	etcdSto.DeleteStoppedJobs()
	cfg := &Config{MaxAttempts: 10,
		Delay: 50 * time.Millisecond, MaxJitter: 10 * time.Millisecond}
	r := NewRetrier(jobCheckPayment, cfg, etcdSto)
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		ii := fmt.Sprintf("%02d", i)
		wg.Add(1)
		go func() {
			defer wg.Add(-1)
			_, err := r.Do(JobId("txId"+ii), "date0"+ii)
			if err != nil {
				t.Error(ii, err)
			}
		}()
	}
	done := make(chan bool)
	go func() {
	Loop0:
		for {
			runningJobs, err := r.Storage.ReadJobsRunning()
			t.Logf("nRunning: %v, err: %v", len(runningJobs), err)
			select {
			case <-time.After(500 * time.Millisecond):
				continue
			case <-done:
				break Loop0
			}
		}
	}()
	wg.Wait()
	runningJobs, err := r.Storage.ReadJobsRunning()
	beauty, _ := json.MarshalIndent(runningJobs, "", "\t")
	t.Logf("last running: %s, err: %v", beauty, err)
	if len(runningJobs) > 0 {
		t.Error(`error done still run ¯\_(ツ)_/¯`)
	}
	failAllAttemptsJobs, err := r.Storage.ReadJobsFailedAllAttempts()
	t.Logf("nFailAllAttemptsJobs: %v", len(failAllAttemptsJobs))
	if len(failAllAttemptsJobs) == 0 {
		t.Error("error unexpected 0 nFailAllAttemptsJobs")
	}
}
