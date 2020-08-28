package retry

import (
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/mywrap/gofast"
)

func TestFibonacci(t *testing.T) {
	fibs := []int64{0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55}
	for i := 0; i <= 10; i++ {
		if r, e := fibonacci(i), fibs[i]; r != e {
			t.Errorf("fibonacci: real: %v, expected: %v", r, e)
		}
	}
}

func callUnreliable(txId string, callAt string) (string, error) {
	if rand.Intn(100) < 80 {
		return "", errors.New("resourceUnavailable")
	}
	return "SUCCESS", nil
}

func jobCheckPayment(inputs ...interface{}) error {
	var txId, date string
	if len(inputs) >= 2 {
		txId, _ = inputs[0].(string)
		date, _ = inputs[1].(string)
	}
	resp, err := callUnreliable(txId, date)
	_ = resp
	return err
}

func TestRetrierMemoryStorage1(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	cfg := &Config{MaxAttempts: 10, Delay: 10 * time.Millisecond}
	r := NewRetrier(jobCheckPayment, cfg, nil, nil)
	memSto := r.Storage.(*MemoryStorage)

	const nJobs = 8000
	wg := &sync.WaitGroup{}
	for i := 0; i < nJobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)
			txId := gofast.UUIDGenNoHyphen()
			_, err := r.Do(JobId(txId), txId, time.Now().Format(time.RFC3339))
			if err != nil {
				t.Errorf("error retrier do: %v", err)
			}
		}()
	}

	time.Sleep(r.cfg.DelayType(6, r.cfg))
	_, err := memSto.ExportCSV("jobsRunning1.csv")
	if err != nil {
		t.Error(err)
	}

	wg.Wait()
	n, err := memSto.ExportCSV("jobsStopped1.csv")
	if err != nil || n != nJobs {
		t.Error(err, n)
	}

	r.Storage.DeleteStoppedJobs()
	if len(memSto.jobs) != 0 || memSto.idxStatusNextTry.Len() != 0 {
		t.Error("fail to DeleteStoppedJobs")
	}
}

func TestRetrierMemoryStorage2(t *testing.T) {
	cfg := &Config{MaxAttempts: 10,
		Delay: 50 * time.Millisecond, MaxJitter: 10 * time.Millisecond}
	r := NewRetrier(jobCheckPayment, cfg, nil, nil)
	memSto := r.Storage.(*MemoryStorage)
	const nJobs = 2000
	wg := &sync.WaitGroup{}
	for i := 0; i < nJobs; i++ {
		txId := gofast.UUIDGenNoHyphen()
		wg.Add(1)
		go func(i int) {
			defer wg.Add(-1)
			_, err := r.Do(JobId(txId), txId, time.Now().Format(time.RFC3339))
			if err != nil {
				t.Errorf("error retrier do: %v", err)
			}
			// random do job again
			if rand.Intn(100) < 20 {
				_, err := r.Do(JobId(txId), txId, time.Now().Format(time.RFC3339))
				if !errors.Is(err, ErrDuplicateJob) {
					t.Errorf("unexpected error retrier do: %v", err)
				}
			}
		}(i)
		// random stop job
		wg.Add(1)
		go func() {
			defer wg.Add(-1)
			if rand.Intn(100) < 50 {
				time.Sleep(r.cfg.DelayType(1+rand.Intn(5), r.cfg))
				err := r.StopJob(JobId(txId))
				if err != nil && err != ErrJobNotRunning {
					t.Errorf("error retrier stop: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	if r.nDoOKJobs+r.nStopOKJobs != nJobs {
		t.Errorf("error number of done jobs: %v, expected: %v",
			r.nDoOKJobs+r.nStopOKJobs, nJobs)
	}
	//t.Logf("nManuallyStoppeds: %v", r.nStopOKJobs)
	if r.nStopOKJobs < 1 {
		t.Errorf("small nManuallyStoppeds: %v, expected: %v",
			r.nStopOKJobs, nJobs/3)
	}
	if l1, l2 := len(memSto.jobs), memSto.idxStatusNextTry.Len(); l1 != nJobs || l2 != nJobs {
		t.Errorf("unexpected nJobs: real: %v, %v, expected: %v", l1, l2, nJobs)
	}
	n, err := memSto.ExportCSV("jobsStopped2.csv")
	if err != nil || n != nJobs {
		t.Error(err, n)
	}
}
