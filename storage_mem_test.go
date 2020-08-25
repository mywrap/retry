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
		return "", errors.New("internal server error")
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
	cfg := &Config{MaxAttempts: 10,
		Delay: 10 * time.Millisecond, MaxJitter: 0 * time.Millisecond,
		DelayType: ExpBackOffDelay}
	r := NewRetrier(jobCheckPayment, cfg, nil, "")
	memSto := r.Storage.(*MemoryStorage)

	const nJobs = 10000
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
	r := NewRetrier(jobCheckPayment, nil, nil, "")
	memSto := r.Storage.(*MemoryStorage)
	const nJobs = 100
	wg := &sync.WaitGroup{}
	for i := 0; i < nJobs; i++ {
		txId := gofast.UUIDGenNoHyphen()
		wg.Add(1)
		go func() {
			defer wg.Add(-1)
			_, err := r.Do(JobId(txId), txId, time.Now().Format(time.RFC3339))
			if err != nil {
				t.Errorf("error retrier do: %v", err)
			}
		}()

		time.Sleep(r.cfg.DelayType(2+rand.Intn(8), r.cfg))
		wg.Add(1)
		go func() {
			defer wg.Add(-1)
			if rand.Intn(100) < 50 {
				_, err := r.Do(JobId(txId), txId, time.Now().Format(time.RFC3339))
				if !errors.Is(err, ErrJobRan) &&
					!errors.Is(err, ErrJobStopped) {
					t.Errorf("unexpected error retrier do: %v", err)
				}
				err = r.Stop(JobId(txId))
				if err != nil && err != ErrJobNotRunning {
					t.Errorf("error retrier stop: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	if r1, r2 := len(memSto.jobs), memSto.idxStatusNextTry.Len(); r1 != nJobs || r2 != nJobs {
		t.Errorf("unexpected nJobs: real: %v, %v, expected: %v", r1, r2, nJobs)
	}
	n, err := memSto.ExportCSV("jobsStopped2.csv")
	if err != nil || n != nJobs {
		t.Error(err, n)
	}
}
