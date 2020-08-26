package retry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Retrier is used for retrying jobs, retry state will be saved to a persistent
// Storage (etcd) so others machine (or restarted machine) can continue the jobs
// after a crash.
type Retrier struct {
	jobFunc     func(inputs ...interface{}) error
	cfg         *Config
	Storage     Storage
	retrierName string // all machines in a cluster share this name
	Hostname    string
	stopJobCxls map[JobId]context.CancelFunc
	mutex       *sync.Mutex // for map stopJobCxls
}

// NewRetrier init a retrier, default save retrying state to memory.
// :param jobFunc: inputs must be JSONable if using persist Storage
// :param retrierName: optional, used as a namespace in persist Storage
func NewRetrier(jobFunc func(inputs ...interface{}) error,
	cfg *Config, storage Storage, retrierName string) *Retrier {
	r := &Retrier{
		jobFunc: jobFunc, cfg: cfg, Storage: storage, retrierName: retrierName,
		stopJobCxls: make(map[JobId]context.CancelFunc), mutex: &sync.Mutex{}}
	if r.cfg == nil {
		r.cfg = NewDefaultConfig()
	}
	if r.cfg.DelayType == nil {
		r.cfg.DelayType = ExpBackOffDelay
	}
	if r.Storage == nil {
		r.Storage = NewMemoryStorage()
	}
	r.Hostname, _ = os.Hostname()
	if r.Hostname == "" {
		r.Hostname = "hostname0"
	}
	if r.retrierName == "" {
		r.retrierName = "/retrier0"
	}
	return r
}

// Do runs a job (newly created or loaded from queue jobs in Storage),
// if returned error is not nil or ErrJobRunningOrStopped, user should Do again
// to ensure the job created
// :params jobFuncInputs: will be ignored if the jobId existed in Storage
func (r Retrier) Do(jobId JobId, jobFuncInputs ...interface{}) (Job, error) {
	j, err := r.Storage.TakeOrCreateJob(jobId, jobFuncInputs)
	if err != nil {
		return Job{}, fmt.Errorf("error storage TakeOrCreateJob: %w", err)
	}

	stopCtx, cxl := context.WithCancel(context.Background())
	r.mutex.Lock()
	r.stopJobCxls[j.Id] = cxl
	r.mutex.Unlock()
	defer func() {
		r.mutex.Lock()
		delete(r.stopJobCxls, j.Id)
		r.mutex.Unlock()
	}()
	for {
		err := r.jobFunc(j.JobFuncInputs...)
		j.NTries++
		j.LastTried = time.Now()
		j.LastHost = r.Hostname
		j.NextDelay = r.cfg.DelayType(j.NTries, r.cfg)
		if r.cfg.MaxDelay > 0 && j.NextDelay > r.cfg.MaxDelay {
			j.NextDelay = r.cfg.MaxDelay
		}
		if err == nil {
			j.Errors = append(j.Errors, "")
			j.Status = Stopped // successfully do the job
		} else {
			j.Errors = append(j.Errors, err.Error())
			if j.NTries >= r.cfg.MaxAttempts {
				j.Status = Stopped //  give up after max attempts
			} else {
				j.Status = Running
			}
		}
		err = r.Storage.UpdateJob(j)
		if err != nil {
			return j, fmt.Errorf("error storage UpdateJob: %w", err)
		}
		if j.Status == Stopped {
			return j, nil
		}
		select {
		case <-time.After(j.NextDelay):
			continue
		case <-stopCtx.Done():
			j.Status = Stopped
			err = r.Storage.UpdateJob(j)
			if err != nil {
				return j, fmt.Errorf("error storage UpdateJob: %w", err)
			}
			return j, nil
		}
	}
}

func (r Retrier) Stop(jobId JobId) error {
	r.mutex.Lock()
	stopJobCxl, found := r.stopJobCxls[jobId]
	r.mutex.Unlock()
	if !found {
		return ErrJobNotRunning
	}
	stopJobCxl()
	return nil
}

// LoopTakeQueueJobs is meaningless on MemoryStorage because queue jobs do not exist
func (r Retrier) LoopTakeQueueJobs() {
	for {
		coolDown := r.cfg.Delay // have to call time.Sleep(coolDown) in each loop
		if coolDown < 1*time.Minute {
			coolDown = 1 * time.Minute
		}

		_, err := r.Storage.RequeueHangingJobs()
		if err != nil {
			time.Sleep(coolDown)
			continue
		}
		jobs, err := r.Storage.TakeJobs()
		if err != nil {
			time.Sleep(coolDown)
			continue
		}
		for _, job := range jobs {
			r.Do(job.Id)
		}
	}
}

// Config for Retrier
type Config struct {
	MaxAttempts int
	Delay       time.Duration // delay between retries, default 100ms
	MaxDelay    time.Duration // max delay between retries, not apply by default
	MaxJitter   time.Duration // random duration added to delay, default 100ms
	// defined delay duration for n-th attempt,
	// default is exponential back-off with jitter function
	DelayType func(nTries int, config *Config) time.Duration
}

func NewDefaultConfig() *Config {
	return &Config{
		MaxAttempts: 10,
		Delay:       100 * time.Millisecond,
		MaxJitter:   100 * time.Millisecond,
		DelayType:   ExpBackOffDelay,
	}
}

// DelayTypeFunc determine delay is after a fail try,
// (exponential back-off or fixed delay are common choices)
type DelayTypeFunc func(nTries int, config *Config) time.Duration

var (
	sqrt5 = math.Sqrt(5)
	phiPo = (1 + sqrt5) / 2
	phiNe = (1 - sqrt5) / 2
)

func fibonacci(n int) int64 {
	nn := float64(n)
	return int64(math.Round((math.Pow(phiPo, nn) - math.Pow(phiNe, nn)) / sqrt5))
}

// a Fibonacci delay sequence,
// example if base delay is 100ms then next 9 delays are  100, 200, 300, .. , 5500 ms
func ExpBackOffDelay(nTries int, config *Config) time.Duration {
	delay := config.Delay * time.Duration(fibonacci(nTries))
	if config.MaxJitter > 0 {
		delay += time.Duration(rand.Int63n(int64(config.MaxJitter)))
	}
	return delay
}

// FixedDelay is a DelayType which keeps delay the same through all iterations
func FixedDelay(nTries int, config *Config) time.Duration {
	delay := config.Delay
	if config.MaxJitter > 0 {
		delay += time.Duration(rand.Int63n(int64(config.MaxJitter)))
	}
	return delay
}

type Job struct {
	JobFuncInputs []interface{}
	Id            JobId // unique
	Status        JobStatus
	NTries        int // number of tried attempts
	NextDelay     time.Duration
	LastTried     time.Time
	LastHost      string   // human readable, the last machine run the job
	Errors        []string // errors of attempts
}

func (j Job) LastErr() error {
	if len(j.Errors) < 1 {
		return nil
	}
	lastErrMsg := j.Errors[len(j.Errors)-1]
	if lastErrMsg == "" {
		return nil
	}
	return errors.New(lastErrMsg)
}

func (j Job) AllErrorsStr() string {
	bs, _ := json.Marshal(j.Errors)
	return string(bs)
}

type JobId string     // unique
type JobStatus string // enum

// JobStatus enum
const (
	Queue   = "QUEUE"
	Running = "RUNNING"
	Stopped = "STOPPED"
)

var (
	ErrJobRunningOrStopped = errors.New("job is running or stopped")
	ErrJobNotRunning       = errors.New("job is not running on this machine")
	errNotImplemented      = errors.New("not implemented")
)

type Storage interface {
	// read jobId from queue jobs (ignore jobFuncInputs), create if not existed,
	// return err ErrJobRunningOrStopped or underlying storage error,
	// update the jobs status to running
	TakeOrCreateJob(jobId JobId, jobFuncInputs []interface{}) (Job, error)
	UpdateJob(Job) error // call after one attempt or when manually stop the job
	// Check all running jobs, if a job lastAttempted too long time ago change it
	// status to queue. Costly func, should run once per minutes.
	RequeueHangingJobs() (nRequeueJobs int, err error)
	// take all queuing jobs to run, update the jobs status to running
	TakeJobs() ([]Job, error)
	DeleteStoppedJobs() (nDeletedJobs int, err error)
}
