package retry

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"
)

type Retrier struct {
	jobFunc      func(inputs ...interface{}) error
	cfg          *Config
	storage      Storage
	hostname     string
	stopJobChans map[JobId]chan bool
	mutex        *sync.Mutex // for map stopJobChans
}

// :param jobFunc: inputs must be JSONable if using persist storage
// :param retrierName: optional, used as a namespace in persist storage
func NewRetrier(jobFunc func(inputs ...interface{}) error,
	cfg *Config, storage Storage, retrierName string) *Retrier {
	if cfg == nil {
		cfg = NewDefaultConfig()
	}
	if storage == nil {
		storage = NewMemoryStorage()
	}
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "hostname0"
	}
	return &Retrier{
		jobFunc: jobFunc,
		cfg:     cfg, storage: storage, hostname: hostname,
		stopJobChans: make(map[JobId]chan bool), mutex: &sync.Mutex{},
	}
}

// do runs a job (newly created or loaded from queue jobs in storage),
// :params jobFuncInputs: will be ignored if the jobId existed in storage
func (r Retrier) Do(jobId JobId, jobFuncInputs ...interface{}) (*Job, error) {
	job, err := r.storage.TakeOrCreateJob(jobId, jobFuncInputs)
	if err != nil {
		return nil, fmt.Errorf("storage TakeOrCreateJob: %v", err)
	}
	stopJobChan := make(chan bool)
	r.mutex.Lock()
	r.stopJobChans[job.Id] = stopJobChan
	r.mutex.Unlock()
	for {
		err := r.jobFunc(job.JobFuncInputs...)
		job.NTries++
		job.LastTried = time.Now()
		job.LastHost = r.hostname
		job.NextDelay = r.cfg.DelayType(job.NTries, r.cfg)
		if r.cfg.MaxDelay > 0 && job.NextDelay > r.cfg.MaxDelay {
			job.NextDelay = r.cfg.MaxDelay
		}
		if err == nil {
			job.Errors = append(job.Errors, "")
			job.Status = Stopped // successfully do the job
		} else {
			job.Errors = append(job.Errors, err.Error())
			if job.NTries == r.cfg.MaxAttempts {
				job.Status = Stopped //  give up after max attempts
			} else {
				job.Status = Running
			}
		}
		err = r.storage.UpdateJob(job)
		if err != nil {
			return job, fmt.Errorf("storage UpdateJob: %v", err)
		}
		if job.Status == Stopped {
			return job, nil
		}
		select {
		case <-time.After(job.NextDelay):
			continue
		case <-stopJobChan:
			job.Status = Stopped
			err = r.storage.UpdateJob(job)
			if err != nil {
				return job, fmt.Errorf("storage UpdateJob: %v", err)
			}
			return job, nil
		}
	}
}

// LoopTakeQueueJobs is meaningless on MemoryStorage because queue jobs do not exist
func (r Retrier) LoopTakeQueueJobs() {
	for {
		coolDown := r.cfg.Delay // have to call time.Sleep(coolDown) in each loop
		if coolDown < 1*time.Minute {
			coolDown = 1 * time.Minute
		}

		_, err := r.storage.RequeueHangingJobs()
		if err != nil {
			time.Sleep(coolDown)
			continue
		}
		jobs, err := r.storage.TakeJobs()
		if err != nil {
			time.Sleep(coolDown)
			continue
		}
		for _, job := range jobs {
			r.Do(job.Id)
		}
	}
}

func (r Retrier) Stop(jobId JobId) error {
	r.mutex.Lock()
	stopJobChan, found := r.stopJobChans[jobId]
	r.mutex.Unlock()
	if !found {
		return ErrJobIsNotRunning
	}
	stopJobChan <- true
	r.mutex.Lock()
	delete(r.stopJobChans, jobId)
	r.mutex.Unlock()
	return nil
}

// Config for Retrier
type Config struct {
	MaxAttempts int
	Delay       time.Duration // delay between retries, default 50ms
	MaxDelay    time.Duration // max delay between retries, not apply by default
	MaxJitter   time.Duration // random duration added to delay, default 100ms
	// defined delay duration for n-th attempt,
	// default is exponential back-off with jitter function
	DelayType func(nTries int, config *Config) time.Duration
}

func NewDefaultConfig() *Config {
	return &Config{
		MaxAttempts: 10,
		Delay:       50 * time.Millisecond,
		MaxJitter:   100 * time.Millisecond,
		DelayType:   ExpBackOffDelay,
	}
}

// DelayTypeFunc determine delay is after a fail try,
// (exponential back-off or fixed delay are common choices)
type DelayTypeFunc func(nTries int, config *Config) time.Duration

// a Fibonacci delay sequence,
// example if base delay is 50ms then next 9 delays are  50, 80, 130, .. , 3800 ms
func ExpBackOffDelay(nTries int, config *Config) time.Duration {
	delay := config.Delay * time.Duration(math.Pow(1.618, float64(nTries-1)))
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

type JobId string     // unique, example catted from a meaningful prefix and a UUID
type JobStatus string // enum

// JobStatus enum
const (
	Queue   = "QUEUE"
	Running = "RUNNING"
	Stopped = "STOPPED"
)

var (
	ErrJobRunning      = errors.New("job is running")
	ErrJobIsNotRunning = errors.New("job is not running on this machine")
	errNotImplemented  = errors.New("not implemented")
)

type Storage interface {
	// read jobId from queue jobs (ignore jobFuncInputs), create if not existed,
	// return err if the job is running (maybe on other machine),
	// update the jobs status to running
	TakeOrCreateJob(jobId JobId, jobFuncInputs []interface{}) (*Job, error)
	UpdateJob(*Job) error // call after one attempt or when manually stop the job
	// Check all running jobs, if a job lastAttempted too long time ago change it
	// status to queue. Costly func, should run once per minutes.
	RequeueHangingJobs() (nRequeueJobs int, err error)
	// take all queuing jobs to run, update the jobs status to running
	TakeJobs() ([]*Job, error)
}
