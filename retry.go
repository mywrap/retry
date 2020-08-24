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
	jobFunc      func(jsonArgs string) error
	cfg          *Config
	storage      Storage
	hostname     string
	stopJobChans map[JobId]chan bool
	mutex        *sync.Mutex
}

func NewRetrier(jobFunc func(jsonArgs string) error,
	cfg *Config, storage Storage) *Retrier {
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
// :params jobFuncJsonArgs: will be ignored if the jobId existed in storage
func (r *Retrier) Do(jobId JobId, jobFUncJsonArgsIfCreate string) (*Job, error) {
	job, err := r.storage.TakeJob(jobId, jobFUncJsonArgsIfCreate)
	if err != nil {
		return nil, fmt.Errorf("storage TakeJob: %v", err)
	}
	stopJobChan := make(chan bool)
	r.mutex.Lock()
	r.stopJobChans[job.Id] = stopJobChan
	r.mutex.Unlock()
	for {
		err := r.jobFunc(job.JobFuncJsonArgs)
		job.NAttempts++
		job.LastAttempted = time.Now()
		job.LastAttemptedHost = r.hostname
		job.NextDelay = r.cfg.DelayType(job.NAttempts, r.cfg)
		if r.cfg.MaxDelay > 0 && job.NextDelay > r.cfg.MaxDelay {
			job.NextDelay = r.cfg.MaxDelay
		}
		if err == nil {
			job.Errors = append(job.Errors, "")
			job.Status = Stopped // successfully do the job
		} else {
			job.Errors = append(job.Errors, err.Error())
			if job.NAttempts == r.cfg.MaxAttempts {
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

func (r *Retrier) Stop(jobId JobId) error {
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

type Config struct {
	MaxAttempts int
	Delay       time.Duration // delay between retries, default 50ms
	MaxDelay    time.Duration // max delay between retries, not apply by default
	MaxJitter   time.Duration // random duration added to delay, default 100ms
	// defined delay duration for n-th attempt,
	// default is exponential back-off with jitter function
	DelayType func(nAttempts int, config *Config) time.Duration
}

func NewDefaultConfig() *Config {
	return &Config{
		MaxAttempts: 10,
		Delay:       50 * time.Millisecond,
		MaxJitter:   100 * time.Millisecond,
		DelayType:   ExpBackOffJitterDelay,
	}
}

type DelayTypeFunc func(nAttempts int, config *Config) time.Duration

// a Fibonacci delay sequence,
// example if base delay is 50ms then 10th retry delay is 3800 ms
func ExpBackOffJitterDelay(nAttempts int, config *Config) time.Duration {
	delay := config.Delay * time.Duration(math.Pow(1.618, float64(nAttempts-1)))
	delay += time.Duration(rand.Int63n(int64(config.MaxJitter)))
	return delay
}

type Job struct {
	JobFuncJsonArgs   string
	Id                JobId // unique
	Status            JobStatus
	NAttempts         int // number of tried attempts
	LastAttempted     time.Time
	LastAttemptedHost string // human readable
	NextDelay         time.Duration
	Errors            []string // errors of attempts
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

type Storage interface {
	// read jobId from queue jobs, create if not existed,
	// return err if the job is running (maybe on other machine),
	// update the jobs status to running
	TakeJob(jobId JobId, jsonArgsIfCreate string) (*Job, error)
	// take some queuing jobs to run,
	// update the jobs status to running
	TakeJobs(maxJobs int) ([]*Job, error)
	UpdateJob(*Job) error // call after one attempt
	StopJob(jobId JobId) (*Job, error)

	// Check all running jobs, if a job lastAttempted too long time ago change it
	// status to queue
	RequeueHangingJobs() ([]*Job, error)
}

type MemoryStorage struct {
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }
func (s *MemoryStorage) TakeJob(jobId JobId, jsonArgsIfCreate string) (*Job, error) {
	return nil, errNotImplemented // TODO MemoryStorage ReadJob
}
func (s *MemoryStorage) TakeJobs(maxJobs int) ([]*Job, error) {
	return nil, errNotImplemented // TODO MemoryStorage TakeJobs
}
func (s *MemoryStorage) UpdateJob(*Job) error {
	return errNotImplemented // TODO MemoryStorage UpdateJob
}
func (s *MemoryStorage) StopJob(jobId JobId) (*Job, error) {
	return nil, errNotImplemented
}
func (s *MemoryStorage) RequeueHangingJobs() ([]*Job, error) {
	return nil, errNotImplemented // TODO MemoryStorage RequeueCrashJobs
}

var (
	ErrJobRunning      = errors.New("job is running")
	ErrJobIsNotRunning = errors.New("job is not running on this machine")
	errNotImplemented  = errors.New("not implemented")
)
