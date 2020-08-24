package retry

import (
	"errors"
	"math"
	"math/rand"
	"time"
)

type Retrier struct {
	cfg     *Config
	storage Storage
}

// New creates a new job in storage,
// job status normally is running and will be doing by the machine created it
func (r Retrier) New(jobFunc func() error, jobId JobId, status JobStatus) (
	*Job, error) {
	job := &Job{
		Id:            jobId,
		Status:        status,
		NAttempts:     0,
		LastAttempted: time.Unix(0, 0),
		NextDelay:     r.cfg.DelayType(0, r.cfg),
		Errors:        make([]error, 0),
	}
	return job, nil
}

// do runs a job (newly created or taken from queue jobs in storage)
func (r Retrier) Do(jobId JobId) {
	var n uint

	//default
	config := &Config{
		attempts:      DefaultAttempts,
		delay:         DefaultDelay,
		maxJitter:     DefaultMaxJitter,
		onRetry:       DefaultOnRetry,
		retryIf:       DefaultRetryIf,
		delayType:     DefaultDelayType,
		lastErrorOnly: DefaultLastErrorOnly,
	}

	//apply opts
	for _, opt := range opts {
		opt(config)
	}

	var errorLog Error
	if !config.lastErrorOnly {
		errorLog = make(Error, config.attempts)
	} else {
		errorLog = make(Error, 1)
	}

	lastErrIndex := n
	for n < config.attempts {
		err := retryableFunc()

		if err != nil {
			errorLog[lastErrIndex] = unpackUnrecoverable(err)

			if !config.retryIf(err) {
				break
			}

			config.onRetry(n, err)

			// if this is last attempt - don't wait
			if n == config.attempts-1 {
				break
			}

			delayTime := config.delayType(n, config)
			if config.maxDelay > 0 && delayTime > config.maxDelay {
				delayTime = config.maxDelay
			}
			time.Sleep(delayTime)
		} else {
			return nil
		}

		n++
		if !config.lastErrorOnly {
			lastErrIndex = n
		}
	}

	if config.lastErrorOnly {
		return errorLog[lastErrIndex]
	}
	return errorLog
}

func (r Retrier) Stop(jobId JobId) error {
	return errors.New("TODO")
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

func ExpBackOffJitterDelay(nAttempts int, config *Config) time.Duration {
	// with a base delay 50ms, delay on 10th retry is 3800 ms
	delay := config.Delay * time.Duration(math.Pow(1.618, float64(nAttempts)))
	delay += time.Duration(rand.Int63n(int64(config.MaxJitter)))
	return delay
}

type Job struct {
	Id            JobId
	Status        JobStatus
	NAttempts     int // number of made attempts
	LastAttempted time.Time
	NextDelay     time.Duration
	Errors        []error // errors of attempts
}

func (j Job) LastErr() error {
	if len(j.Errors) < 1 {
		return nil
	}
	return j.Errors[len(j.Errors)-1]
}

// JobId must be unique, example catted from a meaningful prefix and a UUID
type JobId string
type JobStatus string

// JobStatus enum
const (
	Queue   = "QUEUE"
	Running = "RUNNING"
	Stopped = "STOPPED"
)

type Storage interface {
	// change some queue jobs to running
	// (understand as the jobs are doing on the machineId)
	TakeJobs(machineId string, maxJobs int) []*Job
	// Check all running jobs, if a job retried too long time ago change it
	// to a queue job
	RequeueCrashJobs() []*Job
}

type MemoryStorage struct {
}

func (s *MemoryStorage) TakeJobs(machineId string, maxJobs int) []*Job {
	return nil // TODO MemoryStorage TakeJobs
}

func (s *MemoryStorage) RequeueCrashJobs() []*Job {
	return nil // TODO MemoryStorage RequeueCrashJobs
}
