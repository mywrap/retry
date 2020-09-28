package retry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/mywrap/gofast"
)

// Retrier is used for retrying jobs, retry state will be saved to a persistent
// Storage (etcd) so others machine (or restarted machine) can continue the jobs
// after a crash.
type Retrier struct {
	jobFunc     func(inputs ...interface{}) error
	cfg         *Config
	Storage     Storage
	hostname    string // for human identifying machine
	stopJobCxls map[JobId]context.CancelFunc
	mutex       *sync.Mutex // for map stopJobCxls, nDoneJobs, nManuallyStops
	nDoOKJobs   int         // for testing, number of Do returned nil error
	nStopOKJobs int         // for testing, number of StopJob returned nil error
	nDoErrJobs  int         // for testing, number of Do returned storage error
}

// NewRetrier init a retrier, default save retrying state to memory.
// :param jobFunc: inputs of this func must be JSONable,
// :param retrierName: optional, used as a namespace in persist Storage,
func NewRetrier(jobFunc func(inputs ...interface{}) error,
	cfg *Config, storage Storage) *Retrier {
	r := &Retrier{
		jobFunc: jobFunc, cfg: cfg, Storage: storage,
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
	r.hostname, _ = os.Hostname()
	if r.hostname == "" {
		r.hostname = "hostname0"
	}
	return r
}

// Do runs a new job. If returned error is not nil and not ErrDuplicateJob, user
// should Do again to ensure the job created.
func (r *Retrier) Do(jobId JobId, jobFuncInputs ...interface{}) (Job, error) {
	//Log.Printf("doing JobId %v\n", jobId)
	j, err := r.Storage.CreateJob(jobId, jobFuncInputs)
	if err != nil {
		r.mutex.Lock()
		r.nDoErrJobs++
		r.mutex.Unlock()
		return Job{}, fmt.Errorf("storage CreateJob: %w", err)
	}
	return r.runJob(j)
}

func (r *Retrier) runJob(j Job) (Job, error) {
	stopCtx, cxl := context.WithCancel(context.Background())
	r.mutex.Lock()
	r.stopJobCxls[j.Id] = cxl
	r.mutex.Unlock()
	//Log.Printf("debug set stopJobCxls: %v\n", j.Id)
	defer func() {
		r.mutex.Lock()
		delete(r.stopJobCxls, j.Id)
		r.mutex.Unlock()
	}()
	for {
		err := r.jobFunc(j.JobFuncInputs...)
		j.NTries++
		j.LastTried = time.Now()
		j.LastHost = r.hostname
		j.NextDelay = r.cfg.DelayType(j.NTries, r.cfg)
		if r.cfg.MaxDelay > 0 && j.NextDelay > r.cfg.MaxDelay {
			j.NextDelay = r.cfg.MaxDelay
		}
		if gofast.CheckNilInterface(err) {
			j.Errors = append(j.Errors, "")
			j.Status = Stopped // successfully do the job
		} else {
			j.Errors = append(j.Errors, err.Error())
			if j.NTries >= r.cfg.MaxAttempts {
				j.IsFailedAllAttempts = true
				j.Status = Stopped
			} else {
				j.Status = Running
			}
		}
		err = r.Storage.UpdateJob(j)
		if err != nil {
			r.mutex.Lock()
			r.nDoErrJobs++
			r.mutex.Unlock()
			return j, fmt.Errorf("storage UpdateJob: %w", err)
		}
		if j.Status == Stopped {
			r.mutex.Lock()
			r.nDoOKJobs++
			r.mutex.Unlock()
			return j, nil
		}
		select {
		case <-time.After(j.NextDelay):
			continue
		case <-stopCtx.Done():
			j.Status = Stopped
			err = r.Storage.UpdateJob(j)
			if err != nil {
				r.mutex.Lock()
				r.nDoErrJobs++
				r.mutex.Unlock()
				return j, fmt.Errorf("storage UpdateJob: %w", err)
			}
			r.mutex.Lock()
			r.nStopOKJobs++
			r.mutex.Unlock()
			return j, nil
		}
	}
}

// StopJob manually stops a running job (before MaxAttempts).
// Only read the Retrier but need pointer receive because of Go
func (r *Retrier) StopJob(jobId JobId) error {
	r.mutex.Lock()
	stopJobCxl, found := r.stopJobCxls[jobId]
	r.mutex.Unlock()
	if !found {
		return ErrJobNotRunning
	}
	stopJobCxl()
	return nil
}

// LoopTakeQueueJobs checks all running jobs in storage, if a job lastAttempted
// too long time ago then run the job.
// This func is meaningless on MemoryStorage because queue jobs do not exist.
func (r *Retrier) LoopTakeQueueJobs() {
	go func() { // for human
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
		<-sigChan
		r.mutex.Lock()
		nRunnings := len(r.stopJobCxls)
		r.mutex.Unlock()
		Log.Printf("fatal os sigterm, nRunningJobs: %v\n", nRunnings)
		os.Exit(0)
	}()
	Log.Println("start LoopTakeQueueJobs")
	for {
		coolDown := r.cfg.Delay // have to call time.Sleep(coolDown) in each loop
		if coolDown < 1*time.Minute {
			coolDown = 1 * time.Minute
		}

		nRequeueJobs, err := r.Storage.RequeueHangingJobs()
		if err != nil {
			Log.Printf("error RequeueHangingJobs: %v\n", err)
			time.Sleep(coolDown)
			continue
		}
		if nRequeueJobs > 0 {
			Log.Printf("nRequeueJobs: %v\n", nRequeueJobs)
		}
		redoJobs, err := r.Storage.TakeJobs()
		if err != nil {
			Log.Printf("error TakeJobs: %v\n", err)
			time.Sleep(coolDown)
			continue
		}
		for _, redoJob := range redoJobs {
			go func(j Job) {
				_, err := r.runJob(j)
				if err != nil {
					Log.Printf("error redo job %v: %v\n", j.Id, err)
				}
			}(redoJob)
		}
		time.Sleep(coolDown)
	}
}

type Storage interface {
	// CreateJob creates a job with status running in the storage,
	// returned error can be ErrDuplicateJob or underlying storage error.
	CreateJob(jobId JobId, jobFuncInputs []interface{}) (Job, error)
	// return error if job_Id is not existed
	UpdateJob(Job) error
	// check all running jobs, if a job lastAttempted too long time ago change
	// it status to queue. Costly func, should run once per minutes.
	RequeueHangingJobs() (nRequeueJobs int, err error)
	// take all queuing jobs, update the jobs status to running
	TakeJobs() ([]Job, error)
	// clean up storage space
	DeleteStoppedJobs() (nDeletedJobs int, err error)

	// optional monitor functions

	ReadJobsRunning() ([]Job, error)
	ReadJobsFailedAllAttempts() ([]Job, error)
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
	// take care of changing this field data type when the Storage Read and
	// Update the job with JSON marshal and unmarshal operators
	JobFuncInputs []interface{}
	Id            JobId // unique

	Status              JobStatus
	IsFailedAllAttempts bool
	NTries              int // number of tried attempts
	NextDelay           time.Duration
	LastTried           time.Time
	LastHost            string   // human readable, the last machine run the job
	Errors              []string // errors of attempts
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

func (j Job) JsonMarshal() string {
	bs, _ := json.Marshal(j)
	return string(bs)
}

type JobId string     // unique
type JobStatus string // enum

// JobStatus enum
const (
	Queue   = "QUEUE" // hanging Running job will be changed to Queue
	Running = "RUNNING"
	Stopped = "STOPPED"
)

var (
	ErrDuplicateJob   = errors.New("duplicate jobId")
	ErrJobNotRunning  = errors.New("job is not running on this machine")
	errNotImplemented = errors.New("not implemented")
)

type Logger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
}

type EmptyLogger struct{} // EmptyLogger does not log anything

func (l EmptyLogger) Println(v ...interface{})               {}
func (l EmptyLogger) Printf(format string, v ...interface{}) {}
func (l EmptyLogger) Fatal(v ...interface{})                 {}
func (l EmptyLogger) Fatalf(format string, v ...interface{}) {}

// Log is this package global logger, mainly used for debugging
var Log Logger = EmptyLogger{}

func init() { rand.Seed(time.Now().UnixNano()) }
