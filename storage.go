package retry

import (
	"fmt"
	"sync"
	"time"

	"github.com/petar/GoLLRB/llrb"
)

type MemoryStorage struct {
	jobs map[JobId]*Job
	// Item is *Job, this idx will be updated on job updating,
	idxStatusNextTry *llrb.LLRB
	mutex            *sync.Mutex
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		jobs:             make(map[JobId]*Job),
		idxStatusNextTry: llrb.New(),
		mutex:            &sync.Mutex{},
	}
}
func (s *MemoryStorage) TakeOrCreateJob(jobId JobId, jobFuncInputs []interface{}) (
	Job, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	job, found := s.jobs[jobId]
	if !found {
		job = &Job{
			JobFuncInputs: jobFuncInputs, Id: jobId, Status: Queue,
			NTries: 0, NextDelay: 0, LastTried: time.Now(),
			Errors: make([]string, 0)}
	}
	s.idxStatusNextTry.Delete(job)

	job.Status = Running
	s.jobs[jobId] = job
	s.idxStatusNextTry.InsertNoReplace(job)
	return *job, nil
}
func (s *MemoryStorage) UpdateJob(job Job) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	oldJob, found := s.jobs[job.Id]
	if found {
		s.idxStatusNextTry.Delete(oldJob)
	}

	s.jobs[job.Id] = &job
	s.idxStatusNextTry.InsertNoReplace(&job)
	return nil
}
func (s *MemoryStorage) RequeueHangingJobs() (int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	hangingJobs := make([]*Job, 0)
	iterFunc := func(item llrb.Item) bool {
		job, _ := item.(*Job)
		hangingJobs = append(hangingJobs, job)
		return true
	}
	s.idxStatusNextTry.AscendRange(
		&Job{Status: Running},
		&Job{Status: Running, LastTried: time.Now(), NextDelay: 0},
		iterFunc)
	for _, job := range hangingJobs {
		s.idxStatusNextTry.Delete(job)
		job.Status = Queue
		s.idxStatusNextTry.InsertNoReplace(job)
	}
	return len(hangingJobs), nil
}
func (s *MemoryStorage) TakeJobs() ([]Job, error) {
	return nil, errNotImplemented // TODO MemoryStorage TakeJobs
}

func (j *Job) Less(than llrb.Item) bool {
	job, _ := than.(*Job)
	return j.keyStatusNextTry() < job.keyStatusNextTry()
}

func (j *Job) keyStatusNextTry() string {
	return fmt.Sprintf("/memory/%v/%v",
		j.Status, j.LastTried.Add(3*j.NextDelay).Format(time.RFC3339Nano))
}
