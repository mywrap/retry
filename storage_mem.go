package retry

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/petar/GoLLRB/llrb"
)

type MemoryStorage struct {
	jobs map[JobId]*JobInTree
	// Item is *JobInTree, this index must be updated on updating jobs,
	idxStatusNextTry      *llrb.LLRB
	mutex                 *sync.Mutex
	jobsFailedAllAttempts []Job
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		jobs:             make(map[JobId]*JobInTree),
		idxStatusNextTry: llrb.New(),
		mutex:            &sync.Mutex{},
	}
}
func (s *MemoryStorage) CreateJob(jobId JobId, jobFuncInputs []interface{}) (
	Job, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, found := s.jobs[jobId]
	if found {
		return Job{}, ErrDuplicateJob
	}
	job := NewJobInTree(&Job{JobFuncInputs: jobFuncInputs,
		Id: jobId, Status: Running, LastTried: time.Now()})
	s.jobs[jobId] = job
	s.idxStatusNextTry.InsertNoReplace(job)
	return *job.Job, nil
}
func (s *MemoryStorage) UpdateJob(job Job) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	oldJob, found := s.jobs[job.Id]
	if !found {
		return errors.New("jobId not found")
	}
	s.idxStatusNextTry.Delete(oldJob)
	updatedJob := NewJobInTree(&job)
	s.jobs[job.Id] = updatedJob
	s.idxStatusNextTry.InsertNoReplace(updatedJob)
	if job.Status == Stopped {
		//if job.NTries == job.
	}
	return nil
}
func (s *MemoryStorage) RequeueHangingJobs() (int, error) {
	return 0, nil
}
func (s *MemoryStorage) TakeJobs() ([]Job, error) {
	return nil, nil
}
func (s *MemoryStorage) DeleteStoppedJobs() (int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	jobs := make([]*JobInTree, 0)
	iterFunc := func(item llrb.Item) bool {
		job, _ := item.(*JobInTree)
		jobs = append(jobs, job)
		return true
	}
	s.idxStatusNextTry.AscendRange(
		NewJobInTree(&Job{Status: Stopped}),
		NewJobInTree(&Job{Status: Stopped, LastTried: time.Now().Add(99999 * time.Hour)}),
		iterFunc)
	for _, job := range jobs {
		s.idxStatusNextTry.Delete(job)
		delete(s.jobs, job.Id)
	}
	return len(jobs), nil
}

// write jobs detail to outFile and return number of jobs
func (s *MemoryStorage) ExportCSV(outFile string) (int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	rows := make([][]string, 0)
	iterFunc := func(item llrb.Item) bool {
		j, _ := item.(*JobInTree)
		row := []string{string(j.Id), string(j.Status),
			fmt.Sprintf("%v", j.NTries),
			fmt.Sprintf("%.2f", j.NextDelay.Seconds()),
			j.LastTried.Format(time.RFC3339Nano), j.LastHost,
			j.keyStatusNextTry, j.AllErrorsStr()}
		rows = append(rows, row)
		return true
	}
	s.idxStatusNextTry.AscendLessThan(NewJobInTree(&Job{Status: "ZZZ"}), iterFunc)
	file, err := os.Create(outFile)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	for _, row := range rows {
		if err := w.Write(row); err != nil {
			return 0, err
		}
	}
	w.Flush()
	return len(rows), w.Error()
}

func (s *MemoryStorage) ReadJobsRunning() ([]Job, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	runningJobs := make([]Job, 0)
	iterFunc := func(item llrb.Item) bool {
		job, _ := item.(*JobInTree)
		runningJobs = append(runningJobs, *job.Job)
		return true
	}
	s.idxStatusNextTry.AscendRange(
		NewJobInTree(&Job{Status: Running}),
		NewJobInTree(&Job{Status: Running, LastTried: time.Now().Add(99999 * time.Hour)}),
		iterFunc)
	return runningJobs, nil
}

func (s *MemoryStorage) ReadJobsFailedAllAttempts() ([]Job, error) {
	return nil, nil // TODO
}

// JobInTree must be inited by NewJobInTree
type JobInTree struct {
	*Job
	keyStatusNextTry string
}

// NewJobInTree adds keyStatusNextTry field for improving performance key compare
func NewJobInTree(job *Job) *JobInTree {
	ret := &JobInTree{Job: job}
	ret.calcKeyStatusNextTry()
	return ret
}
func (j *JobInTree) calcKeyStatusNextTry() {
	j.keyStatusNextTry = fmt.Sprintf("/memory/%v/%v/%v",
		j.Status,
		j.LastTried.Add(3*j.NextDelay).Format(fmtRFC3339Mili),
		j.Id)
}

func (j *JobInTree) Less(than llrb.Item) bool {
	job, _ := than.(*JobInTree)
	return j.keyStatusNextTry < job.keyStatusNextTry
}
