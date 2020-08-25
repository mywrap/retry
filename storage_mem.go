package retry

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/petar/GoLLRB/llrb"
)

type MemoryStorage struct {
	jobs map[JobId]*JobInTree
	// Item is *JobInTree, this index must be updated on updating jobs,
	idxStatusNextTry *llrb.LLRB
	mutex            *sync.Mutex
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		jobs:             make(map[JobId]*JobInTree),
		idxStatusNextTry: llrb.New(),
		mutex:            &sync.Mutex{},
	}
}
func (s *MemoryStorage) TakeOrCreateJob(jobId JobId, jobFuncInputs []interface{}) (
	Job, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	job, found := s.jobs[jobId]
	if found {
		if job.Status == Running {
			return Job{}, ErrJobRan
		}
		if job.Status == Stopped {
			return Job{}, ErrJobStopped
		}
		s.idxStatusNextTry.Delete(job)
	} else { // not found, create new job
		job = NewJobInTree(&Job{
			JobFuncInputs: jobFuncInputs, Id: jobId, Status: Queue,
			NTries: 0, NextDelay: 0, LastTried: time.Now(),
			Errors: make([]string, 0), mutex: &sync.Mutex{}})
	}
	job.Status = Running
	job.calcKeyStatusNextTry()
	s.jobs[jobId] = job
	s.idxStatusNextTry.InsertNoReplace(job)
	return *job.Job, nil
}
func (s *MemoryStorage) UpdateJob(job Job) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	oldJob, found := s.jobs[job.Id]
	if found {
		s.idxStatusNextTry.Delete(oldJob)
	}
	updatedJob := NewJobInTree(&job)
	s.jobs[job.Id] = updatedJob
	s.idxStatusNextTry.InsertNoReplace(updatedJob)
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
		NewJobInTree(&Job{Status: Stopped,
			LastTried: time.Now().Add(999 * time.Hour), NextDelay: 0}), iterFunc)
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

// JobInTree must be inited by NewJobInTree
type JobInTree struct {
	*Job
	keyStatusNextTry string // for improving performance key compare
}

// wrap calcKeyStatusNextTry
func NewJobInTree(job *Job) *JobInTree {
	ret := &JobInTree{Job: job}
	ret.calcKeyStatusNextTry()
	return ret
}
func (j *JobInTree) calcKeyStatusNextTry() {
	j.keyStatusNextTry = fmt.Sprintf("/memory/%v/%v/%v",
		j.Status,
		j.LastTried.Add(3*j.NextDelay).Format("2006-01-02T15:04:05.999Z07:00"),
		j.Id)
}

func (j *JobInTree) Less(than llrb.Item) bool {
	job, _ := than.(*JobInTree)
	return j.keyStatusNextTry < job.keyStatusNextTry
}
