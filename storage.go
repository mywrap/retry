package retry

import (
	"github.com/petar/GoLLRB/llrb"
)

type MemoryStorage struct {
	jobs                   map[JobId]*Job
	idxStatusNextAttempted *llrb.LLRB
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }
func (s *MemoryStorage) TakeOrCreateJob(jobId JobId, jobFuncInputs []interface{}) (
	*Job, error) {
	return nil, errNotImplemented // TODO MemoryStorage ReadJob
}
func (s *MemoryStorage) TakeJobs() ([]*Job, error) {
	return nil, errNotImplemented // TODO MemoryStorage TakeJobs
}
func (s *MemoryStorage) UpdateJob(*Job) error {
	return errNotImplemented // TODO MemoryStorage UpdateJob
}
func (s *MemoryStorage) StopJob(jobId JobId) (*Job, error) {
	return nil, errNotImplemented
}
func (s *MemoryStorage) RequeueHangingJobs() (int, error) {
	return 0, errNotImplemented // TODO MemoryStorage RequeueCrashJobs
}
