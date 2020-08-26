package retry

/*
import (
	"context"
	"fmt"
	"time"

	"github.com/petar/GoLLRB/llrb"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
)

const timeout0 = 5 * time.Second

type EtcdStorage struct {
	cli    *clientv3.Client
	mutex  *concurrency.Mutex // global mutex for all jobs
	keyPfx string
}

// :param retrierName: will be used as etcd key prefix
func NewEtcdStorage(cli *clientv3.Client, retrierName string) (*EtcdStorage, error) {
	s := &EtcdStorage{cli: cli, keyPfx: retrierName}
	lockName := fmt.Sprintf("%v/lock", s.keyPfx)
	//all locks created by this session have a TTL of 5s
	session, err := concurrency.NewSession(
		cli, concurrency.WithTTL(int(timeout0.Seconds())))
	if err != nil {
		return nil, err
	}
	s.mutex = concurrency.NewMutex(session, lockName)
	return s, nil
}

func (s EtcdStorage) Lock() error {
	return s.mutex.Lock(context.TODO())
}

func (s EtcdStorage) Unlock() error {
	return s.mutex.Unlock(context.TODO())
}

func (s EtcdStorage) TakeOrCreateJob(jobId JobId, jobFuncInputs []interface{}) (
	Job, error) {
	err := s.Lock()
	if err != nil {
		return Job{}, fmt.Errorf("cannot lock: %v", err)
	}
	defer s.Unlock()

	var job Job
	var found bool
	ctx, cxl := context.WithTimeout(context.Background(), timeout0)
	resp, err := s.cli.Get(ctx, string(jobId))
	cxl()
	if err != nil && err != rpctypes.ErrEmptyKey{
		return Job{}, fmt.Errorf("clientv3 get key %v: %v", jobId,err)
	}
	log.Printf("ret: %#v", ret)
	for _, kv := range ret.Kvs {
		log.Printf("k: %s, v: %s", kv.Key, kv.Value)
	}

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
	}
	job.Status = Running
	job.calcKeyStatusNextTry()
	s.jobs[jobId] = job
	s.idxStatusNextTry.InsertNoReplace(job)
	return *job.Job, nil
}
func (EtcdStorage) UpdateJob(job Job) error {
	oldJob, found := s.jobs[job.Id]
	if found {
		s.idxStatusNextTry.Delete(oldJob)
	}
	updatedJob := NewJobInTree(&job)
	s.jobs[job.Id] = updatedJob
	s.idxStatusNextTry.InsertNoReplace(updatedJob)
	return nil
}
func (s EtcdStorage) RequeueHangingJobs() (int, error) {
	return 0, nil
}
func (s EtcdStorage) TakeJobs() ([]Job, error) {
	return nil, nil
}
func (s EtcdStorage) DeleteStoppedJobs() (int, error) {
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
*/