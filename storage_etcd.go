package retry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/v3/clientv3"
	"go.etcd.io/etcd/v3/clientv3/concurrency"
	"go.etcd.io/etcd/v3/mvcc/mvccpb"
)

type EtcdStorage struct {
	cli    *clientv3.Client
	mutex  *concurrency.Mutex // global mutex for all jobs
	keyPfx string             // keyPfx: /retrierName
}

// etcd client
const (
	timeout0            = 25 * time.Second
	pfxLock             = "/lock"
	pfxJob              = "/job/"               // save jsoned job
	pfxIdxStatusNextTry = "/idx/statusNextTry/" // save jobId
)

// :param keyPfx: different retriers must have different keyPfxs
func NewEtcdStorage(cli *clientv3.Client, keyPfx string) (*EtcdStorage, error) {
	s := &EtcdStorage{cli: cli, keyPfx: keyPfx}
	//all locks created by this session have a TTL of timeout0
	session, err := concurrency.NewSession(
		cli, concurrency.WithTTL(int(timeout0.Seconds())))
	if err != nil {
		return nil, err
	}
	s.mutex = concurrency.NewMutex(session, s.keyPfx+pfxLock)

	if err := s.lock(); err != nil {
		return nil, fmt.Errorf("error try 1st lock: %v", err)
	}
	if err := s.unlock(); err != nil {
		return nil, fmt.Errorf("error try 1st unlock: %v", err)
	}

	return s, nil
}

func (s EtcdStorage) CreateJob(jobId JobId, jobFuncInputs []interface{}) (
	Job, error) {
	if err := s.lock(); err != nil {
		return Job{}, fmt.Errorf("cannot lock: %v", err)
	}
	defer s.unlock()

	job, err := s.getJob(jobId)
	if err != errNoKeys {
		if err == nil {
			return Job{}, ErrDuplicateJob
		}
		return Job{}, err
	}
	job = Job{JobFuncInputs: jobFuncInputs,
		Id: jobId, Status: Running, LastTried: time.Now()}
	ctx1, cxl1 := context.WithTimeout(context.Background(), timeout0)
	_, err = s.cli.Put(ctx1, s.keyJob(jobId), job.JsonMarshal())
	cxl1()
	if err != nil {
		return Job{}, fmt.Errorf("clientv3 put key %v: %v", s.keyJob(jobId), err)
	}
	ctx2, cxl2 := context.WithTimeout(context.Background(), timeout0)
	// TODO: handle Put idx err after successfully Put job
	s.cli.Put(ctx2, s.keyIdxStatusNextTry(job), string(jobId))
	cxl2()
	return job, nil
}

// returned error can be errNoKeys or other exceptions
func (s EtcdStorage) getJob(jobId JobId) (Job, error) {
	ctx, cxl := context.WithTimeout(context.Background(), timeout0)
	resp, err := s.cli.Get(ctx, s.keyJob(jobId))
	cxl()
	if err != nil {
		return Job{}, fmt.Errorf("clientv3 get key %v: %v", jobId, err)
	}
	if len(resp.Kvs) < 1 {
		return Job{}, errNoKeys
	}
	var job Job
	err = json.Unmarshal(resp.Kvs[0].Value, &job)
	if err != nil { // unreachable
		return Job{}, fmt.Errorf("invalid job: %v, %v", jobId, err)
	}
	return job, nil
}

func (s EtcdStorage) UpdateJob(job Job) error {
	oldJob, err := s.getJob(job.Id)
	if err != nil && err != errNoKeys {
		return fmt.Errorf("getJob %v: %v", job.Id, err)
	}
	if err == nil {
		ctx, cxl := context.WithTimeout(context.Background(), timeout0)
		s.cli.Delete(ctx, s.keyIdxStatusNextTry(oldJob))
		cxl()
	}

	ctx1, cxl1 := context.WithTimeout(context.Background(), timeout0)
	_, err = s.cli.Put(ctx1, s.keyJob(job.Id), job.JsonMarshal())
	cxl1()
	if err != nil {
		return fmt.Errorf("clientv3 put key %v: %v", s.keyJob(job.Id), err)
	}
	ctx2, cxl2 := context.WithTimeout(context.Background(), timeout0)
	_, err = s.cli.Put(ctx2, s.keyIdxStatusNextTry(job), string(job.Id))
	cxl2()
	return nil
}

// hanging jobs have keyIdxStatusNextTry < time_Now
func (s EtcdStorage) RequeueHangingJobs() (int, error) {
	if err := s.lock(); err != nil {
		return 0, fmt.Errorf("cannot lock: %v", err)
	}
	defer s.unlock()

	beginKey := s.keyPfx + pfxIdxStatusNextTry + string(Running)
	endKey := beginKey + "/" + time.Now().Format(fmtRFC3339Mili)
	ctx, cxl := context.WithTimeout(context.Background(), timeout0)
	resp, err := s.cli.Get(ctx, beginKey, clientv3.WithRange(endKey))
	cxl()
	if err != nil {
		return 0, fmt.Errorf("clientv3 get range: %v", err)
	}

	updated := struct {
		val int
		mu  sync.Mutex
	}{}
	wg := &sync.WaitGroup{}
	for _, kv := range resp.Kvs {
		wg.Add(1)
		go func(kv *mvccpb.KeyValue) {
			defer wg.Add(-1)
			jobId := JobId(kv.Value)
			job, err := s.getJob(jobId)
			if err != nil {
				return
			}
			//fmt.Printf("debug hanging job: %s, %#v, %v\n", kv.Key, job, job.LastTried)
			job.Status = Queue
			err = s.UpdateJob(job)
			if err != nil {
				return
			}
			updated.mu.Lock()
			updated.val++
			updated.mu.Unlock()
		}(kv)
	}
	wg.Wait()
	return updated.val, nil
}
func (s EtcdStorage) TakeJobs() ([]Job, error) {
	if err := s.lock(); err != nil {
		return nil, fmt.Errorf("cannot lock: %v", err)
	}
	defer s.unlock()

	beginKey := s.keyPfx + pfxIdxStatusNextTry + string(Queue)
	ctx, cxl := context.WithTimeout(context.Background(), timeout0)
	resp, err := s.cli.Get(ctx, beginKey, clientv3.WithPrefix())
	cxl()
	if err != nil {
		return nil, fmt.Errorf("clientv3 get range: %v", err)
	}

	updated := struct {
		val []Job
		mu  sync.Mutex
	}{}
	wg := &sync.WaitGroup{}
	for _, kv := range resp.Kvs {
		wg.Add(1)
		go func(kv *mvccpb.KeyValue) {
			defer wg.Add(-1)
			jobId := JobId(kv.Value)
			job, err := s.getJob(jobId)
			if err != nil {
				return
			}
			job.Status = Running
			err = s.UpdateJob(job)
			if err == nil {
				updated.mu.Lock()
				updated.val = append(updated.val, job)
				updated.mu.Unlock()
			}
		}(kv)
	}
	wg.Wait()
	ctx2, cxl2 := context.WithTimeout(context.Background(), timeout0)
	s.cli.Delete(ctx2, beginKey, clientv3.WithPrefix())
	cxl2()
	return updated.val, nil
}
func (s EtcdStorage) DeleteStoppedJobs() (int, error) {
	beginKey := s.keyPfx + pfxIdxStatusNextTry + string(Stopped)
	ctx, cxl := context.WithTimeout(context.Background(), timeout0)
	resp, err := s.cli.Get(ctx, beginKey, clientv3.WithPrefix())
	cxl()
	if err != nil {
		return 0, fmt.Errorf("clientv3 get range: %v", err)
	}
	updated := struct {
		val int
		mu  sync.Mutex
	}{}
	wg := &sync.WaitGroup{}
	for _, kv := range resp.Kvs {
		wg.Add(1)
		go func(kv *mvccpb.KeyValue) {
			defer wg.Add(-1)
			jobId := JobId(kv.Value)
			ctx, cxl := context.WithTimeout(context.Background(), timeout0)
			_, err := s.cli.Delete(ctx, s.keyJob(jobId))
			cxl()
			if err == nil {
				updated.mu.Lock()
				updated.val++
				updated.mu.Unlock()
			}
		}(kv)
	}
	wg.Wait()
	ctx2, cxl2 := context.WithTimeout(context.Background(), timeout0)
	s.cli.Delete(ctx2, beginKey, clientv3.WithPrefix())
	cxl2()
	return updated.val, nil
}

// delete all key with the storage's prefix, for testing
func (s EtcdStorage) deleteAllKey() (int, error) {
	ctx, cxl := context.WithTimeout(context.Background(), timeout0)
	resp, err := s.cli.Delete(ctx, s.keyPfx, clientv3.WithPrefix())
	cxl()
	if err != nil {
		return 0, err
	}
	return int(resp.Deleted), nil
}

func (s EtcdStorage) lock() (retErr error) {
	defer func() { // sometime panic on clientv3 Lock tryAcquire
		if r := recover(); r != nil {
			retErr = fmt.Errorf("recover %v", r)
		}
	}()
	return s.mutex.Lock(context.TODO())
}

func (s EtcdStorage) unlock() error {
	return s.mutex.Unlock(context.TODO())
}

func (s EtcdStorage) keyJob(jobId JobId) string {
	return s.keyPfx + pfxJob + string(jobId)
}

func (s EtcdStorage) keyIdxStatusNextTry(j Job) string {
	nextHeartbeat := 3 * j.NextDelay
	if nextHeartbeat < 5*time.Second {
		// prevent running jobs can be treated as hanging if NextDelay is too small
		nextHeartbeat = 5 * time.Second
	}
	return fmt.Sprintf("%v%v/%v/%v",
		s.keyPfx+pfxIdxStatusNextTry,
		j.Status,
		j.LastTried.Add(nextHeartbeat).Format(fmtRFC3339Mili),
		j.Id)
}

const fmtRFC3339Mili = "2006-01-02T15:04:05.999Z07:00"

var errNoKeys = errors.New("etcd get no keys")
