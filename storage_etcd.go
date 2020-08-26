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
	timeout0            = 5 * time.Second
	pfxLock             = "/lock"               // must not follow by "/"
	pfxJob              = "/job/"               // save jsoned job
	pfxIdxStatusNextTry = "/idx/statusNextTry/" // save jobId
)

// :param keyPfx: different retriers must have different keyPfxs
func NewEtcdStorage(cli *clientv3.Client, keyPfx string) (*EtcdStorage, error) {
	s := &EtcdStorage{cli: cli, keyPfx: keyPfx}
	//all locks created by this session have a TTL of 5s
	session, err := concurrency.NewSession(
		cli, concurrency.WithTTL(int(timeout0.Seconds())))
	if err != nil {
		return nil, err
	}
	s.mutex = concurrency.NewMutex(session, s.keyPfx+pfxLock)
	return s, nil
}

func (s EtcdStorage) TakeOrCreateJob(jobId JobId, jobFuncInputs []interface{}) (
	Job, error) {
	err := s.lock()
	if err != nil {
		return Job{}, fmt.Errorf("cannot lock: %v", err)
	}
	defer s.unlock()

	job, err := s.getJob(jobId)
	if err != nil && err != errNoKeys {
		return Job{}, err
	} else if err == nil {
		if job.Status == Running || job.Status == Stopped {
			return job, ErrJobRunningOrStopped
		}
		// remove the loaded job from queuing index
		ctx, cxl := context.WithTimeout(context.Background(), timeout0)
		s.cli.Delete(ctx, s.keyIdxStatusNextTry(job))
		cxl()
	} else { // errNoKeys, create new job
		job = Job{JobFuncInputs: jobFuncInputs,
			Id: jobId, Status: Queue, LastTried: time.Now()}
	}

	job.Status = Running
	ctx1, cxl1 := context.WithTimeout(context.Background(), timeout0)
	_, err = s.cli.Put(ctx1, s.keyJob(jobId), job.JsonMarshal())
	cxl1()
	if err != nil {
		return Job{}, fmt.Errorf("clientv3 put key %v: %v", s.keyJob(jobId), err)
	}
	ctx2, cxl2 := context.WithTimeout(context.Background(), timeout0)
	_, err = s.cli.Put(ctx2, s.keyIdxStatusNextTry(job), string(jobId))
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
		return fmt.Errorf("clientv3 get key %v: %v", job.Id, err)
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
func (s EtcdStorage) RequeueHangingJobs() (int, error) {
	err := s.lock()
	if err != nil {
		return 0, fmt.Errorf("cannot lock: %v", err)
	}
	defer s.unlock()

	beginKey := s.keyPfx + pfxIdxStatusNextTry + string(Running)
	endKey := beginKey + time.Now().Format(fmtRFC3339Mili)
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
func (s EtcdStorage) TakeJobs() ([]JobId, error) {
	err := s.lock()
	if err != nil {
		return nil, fmt.Errorf("cannot lock: %v", err)
	}
	defer s.unlock()

	beginKey := s.keyPfx + pfxIdxStatusNextTry + string(Queue)
	endKey := beginKey + "/9999-99-99"
	ctx, cxl := context.WithTimeout(context.Background(), timeout0)
	resp, err := s.cli.Get(ctx, beginKey, clientv3.WithRange(endKey))
	cxl()
	if err != nil {
		return nil, fmt.Errorf("clientv3 get range: %v", err)
	}

	updated := struct {
		val []JobId
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
				updated.val = append(updated.val, job.Id)
				updated.mu.Unlock()
			}
		}(kv)
	}
	wg.Wait()
	return updated.val, nil
}
func (s EtcdStorage) DeleteStoppedJobs() (int, error) {
	beginKey := s.keyPfx + pfxIdxStatusNextTry + string(Stopped)
	endKey := beginKey + "/9999-99-99"
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
	s.cli.Delete(ctx2, beginKey, clientv3.WithRange(endKey))
	cxl2()
	return updated.val, nil
}

func (s EtcdStorage) lock() error {
	return s.mutex.Lock(context.TODO())
}

func (s EtcdStorage) unlock() error {
	return s.mutex.Unlock(context.TODO())
}

func (s EtcdStorage) keyJob(jobId JobId) string {
	return s.keyPfx + pfxJob + string(jobId)
}

func (s EtcdStorage) keyIdxStatusNextTry(j Job) string {
	return fmt.Sprintf("%v%v/%v/%v",
		s.keyPfx+pfxIdxStatusNextTry,
		j.Status,
		j.LastTried.Add(3*j.NextDelay).Format(fmtRFC3339Mili),
		j.Id)
}

const fmtRFC3339Mili = "2006-01-02T15:04:05.999Z07:00"

var errNoKeys = errors.New("etcd get no keys")
