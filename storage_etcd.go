package retry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mywrap/metric"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type EtcdStorage struct {
	cli    *clientv3.Client
	keyPfx string // keyPfx: /retrierName

	session   *concurrency.Session // reuse etcd session for reducing grant lease operations
	sessionMu *sync.Mutex          // protect above field "session"

	metric metric.Metric // can be nil
}

// etcd client
const (
	opCRUDTimeout = 5 * time.Second
	// session was created in maxMutexLockDur can be reused to create new mutex
	// sessionTTL > maxMutexLockDur
	sessionTTL           = 3*opCRUDTimeout + 1*time.Second
	pfxLock              = "/lock"
	pfxJob               = "/job/"               // save jsoned job
	pfxIdxStatusNextTry  = "/idx/statusNextTry/" // save jobId
	pfxFailedAllAttempts = "/jobFailedAllAttempts/"
)

// :param keyPfx: different retriers must have different keyPfxs, ex: "/retrierTest3"
func NewEtcdStorage(cliCfg EtcdClientConfig, keyPfx string) (*EtcdStorage, error) {
	cli, err := clientv3.New(clientv3.Config(cliCfg))
	if err != nil {
		return nil, fmt.Errorf("clientv3 New %#v: %v", cliCfg, err)
	}
	// clientv3_New can return nil even if the cluseter is not running so we
	// try to lock and unlock as PING
	s := &EtcdStorage{cli: cli, keyPfx: keyPfx, sessionMu: &sync.Mutex{}}
	mutex, err := s.lock()
	if err != nil {
		return nil, fmt.Errorf("try 1st lock: %v", err)
	}
	if err := s.unlock(mutex); err != nil {
		return nil, fmt.Errorf("try 1st unlock: %v", err)
	}
	return s, nil
}

func (s *EtcdStorage) lock() (mutex *concurrency.Mutex, retErr error) {
	defer func() { // sometime mutex.Lock panics
		if r := recover(); r != nil {
			retErr = fmt.Errorf("recover %v", r)
		}
	}()
	if s.metric != nil {
		s.metric.Count("lock")
		defer func(bt time.Time) { s.metric.Duration("lock", time.Since(bt)) }(time.Now())
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	var isNeedNewSession bool
	if s.session == nil {
		isNeedNewSession = true
	} else {
		select {
		case <-s.session.Done():
			isNeedNewSession = true
		default:
			isNeedNewSession = false
		}
	}
	if isNeedNewSession {
		//fmt.Println("debug create new session")
		newSession, err := concurrency.NewSession(
			s.cli, concurrency.WithTTL(int(sessionTTL.Seconds())))
		if err != nil {
			return nil, fmt.Errorf("cli NewSession WithTTL: %v", err)
		}
		s.session = newSession
	}
	mutex = concurrency.NewMutex(s.session, s.keyPfx+pfxLock)
	return mutex, mutex.Lock(context.TODO())
}

func (s *EtcdStorage) unlock(mutex *concurrency.Mutex) error {
	if s.metric != nil { // debug performance
		s.metric.Count("unlock")
		defer func(bt time.Time) { s.metric.Duration("unlock", time.Since(bt)) }(time.Now())
	}
	return mutex.Unlock(context.TODO())
}

func (s *EtcdStorage) CreateJob(jobId JobId, jobFuncInputs []interface{}) (
	Job, error) {
	if s.metric != nil { // debug performance
		s.metric.Count("CreateJob")
		defer func(bt time.Time) { s.metric.Duration("CreateJob", time.Since(bt)) }(time.Now())
	}

	if mutex, err := s.lock(); err != nil {
		return Job{}, fmt.Errorf("cannot lock: %v", err)
	} else {
		defer s.unlock(mutex)
	}

	job, err := s.getJob(jobId)
	if err != errNoKeys {
		if err != nil {
			return Job{}, err
		}
		return Job{}, ErrDuplicateJob
	}
	job = Job{JobFuncInputs: jobFuncInputs,
		Id: jobId, Status: Running, LastTried: time.Now()}
	ctx1, cxl1 := context.WithTimeout(context.Background(), opCRUDTimeout)
	_, err = s.cli.Put(ctx1, s.keyJob(jobId), job.JsonMarshal())
	cxl1()
	if err != nil {
		return Job{}, fmt.Errorf("clientv3 put key %v: %v", s.keyJob(jobId), err)
	}
	ctx2, cxl2 := context.WithTimeout(context.Background(), opCRUDTimeout)
	// TODO: handle Put idx err after successfully Put job
	s.cli.Put(ctx2, s.keyIdxStatusNextTry(job), string(jobId))
	cxl2()
	return job, nil
}

// returned error can be errNoKeys or other exceptions.
// Only read the EtcdStorage.
func (s *EtcdStorage) getJob(jobId JobId) (Job, error) {
	ctx, cxl := context.WithTimeout(context.Background(), opCRUDTimeout)
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

// UpdateJob does not lock. The locks in CreateJob and TakeJobs ensured only
// one retrier runs a job at a moment
func (s *EtcdStorage) UpdateJob(job Job) error {
	if s.metric != nil { // debug performance
		s.metric.Count("UpdateJob")
		defer func(bt time.Time) { s.metric.Duration("UpdateJob", time.Since(bt)) }(time.Now())
	}

	oldJob, err := s.getJob(job.Id)
	if err != nil && err != errNoKeys {
		return fmt.Errorf("getJob %v: %v", job.Id, err)
	}
	if err == errNoKeys {
		// should be unreachable
	} else {
		ctx, cxl := context.WithTimeout(context.Background(), opCRUDTimeout)
		s.cli.Delete(ctx, s.keyIdxStatusNextTry(oldJob))
		cxl()
	}

	ctx1, cxl1 := context.WithTimeout(context.Background(), opCRUDTimeout)
	_, err = s.cli.Put(ctx1, s.keyJob(job.Id), job.JsonMarshal())
	cxl1()
	if err != nil {
		return fmt.Errorf("clientv3 put key %v: %v", s.keyJob(job.Id), err)
	}
	ctx2, cxl2 := context.WithTimeout(context.Background(), opCRUDTimeout)
	_, err = s.cli.Put(ctx2, s.keyIdxStatusNextTry(job), string(job.Id))
	cxl2()
	if err != nil {
		return fmt.Errorf("clientv3 put key %v: %v", s.keyIdxStatusNextTry(job), err)
	}
	if job.IsFailedAllAttempts {
		ctx3, cxl3 := context.WithTimeout(context.Background(), opCRUDTimeout)
		_, err := s.cli.Put(ctx3, s.keyJobFailedAll(job.Id), job.JsonMarshal())
		cxl3()
		if err != nil {
			return fmt.Errorf("clientv3 put key %v: %v", s.keyJobFailedAll(job.Id), err)
		}
	}
	return nil
}

// hanging jobs have keyIdxStatusNextTry < time_Now
func (s *EtcdStorage) RequeueHangingJobs() (int, error) {
	if mutex, err := s.lock(); err != nil {
		return 0, fmt.Errorf("cannot lock: %v", err)
	} else {
		defer s.unlock(mutex)
	}

	beginKey := s.keyPfx + pfxIdxStatusNextTry + string(Running)
	endKey := beginKey + "/" + time.Now().Format(fmtRFC3339Mili)
	ctx, cxl := context.WithTimeout(context.Background(), opCRUDTimeout)
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
func (s *EtcdStorage) TakeJobs() ([]Job, error) {
	if mutex, err := s.lock(); err != nil {
		return nil, fmt.Errorf("cannot lock: %v", err)
	} else {
		defer s.unlock(mutex)
	}

	beginKey := s.keyPfx + pfxIdxStatusNextTry + string(Queue)
	ctx, cxl := context.WithTimeout(context.Background(), opCRUDTimeout)
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
	ctx2, cxl2 := context.WithTimeout(context.Background(), opCRUDTimeout)
	s.cli.Delete(ctx2, beginKey, clientv3.WithPrefix())
	cxl2()
	return updated.val, nil
}

// Only read the EtcdStorage.
func (s *EtcdStorage) DeleteStoppedJobs() (int, error) {
	beginKey := s.keyPfx + pfxIdxStatusNextTry + string(Stopped)
	ctx, cxl := context.WithTimeout(context.Background(), opCRUDTimeout)
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
			ctx, cxl := context.WithTimeout(context.Background(), opCRUDTimeout)
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
	ctx2, cxl2 := context.WithTimeout(context.Background(), opCRUDTimeout)
	s.cli.Delete(ctx2, beginKey, clientv3.WithPrefix())
	cxl2()
	return updated.val, nil
}

// delete all key with the storage's prefix, for testing.
// Only read the EtcdStorage.
func (s *EtcdStorage) deleteAllKey() (int, error) {
	ctx, cxl := context.WithTimeout(context.Background(), opCRUDTimeout)
	resp, err := s.cli.Delete(ctx, s.keyPfx, clientv3.WithPrefix())
	cxl()
	if err != nil {
		return 0, err
	}
	return int(resp.Deleted), nil
}

// Only read the EtcdStorage.
func (s *EtcdStorage) keyJob(jobId JobId) string {
	return s.keyPfx + pfxJob + string(jobId)
}

// Only read the EtcdStorage.
func (s *EtcdStorage) keyJobFailedAll(jobId JobId) string {
	return s.keyPfx + pfxFailedAllAttempts + string(jobId)
}

// Only read the EtcdStorage.
func (s *EtcdStorage) keyIdxStatusNextTry(j Job) string {
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

// Only read the EtcdStorage.
func (s *EtcdStorage) ReadJobsRunning() ([]Job, error) {
	beginKey := s.keyPfx + pfxIdxStatusNextTry + string(Running)
	ctx, cxl := context.WithTimeout(context.Background(), opCRUDTimeout)
	resp, err := s.cli.Get(ctx, beginKey, clientv3.WithPrefix())
	cxl()
	if err != nil {
		return nil, fmt.Errorf("clientv3 get range: %v", err)
	}
	runningJobs := make([]Job, 0)
	mu := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	for _, kv := range resp.Kvs {
		wg.Add(1)
		go func(kv *mvccpb.KeyValue) {
			defer wg.Add(-1)
			job, err := s.getJob(JobId(kv.Value))
			if err == nil {
				mu.Lock()
				runningJobs = append(runningJobs, job)
				mu.Unlock()
			}
		}(kv)
	}
	wg.Wait()
	return runningJobs, nil
}

// Only read the EtcdStorage.
func (s *EtcdStorage) ReadJobsFailedAllAttempts() ([]Job, error) {
	beginKey := s.keyPfx + pfxFailedAllAttempts
	ctx, cxl := context.WithTimeout(context.Background(), opCRUDTimeout)
	resp, err := s.cli.Get(ctx, beginKey, clientv3.WithPrefix())
	cxl()
	if err != nil {
		return nil, fmt.Errorf("clientv3 get range: %v", err)
	}
	failJobs := make([]Job, 0)
	for _, kv := range resp.Kvs {
		var job Job
		err = json.Unmarshal(kv.Value, &job)
		if err != nil { // unreachable
			return failJobs, fmt.Errorf("invalid failJobs: %v", err)
		}
		failJobs = append(failJobs, job)
	}
	return failJobs, nil
}

const fmtRFC3339Mili = "2006-01-02T15:04:05.999Z07:00"

var errNoKeys = errors.New("etcd get no keys")

// following code wraps "go.etcd.io/etcd/v3" type so projects use this package
// do not need to import "go.etcd.io/etcd/v3" (buggy go.mod)

type EtcdClientConfig = clientv3.Config
type EtcdClient = clientv3.Client

func NewEtcdClient(config EtcdClientConfig) (*EtcdClient, error) {
	return clientv3.New(config)
}

// EtcdLock must be inited by calling NewEtcdLock.
// EtcdLock is not safe to use in multiple goroutines, it should be called once
// on each machine of a cluster.
type EtcdLock struct {
	Session *concurrency.Session
	Mutex   *concurrency.Mutex
}

// if machines in a cluster call NewEtcdLock on the same "lockingKey", only one
// of them get the lock, others block until the lock holder call Unlock
func NewEtcdLock(etcdClient *EtcdClient, lockingKey string) (
	myMutex *EtcdLock, err error) {
	defer func() { // sometimes clientv3 Mutex Lock panics
		if r := recover(); r != nil {
			myMutex = nil
			err = fmt.Errorf("recover %v", r)
		}
	}()

	// session WithTTL meaning: https://github.com/etcd-io/etcd/issues/6736,
	// TLDR: if the lock holder crash, the lock will be released after the WithTTL duration
	session, err := concurrency.NewSession(etcdClient, concurrency.WithTTL(5))
	if err != nil {
		return nil, fmt.Errorf("concurrency.NewSession WithTTL: %v", err)
	}
	mutex := concurrency.NewMutex(session, lockingKey)
	err = mutex.Lock(context.TODO())
	if err != nil {
		session.Close()
		return nil, fmt.Errorf("concurrency.NewMutex: %v", err)
	}
	return &EtcdLock{Session: session, Mutex: mutex}, nil
}

func (m *EtcdLock) Unlock() error {
	err1 := m.Mutex.Unlock(context.TODO())
	err2 := m.Session.Close()
	if err2 != nil {
		return err2
	}
	if err1 != nil {
		return err1
	}
	return nil
}
