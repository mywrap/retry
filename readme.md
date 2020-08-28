# Stateful retry

A lib for retrying jobs, retry state will be saved to a persistent storage
([etcd](https://github.com/etcd-io/etcd)) so others machine (or 
restarted machine) can continue the jobs after a crash.  
Support a in-memory storage for simple usage.  
Inspired by [avast/retry-go](https://github.com/avast/retry-go).

## Usage

````go
func main() {
    callUnreliableResource := func(inputs ...interface{}) error {
        rand.Seed(time.Now().UnixNano())
        log.Printf("about to callUnreliableResource")
        if rand.Intn(100) < 90 {
            return errors.New("resourceUnavailable")
        }
        return nil
    }
    cfg := &retry.Config{MaxAttempts: 15, DelayType: retry.ExpBackOffDelay,
        Delay: 100 * time.Millisecond, MaxJitter: 0 * time.Millisecond}
    retrier := retry.NewRetrier(callUnreliableResource, cfg, etcdSto, nil)

    // checks all running jobs in storage, if a job lastAttempted too long time '
    // ago (maybe the runner crashed) then run the job
    go retrier.LoopTakeQueueJobs()

    // run a new job
    runResult, err := retrier.Do("callUnreliableResourceInput049")
    if err != nil {
        log.Fatalf("error retrier Do: %v", err)
    }
    log.Printf("errors of attempts: %#v", runResult.Errors)
}

""""output:
14:09:56.997393 example.go:25: inited etcd storage
14:09:57.010163 example.go:29: about to callUnreliableResource
14:09:57.117150 example.go:29: about to callUnreliableResource
14:09:57.247232 example.go:29: about to callUnreliableResource
14:09:57.478571 example.go:29: about to callUnreliableResource
14:09:57.805953 example.go:29: about to callUnreliableResource
14:09:58.336354 example.go:29: about to callUnreliableResource
14:09:59.164013 example.go:29: about to callUnreliableResource
14:10:00.492603 example.go:29: about to callUnreliableResource
14:10:02.622006 example.go:29: about to callUnreliableResource
14:10:02.657347 example.go:46: errors of attempts: []string{"resourceUnavailable", "resourceUnavailable", "resourceUnavailable", "resourceUnavailable", "resourceUnavailable", "resourceUnavailable", "resourceUnavailable", "resourceUnavailable", ""}
````
Detail in [example.go](./example/example.go) and test files.

## Setup etcd cluster

Example deployment in sub dir `etcd` of [https://github.com/daominah/hello_docker].

## go mod etcd issue

Must import `go.etcd.io/etcd/v3/clientv3` and

````bash
go mod init
go get -v go.etcd.io/etcd/v3/clientv3@master
````

[github issue](https://github.com/etcd-io/etcd/issues/11749#issuecomment-620893935)
