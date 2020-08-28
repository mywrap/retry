# Stateful retry

A lib for retrying jobs, retry state will be saved to a persistent storage
([etcd](https://github.com/etcd-io/etcd)) so others machine (or 
restarted machine) can continue the jobs after a crash.  
Support a in-memory storage for simple usage.  
Inspired by [avast/retry-go](https://github.com/avast/retry-go).

## Usage

````go
// TODO
````
Detail in [example.go](./example/example.go).

## Setup etcd cluster

Example in sub dir `etcd` of [https://github.com/daominah/hello_docker].

## go mod etcd issue

Must import `go.etcd.io/etcd/v3/clientv3` and

````bash
go mod init
go get -v go.etcd.io/etcd/v3/clientv3@master
````

[github issue](https://github.com/etcd-io/etcd/issues/11749#issuecomment-620893935)
