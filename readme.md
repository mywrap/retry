# Stateful retry

A lib for retrying jobs, retry state will be saved to a persistent storage
([etcd](https://github.com/etcd-io/etcd)) so others instant (or 
restarted instant) can continue the jobs after a crash.  
Support a in-memory storage for simple usage.  
Inspired by [avast/retry-go](https://github.com/avast/retry-go).

## Usage

````go
func main() {
	s := template.DemoFunc0(1, 2)
	fmt.Println("sum:", s) // sum: 3
}
````
Detail in [example.go](./example/example.go).
