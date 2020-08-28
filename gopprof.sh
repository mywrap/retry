#go test -v -run=TestRetrierMemoryStorage2 -cpuprofile=cpu.out

go test -v -run=TestEtcdStorageNewRetrier -cpuprofile=cpu.out
