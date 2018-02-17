# Benchmark

```
go test -bench=. -run=BenchmarkCmd -benchmem -benchtime=2s -cpuprofile=cpu.prof -memprofile=mem.prof
goos: darwin
goarch: amd64
pkg: github.com/felixhao/overlord/proxy
BenchmarkCmdSet-4    	    1000	   4147931 ns/op	 2308053 B/op	   29862 allocs/op
BenchmarkCmdGet-4    	    5000	   4283618 ns/op	 2283077 B/op	   29522 allocs/op
BenchmarkCmdMGet-4   	    2000	   5628274 ns/op	 2635941 B/op	   33914 allocs/op
PASS
ok  	github.com/felixhao/overlord/proxy	38.476s
```

# Flamegraph

CPU: [svg](/doc/images/1.0.0/bench_cpu.svg)  
![cpu](/doc/images/1.0.0/bench_cpu.png)



MEMORY: [svg](/doc/images/1.0.0/bench_mem.svg)  
![mem](/doc/images/1.0.0/bench_mem.png)
