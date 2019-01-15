# 性能测试

## 配置情况

#### 机器配置
Linux 3.16.0-4-amd64 Debian 3.16.7 x86_64 GNU/Linux  
Intel(R) Xeon(R) CPU E5-2620 v4 @ 2.10GHz -- (32core 128G)  

#### 语言版本
Go version 1.11.3 linux/amd64

#### 缓存节点
* memcache: 10个memcache节点，overlord-proxy与每个节点配置2个固定长连接
* redis: 24个redis节点，overlord-proxy与每个节点配置2个固定长连接
* redis-cluster: 分别10个master和slave，overlord-proxy与每个节点配置2个固定长连接

## 压测工具
[memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark)

***压测工具使用单独物理机器***

## 压测结果

#### 压测结果1

```shell
# 压测结果1：对memcache进行压测，开40个线程，每个线程100个连接，每个连接发送10W个命令，单个数据32B，不用pipeline
memtier_benchmark -s $host -p $port -P memcache_text -t 40 -c 100 -n 100000 --hide-histogram --pipeline=1

[RUN #1] Preparing benchmark client...
[RUN #1] Launching threads now...
[RUN #1 100%, 456 secs]  0 threads:   399800000 ops, 1184942 (avg:  875371) ops/sec, 36.45MB/sec (avg: 26.93MB/sec),  3.37 (avg:  4.56) msec latency

40        Threads
100       Connections per thread
100000    Requests per thread

ALL STATS
========================================================================
Type        Ops/sec     Hits/sec   Misses/sec      Latency       KB/sec
------------------------------------------------------------------------
Sets       79413.58          ---          ---      4.57300      5419.95
Gets      794127.09     33517.76    760609.34      4.56300     22101.65
Waits          0.00          ---          ---      0.00000          ---
Totals    873540.67     33517.76    760609.34      4.56400     27521.60
```

#### 压测结果2

```shell
# 压测结果2：对memcache进行压测，开40个线程，每个线程100个连接，每个连接发送10W个命令，单个数据32B，每10个请求一波pipeline
memtier_benchmark -s $host -p $port -P memcache_text -t 40 -c 100 -n 100000 --hide-histogram --pipeline=10

[RUN #1] Preparing benchmark client...
[RUN #1] Launching threads now...
[RUN #1 100%, 166 secs]  0 threads:   399800000 ops, 5733340 (avg: 2394324) ops/sec, 176.83MB/sec (avg: 73.67MB/sec),  6.98 (avg: 16.69) msec latency

40        Threads
100       Connections per thread
100000    Requests per thread

ALL STATS
========================================================================
Type        Ops/sec     Hits/sec   Misses/sec      Latency       KB/sec
------------------------------------------------------------------------
Sets      214623.70          ---          ---     17.05000     14648.00
Gets     2146213.40     90585.32   2055628.08     16.65300     59732.06
Waits          0.00          ---          ---      0.00000          ---
Totals   2360837.10     90585.32   2055628.08     16.68900     74380.06
```

#### 压测结果3

```shell
# 压测结果3：对redis进行压测，开40个线程，每个线程100个连接，每个连接发送10W个命令，单个数据32B，不用pipeline
memtier_benchmark -s $host -p $port -P redis -t 40 -c 100 -n 100000 --hide-histogram --pipeline=1

[RUN #1] Preparing benchmark client...
[RUN #1] Launching threads now...
[RUN #1 100%, 489 secs]  0 threads:   399800000 ops, 1225820 (avg:  817118) ops/sec, 45.64MB/sec (avg: 30.49MB/sec),  3.26 (avg:  4.89) msec latency

40        Threads
100       Connections per thread
100000    Requests per thread

ALL STATS
========================================================================
Type        Ops/sec     Hits/sec   Misses/sec      Latency       KB/sec
------------------------------------------------------------------------
Sets       74016.92          ---          ---      4.91000      5702.17
Gets      740161.01      4999.05    735161.96      4.88700     25407.61
Waits          0.00          ---          ---      0.00000          ---
Totals    814177.92      4999.05    735161.96      4.88900     31109.79
```

#### 压测结果4

```shell
# 压测结果4：对redis进行压测，开40个线程，每个线程100个连接，每个连接发送10W个命令，单个数据32B，每10个请求一波pipeline
memtier_benchmark -s $host -p $port -P redis -t 40 -c 100 -n 100000 --hide-histogram --pipeline=10

[RUN #1] Preparing benchmark client...
[RUN #1] Launching threads now...
[RUN #1 100%, 189 secs]  0 threads:   399800000 ops, 3861742 (avg: 2104996) ops/sec, 143.96MB/sec (avg: 78.55MB/sec), 10.36 (avg: 18.99) msec latency

40        Threads
100       Connections per thread
100000    Requests per thread

ALL STATS
========================================================================
Type        Ops/sec     Hits/sec   Misses/sec      Latency       KB/sec
------------------------------------------------------------------------
Sets      189373.65          ---          ---     19.08200     14589.11
Gets     1893715.68     12790.17   1880925.51     18.97600     65005.85
Waits          0.00          ---          ---      0.00000          ---
Totals   2083089.33     12790.17   1880925.51     18.98600     79594.97
```

#### 压测结果5

```shell
# 压测结果5：对redis-cluster进行压测，开40个线程，每个线程100个连接，每个连接发送10W个命令，单个数据32B，不用pipeline
memtier_benchmark -s $host -p $port -P redis -t 40 -c 100 -n 100000 --hide-histogram --pipeline=1

[RUN #1] Preparing benchmark client...
[RUN #1] Launching threads now...
[RUN #1 100%, 477 secs]  0 threads:   399800000 ops, 1527090 (avg:  837501) ops/sec, 56.96MB/sec (avg: 31.35MB/sec),  2.62 (avg:  4.77) msec latency

40        Threads
100       Connections per thread
100000    Requests per thread

ALL STATS
========================================================================
Type        Ops/sec     Hits/sec   Misses/sec      Latency       KB/sec
------------------------------------------------------------------------
Sets       76019.60          ---          ---      4.78900      5856.46
Gets      760187.67      7760.00    752427.67      4.76800     26195.07
Waits          0.00          ---          ---      0.00000          ---
Totals    836207.27      7760.00    752427.67      4.77000     32051.53
```

#### 压测结果6

```shell
# 压测结果6：对redis-cluster进行压测，开40个线程，每个线程100个连接，每个连接发送10W个命令，单个数据32B，每10个请求一波pipeline
memtier_benchmark -s $host -p $port -P redis -t 40 -c 100 -n 100000 --hide-histogram --pipeline=10

[RUN #1] Preparing benchmark client...
[RUN #1] Launching threads now...
[RUN #1 100%, 195 secs]  0 threads:   399800000 ops, 5709367 (avg: 2046968) ops/sec, 213.06MB/sec (avg: 76.62MB/sec),  7.03 (avg: 19.52) msec latency

40        Threads
100       Connections per thread
100000    Requests per thread

ALL STATS
========================================================================
Type        Ops/sec     Hits/sec   Misses/sec      Latency       KB/sec
------------------------------------------------------------------------
Sets      184982.76          ---          ---     19.63900     14250.84
Gets     1849807.26     18882.85   1830924.41     19.51300     63741.94
Waits          0.00          ---          ---      0.00000          ---
Totals   2034790.02     18882.85   1830924.41     19.52400     77992.79
```
