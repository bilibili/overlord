package create

var memcacheScriptTpl = `
#!/bin/bash
/data/lib/memcache/{{.Version}}/bin/memcached -u root -p {{.Port}} -A -l 0.0.0.0 -m {{.MaxMemory}} -t {{.Thread}}`

var redisconfTpl = `
bind 0.0.0.0
protected-mode no
port {{.Port}}
tcp-backlog 65535
timeout 0
tcp-keepalive 300
daemonize no
supervised no
pidfile redis.pid
loglevel notice
logfile /data/{{.Port}}/redis.log
databases 16
always-show-logo yes
stop-writes-on-bgsave-error no
rdbcompression yes
rdbchecksum yes
dir /data/{{.Port}}/
slave-serve-stale-data yes
slave-read-only yes
repl-diskless-sync no
repl-diskless-sync-delay 5
repl-disable-tcp-nodelay no
slave-priority 100
rename-command CONFIG "BILI-SUPER-CONFIG"
rename-command KEYS "BILI-SUPER-KEYS"
maxmemory {{.MaxMemoryInBytes}}
maxmemory-policy allkeys-lru
lazyfree-lazy-eviction no
lazyfree-lazy-expire no
lazyfree-lazy-server-del no
slave-lazy-flush no
appendonly no
appendfilename "appendonly.aof"
appendfsync everysec
no-appendfsync-on-rewrite no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb
aof-load-truncated yes
aof-use-rdb-preamble no
lua-time-limit 5000
cluster-enabled no
#cluster-config-file nodes.conf
#cluster-node-timeout 15000
slowlog-log-slower-than 10000
slowlog-max-len 1024
latency-monitor-threshold 0
notify-keyspace-events ""
hash-max-ziplist-entries 512
hash-max-ziplist-value 64
list-max-ziplist-size -2
list-compress-depth 0
set-max-intset-entries 512
zset-max-ziplist-entries 128
zset-max-ziplist-value 64
hll-sparse-max-bytes 3000
activerehashing yes
client-output-buffer-limit normal 0 0 0
client-output-buffer-limit slave 256mb 64mb 60
client-output-buffer-limit pubsub 32mb 8mb 60
hz 10
aof-rewrite-incremental-fsync yes`

var redisClusterConfTpl = `
bind 0.0.0.0
protected-mode no
port {{.Port}}
tcp-backlog 65535
timeout 0
tcp-keepalive 300
daemonize no
supervised no
pidfile redis.pid
loglevel notice
logfile /data/{{.Port}}/redis.log
databases 16
always-show-logo yes
stop-writes-on-bgsave-error no
rdbcompression yes
rdbchecksum yes
dir /data/{{.Port}}/
slave-serve-stale-data yes
slave-read-only yes
repl-diskless-sync no
repl-diskless-sync-delay 5
repl-disable-tcp-nodelay no
slave-priority 100
rename-command CONFIG "BILI-SUPER-CONFIG"
rename-command KEYS "BILI-SUPER-KEYS"
maxmemory {{.MaxMemoryInBytes}}
maxmemory-policy allkeys-lru
lazyfree-lazy-eviction no
lazyfree-lazy-expire no
lazyfree-lazy-server-del no
slave-lazy-flush no
appendonly no
appendfilename "appendonly.aof"
appendfsync everysec
no-appendfsync-on-rewrite no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb
aof-load-truncated yes
aof-use-rdb-preamble no
lua-time-limit 5000
cluster-enabled yes
cluster-config-file /data/{{.Port}}/nodes.conf
cluster-node-timeout 15000
slowlog-log-slower-than 10000
slowlog-max-len 1024
latency-monitor-threshold 0
notify-keyspace-events ""
hash-max-ziplist-entries 512
hash-max-ziplist-value 64
list-max-ziplist-size -2
list-compress-depth 0
set-max-intset-entries 512
zset-max-ziplist-entries 128
zset-max-ziplist-value 64
hll-sparse-max-bytes 3000
activerehashing yes
client-output-buffer-limit normal 0 0 0
client-output-buffer-limit slave 256mb 64mb 60
client-output-buffer-limit pubsub 32mb 8mb 60
hz 10
aof-rewrite-incremental-fsync yes
`
