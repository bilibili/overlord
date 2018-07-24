# Overlord

## Version 1.1.0
1. add memcache binary protocol support.
2. add conf file check

## Version 1.0.0
1. compitable consist hash with twemproxy.
2. reduce object alloc (by using Pool).
3. recycle using of request and response memory.
4. batch message execute to reduce conflict of command channel.
5. using writev to reduce syscall times.
6. synchronously send and wait of message.
7. zero copy of request and response data at all.
