# Overlord

## Version 1.3.0
1. add executor interface.
2. add pinger interface.

## Version 1.2.2
1. fix batchdone err.
2. add node reconn.

## Version 1.2.1
1. fix message and request reuse bug.

## Version 1.2.0
1. add redis protocol support.

## Version 1.1.0
1. add memcache binary protocol support.
2. add conf file check.

## Version 1.0.0
1. compitable consist hash with twemproxy.
2. reduce object alloc (by using Pool).
3. recycle using of request and response memory.
4. batch message execute to reduce conflict of command channel.
5. using writev to reduce syscall times.
6. synchronously send and wait of message.
7. zero copy of request and response data at all.
