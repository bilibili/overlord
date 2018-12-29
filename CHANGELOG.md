# Overlord

## Version 1.5.3
1. fix pipe read when one err.

## Version 1.5.2
1. max redirects 5.

## Version 1.5.1
1. reset sub message only in need.

## Version 1.5.0
1. refactor message pipeline.
2. non-persistent connection for cluster redirect.

## Version 1.4.0
1. add redis cluster support.

## Version 1.3.2
1. change round-chan to race-chan. 

## Version 1.3.1
1. hot fix  reconn. 

## Version 1.3.0
1. add executor interface.
2. add pinger interface.
3. add MsgBatchAlloctor for one node WR.

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
