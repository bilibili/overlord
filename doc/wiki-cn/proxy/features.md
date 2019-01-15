# 支持的功能

## 多种协议支持

我们支持memcache的两种协议：text和binary，支持redis的两种使用模式：纯代理和cluster。  

因为B站不同业务对缓存的使用场景不同，memcache和redis的使用都非常广泛，且redis两种使用模式也分别都有。  
对memcache binary的支持是我们打算后期业务都使用binary协议，因为相对text协议来说，最大的优势是支持pipeline，可以节省很多消耗。  
虽然redis-cluster模式已经很成熟了，但相信还是有业务场景不想要冗余一倍内存，而只是单纯将redis当做一层缓存来使用。  

在proxy的配置文件中，有`cache_type`配置项，可以配置为：`memcache` | `memcache_binary` | `redis` | `redis_cluster`  
当使用`redis-cluster`模式时，proxy会将自己伪装为cluster的节点，可以支持`cluster nodes`和`cluster slots`命令，方便使用SDK如jedis的客户端无缝使用overlord-proxy。

## 哈希标签

我们支持哈希标签，默认为`{}`。与redis-cluster一致，且将这个特性扩展到四种模式都支持。

## 固定连接数

我们将proxy与缓存节点之间的连接数作为配置`node_connections`，可以自定义连接数。为了充分节省和利用资源，建议将其配置为`2`。这个值是我们经过压测和线上尝试后的最佳实践。

## 代理模式下自动踢节点

proxy内设计了`Pinger`接口，且支持配置项`ping_auto_eject`和`ping_fail_limit`，分别表示是否自动踢出节点和连续ping失败多少次后踢出。  
缓存（不是存储，默认对一致性要求较低）是可以被降级容错的，所以我们优先支持了故障节点自动踢出，快速恢复服务优先。当然，使用方也可以配置为关闭该功能。

## TODO: 多级缓存

## TODO: 缓存多写

## TODO: 冷缓存预热

## TODO: 平滑 reload 配置
