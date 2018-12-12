// 集群类型
const TYPE_OPTIONS = [{
  name: 'Redis Cluster',
  value: 'redis_cluster',
  url: require('@/assets/redis-cluster.png'),
  desc: '如果你不知道用什么缓存的话，我们推荐选择本项。Redis Cluster 提供了比单节点更高的安全性和比 Memcache 更强、更多的第三方工具。Redis Cluster 的主从机制为您的数据保驾护航。'
}, {
  name: 'Redis',
  value: 'redis',
  url: require('@/assets/redis.png'),
  desc: '如果你有的项目有历史积累，必须要用 Redis 单节点，或者对 Redis 的稳定和安全性没有需求；并且你的运维非常抠门的话，请选择本项。'
}, {
  name: 'MEMECACHED',
  value: 'memcached',
  url: require('@/assets/memcached.png'),
  desc: 'Memcache 多线程的特性使得它特别适合于那些value超大的（例如 binary 文件）kv 存储，但是配套的同步工具比较弱，需要慎重。'
}]

// 集群型号
const SPEC_OPTIONS = [{
  name: '小型',
  active: true,
  value: '0.25c2g',
  desc: '0.5 核 1 G'
}, {
  name: '中型 ',
  value: '0.5c2g',
  desc: '1 核 2 G'
}, {
  name: '大型',
  value: '1c4g',
  desc: '1 核 4 G'
}, {
  name: '定制',
  value: 'custom'
}]

const GROUP_OPTIONS = [{
  name: '上海核心',
  value: 'sh001'
}, {
  name: '上海腾讯云',
  value: 'sh002'
}, {
  name: '上海金山云',
  value: 'sh005'
}, {
  name: '杭州下沙',
  value: 'hz001'
}, {
  name: '上海嘉定',
  value: 'sh004'
}, {
  name: '苏州新海宜',
  value: 'sz001'
}, {
  name: '广州腾讯',
  value: 'gz001'
}]

export { TYPE_OPTIONS, SPEC_OPTIONS, GROUP_OPTIONS }
