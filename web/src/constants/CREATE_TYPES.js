// 集群类型
const TYPE_OPTIONS = [{
  name: 'Redis Cluster',
  value: 'redis_cluster'
}, {
  name: 'Redis',
  value: 'redis'
}, {
  name: 'Memcache',
  value: 'memcache'
}]

// 集群型号
const SPEC_OPTIONS = [{
  name: '小型',
  value: '0.25c2g'
}, {
  name: '中型',
  value: '0.5c2g'
}, {
  name: '大型',
  value: '1c4g'
}, {
  name: '定制',
  value: 'custom'
}]

export { TYPE_OPTIONS, SPEC_OPTIONS }
