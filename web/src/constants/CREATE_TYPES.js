// 集群类型
const TYPE_OPTIONS = [{
  name: 'Redis Cluster',
  value: 'redis_cluster'
}, {
  name: 'Redis',
  value: 'redis'
}, {
  name: 'Memcache',
  value: 'memcached'
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
