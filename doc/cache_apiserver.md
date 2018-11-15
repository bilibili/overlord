# apiserver http api design

apiserver有以下约定：

1. required 字段为加重字体。列表类型参数，加重表示至少有一个
2. api 约定前缀为 `api/v1`
3. api操作的对象或者返回的对象是名词代表的资源，或者名词代表资源的子资源，名词用复数，否则名词用单数。
4. 复数资源GET请求均可进行分页，分页页码 `pn` ，分页大小 `pc`，均在 url query arguments 里。
5. 所有资源能使用 name 直接定位的均使用 name

# APIs

## Clusters

表示集群列表

### POST /clusters

创建一个新的集群

#### body args
|name|type|description|
|----|----|-----------|
|**name**|string|全局唯一不重复的集群名字|
|appids|[]string|appid list 用于创建时关联appid|
|**cache_type**|string|cache_type name(only support "memcache", "redis", "redis_cluster")|
|**spec**|string|容量规格表达式例如："0.5c2g"、"1c2g"|
|**master_num**|integer|主节点数量为必选|
|**version**|string|选择redis version|

### GET /clusters

#### response

Reply with Cluster list. Each cluster was defined as:


| name | type   | description              |
|------|--------|--------------------------|
| name | string | 全局不重复的唯一集群名字 |
|      |        |                          |


#### query arguments:

|name|type|description|
|----|----|-----------|
|appid|string| appid模糊匹配 |
|name|stirng| 通过 name 模糊匹配|

### DELETE /clusters/:cluster_name

#### path arguments
|name|type|description|
|----|----|-----------|
|cluster_name|string| 唯一精确匹配的 cluster_name|

### PUT /clusters/:clusters_name

#### path arguments
|name|type|description|
|----|----|-----------|
|cluster_name|string| 唯一精确匹配的 cluster_name|

#### body args
|name|type|description|
|----|----|-----------|
|**name**|string|全局唯一不重复的集群名字|
|appids|[]string|appid list 用于创建时关联appid|
|**cache_type**|string|cache_type name(only support "memcache", "redis", "redis_cluster")|
|**spec**|string|容量规格表达式例如："0.5c2g"、"1c2g"|
|**master_num**|integer|主节点数量为必选|
|**version**|string|选择redis version|

# Specs

规格列表

### GET /specs/

获得所有的资源选型列表

### POST /specs/

创建新的资源列表

### DELETE /specs/:spec_name


# Appids

appid 列表

### Get /appids/

### POST /appids/

*开源版独享*

### DELETE /appids/:appid_name

*开源版独享*
