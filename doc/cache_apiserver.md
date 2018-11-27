# apiserver http api design

apiserver有以下约定：

1. required 字段为加重字体。列表类型参数，加重表示至少有一个
2. api 约定前缀为 `api/v1`(可以后期通过nginx区分)
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

#### Response

response `Job`

|name|type|description|
|----|----|-----------|
|id| string | job id |
|state|string| job state|

### GET /clusters


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

## Job

### GET /jobs/:job_id

get the job response by given id

### GET /jobs

get all jobs.

## Specs

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



# V1 版本API整理

## GET /search

### query args
|name|type|description|
|----|----|-----------|
|**q**|string|搜索集群名字和appid列表里的关键字|
|cache_type|string|搜索的时候指定集群类型|

examples: 

1. /search?q=baka&cache_type=redis
2. /search?q=chu

### Response

example response:

```
[
    {
        "appids": ["test.app1", "test.app2"], 
        "name": "test-cluster", 
        "max_memory": 2048, 
        "cache_type": "redis",
        "number": 20, "port": 1277
    },
    ...
]
```
## DELETE /clusters/:cluster_name

### path arguments

|name|type|description|
|----|----|-----------|
|cluster_name|string| 唯一精确匹配的 cluster_name|

### example response

```
{
    "name": "test-cluster",
    "max_memory": 2048,
    "appids": ["abc", "def", "hij"],
    "cpu": "1",
    "state": "done",
    "Number": "20",
    "Instances": [{
        "ip": "127.0.0.1",
        "port": "12000",
        "state": 123,
        "weight": 1,
    }],
    "version": "0.1.1"
}
```

## PATCH /:cluster_name/instances/:instance_id


### body arguments

```
{
    "weight": 12,
}
```
