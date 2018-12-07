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

<details>
<summary> 创建一个新的集群 </summary>

#### body args
| name             | type     | description                                                        |
|------------------|----------|--------------------------------------------------------------------|
| **name**         | string   | 全局唯一不重复的集群名字                                           |
| appids           | []string | appid list 用于创建时关联appid                                     |
| **cache_type**   | string   | cache_type name(only support "memcache", "redis", "redis_cluster") |
| **spec**         | string   | 容量规格表达式例如："0.5c2g"、"1c2g"                               |
| **total_memory** | integer  | 总容量，MB单位                                                     |
| **version**      | string   | 选择redis/memcache的版本                                           |
| **group**        | string   | 精确选取机房                                                       |

#### Response

response `Job`

|name|type|description|
|----|----|-----------|
|id| string | job id |
|state|string| job state|

#### example Response

```json
{
  "id": "sh002.0000000001",
  "state": "pending",
}
```

</details>

### GET /clusters

<details>
<summary> 通过cluster name 拿到集群名列表 </summary>

#### query arguments:

|name|type|description|
|----|----|-----------|
|name|stirng| 通过 name 模糊匹配|

#### example response

```json
{
  "count": 2,
  "items": [
    {
      "name": "test-cluster1",
      "appids": ["main.pikachu", "main.zelda"],
      "state": "done",
      "cache_type": "redis_Cluster",
      "group": "sh001",
      "cpu": 1,
      "max_memory": 1024.0,
      "version": "4.0.11",
      "number": 4,
      "instances": [{
        "ip": "127.0.0.1",
        "port": 7777,
        "weight": 0,
        "alias": "",
        "state": "running"
      }, ... ]
    },
    {...}
  ]
}
```

</details>

### DELETE /clusters/:cluster_name
<details>
<summary>创建删除集群任务</summary>

#### path arguments
|name|type|description|
|----|----|-----------|
|cluster_name|string| 唯一精确匹配的 cluster_name|

#### example response

```json
{
  "id": "sh001.12213345453450",
  "state": "pending",
}
```

</details>


## Job

### GET /jobs/:job_id

<details>
<summary>按照job id 获取job状态(可用于轮询)</summary>
get the job response by given id

#### example response

```json
{
  "id": "sh001.12213345453450",
  "state": "pending"
}
```

</details>

### GET /jobs

<details>
<summary>get all jobs</summary>

#### example responses

```json
{
  "count": 2,
  "items": [{
    "id": "sh001.12313124143234",
    "state": "running"
    "param": "{..}"
  },{
    "id": "sh001.12213345453450",
    "state": "pending"
    "param": "{}"
  }]
}
```
</details>


## Specs

规格列表

### GET /specs/

<details>
<summary> 获得所有的资源选型列表 </summary>

#### example response

```json
{
   "count": 2,
   "items": ["1c2g", "2c4g"]
}
```

</details>

<!-- ### POST /specs/ -->
<!-- <details> -->
<!-- <summary>创建新的资源列表</summary> -->
<!-- </details> -->

### DELETE /specs/:spec_name

<details>
<summary>删除对应的spec</summary>

#### example response
```
{
  "message": "done"
}
```
</details>

# Appids

appid 列表

### Get /appids/

<details>
<summary>取得所有的appid</summary>

#### example respones
```
{
  "count": 2,
  "items": [{
     "name": "main.platform",
     "label": "main.platform"
     "children": [{
       "name": "main.platform.overlord",
       "label": "overlord"
     },{
       "name": "main.platform.overlord",
       "label": "discorvery"
     }]
  },{
    "name": "live.live",
    "label": "live.live",
    "children": [{
      "name": "live.live.xreward-service",
      "label": "xreward-service"
    }]
  }]
}
```
</details>

### GET /appids/:appid

<details>
<summary>获取指定的appid的详细内容（主要是集群信息）</summary>

#### path arguments

|name|type|description|
|----|----|-----------|
|appid|string| 唯一精确匹配的 appid|

#### example response

```
{
   "name": "main.platform.overlord",
   "grouped_clusters": [{
     "group": "sh001",
     "clusters": [{
      "name": "test-cluster1",
      "appids": ["main.pikachu", "main.zelda"],
      "group": "sh001",
      "state": "done",
      "cache_type": "redis_Cluster",
      "cpu": 1,
      "max_memory": 1024.0,
      "version": "4.0.11",
      "number": 1,
      "instances": [{
        "ip": "127.0.0.1",
        "port": 7777,
        "weight": 0,
        "alias": "",
        "state": "running"
      }]
     },{
       "group": "村头机房",
       "clusters": [{...}]
     }]
   }]
}
```

</details>

### DELETE /appids/:appid_name

*开源版独享*

<details>
<summary>删除对应的appid</summary>

#### path arguments

|name|type|description|
|----|----|-----------|
|appid|string| 唯一精确匹配的 appid|

#### example response

```
{
  "message": "done"
}
```
</details>


### GET /versions

<details>
<summary>提供所有的版本信息</summary>

#### example response
```json
{
  "count": 2,
  "items": [{
    "cache_type": "redis",
    "versions": ["4.0.11", "3.2.8"]
  },{
    "cache_type": "memcache",
    "versions": ["1.5.0"]
  }]
}
```

</details>



# V1 版本API整理

## PATCH /:cluster_name/instances/:instance_id


<details>
<summary>修改权重</summary>


### body arguments

```
{
    "weight": 12,
}
```

#### example response

```
{
  "message": "done",
}
```
</details>
