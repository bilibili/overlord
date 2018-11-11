# apiserver http api design

apiserver有以下约定：

1. required 字段为加重字体
2. api 约定前缀为 `api/v1`
3. api操作的对象或者返回的对象是名词代表的资源，或者名词代表资源的子资源，名词用复数，否则名词用单数。

## POST /clusters

|name|type|description|
|----|----|-----------|
|q|string|模糊匹配查询集群名用的搜索字符串|
