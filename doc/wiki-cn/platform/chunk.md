# chunk/dist 算法
chunk算法的目的是保证redis cluster部署的高可用。chunk算法节点分布必须满足如下三个条件
1. 主从不在同一个物理节点  
2. 任意一个物理节点分配的节点数少于总数的一半  
3. 尽可能部署在资源最充足的物理节点
对于非cluster的缓存集群，由于没有主从的概念，因此只需简化为dist，只需满足上述的2 3 两个条件。
同时对chunk的实现有如下约定：
1. master数量必须为偶数
2. 可用机器数必须不小于三
2. 机器节点数不能为3且master数量不能为4

## chunk 算法go实现
* 把offer根据节点mem cpu的需求转换成对应的可用资源单元

```
func mapIntoHostRes(offers []ms.Offer, mem float64, cpu float64) (hosts []*hostRes) {
// 具体实现chunk.go/mapIntoHostRes
}

type hostRes struct {
	name  string  // 机器名
	count int // 当前机器可分配的节点数
}
```

* 按每台机器可部署的节点数进行降序排序

* 填充每台机器应部署的节点数

```
func dpFillHostRes(chunks []*Chunk, disableHost map[string]struct{}, hrs []*hostRes, count int, scale int) (hosts []*hostRes) {
    for {
       // 寻找已部署该集群节点数最少的机器
		i := findMinHrs(hrs, hosts, disableHost, all, scale)
		if left == 0 {
			return
		}
		hosts[i].count += scale
		left -= scale

	}
}
```
* 根据hostRes寻找chunk组

```chunk.go/Chunks
for {
        // 找到剩余资源最多的机器
		name, count := maxHost(hrs)
		if count == 0 {
			break
		}
		m := hrmap[name]
        // 寻找与该机器chunk对最少的对端机器，组成一组chunk
		llh := findMinLink(linkTable, m)
		if hrs[llh].count < 2 {
			linkTable[llh][m]++
			linkTable[m][llh]++
			continue
		}
		llHost := hrs[llh]
		links = append(links, link{Base: name, LinkTo: llHost.name})
		linkTable[llh][m]++
		linkTable[m][llh]++
		hrs[m].count -= 2
		hrs[llh].count -= 2
	}
```

* 把chunk组转化为最终的主从分布关系

```
func links2Chunks(links []link, portsMap map[string][]int) []*Chunk {
    // ...
}
```

## 动态chunk
chunk算法实现了redis cluster集群创建时节点分布的规则计算。但是对于集群扩容，集群节点故障则没法没法的处理，为了解决这种问题，基于chunk算法，又实现了chunkAppend和chunkRecovery。

**ChunkAppend**会在原有的chunk的基础上，在不破坏原有chunk的情况下，寻找到合适的新chunk组并加入原有的chunks。保证新加入的chunk与旧chunk同时满足chunk的三个条件。

**ChunkRecovery**则会剔除故障的old host，并尝试寻找全新的host替代旧的host，如果寻找不到全新的host，则会在原先的chunk基础上，重新寻找合适的new host并将old host 的节点从新规划到合适的host上。并且依然满足chunk的约束。

## chunk 保证
根据chunk规划了redis cluster节点的分布以主从情况。但是实际redis启动的时候，可能由于种种原因发生了不希望看到的主从切换。或者其他意料不到的情况。因此为了保证最终部署情况满足chunk，还需要对部署好的集群进行chunk校验.
