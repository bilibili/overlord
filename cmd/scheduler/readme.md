### sheduler

#### 设计概览

1. 连接到etcd/zk 获取 `/overlord/task` 目录下未完成的task并监听目录实时获取api-server下发的任务信息并保存到本地任务队列
2. 注册到mesos master 并订阅mesos event。
3. 监听event_offer事件，并判断当前任务队列是否有未完成任务
4. oferr resources 是否符合task任务资源描述需求 （判断依据 mem cpu balance）
5. 接受offer，并下发executor任务给agent
6. 监听agent回复的event_update事件
7. event为running时，则表示任务启动成功并且正常运行
8. event为failed时，scheduler 需要重新调度重新分配task
9. 当running数为task需要部署的instance数时，表示所有任务部署成功，scheduler更新task状态为完成并写入将状态写入ectd