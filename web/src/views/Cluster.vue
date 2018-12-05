<template>
  <div class="cluster-page">
    <div class="cluster-panel">
      <div class="cluster-header">
        <span class="cluster-header__title">集群信息</span>
      </div>
      <div class="cluster-detail cluster-info">
        <div>
          <p>名称: <span>test-cluster</span></p>
          <p>类型: <span>Redis Cluster</span></p>
          <p>容量: <span>20G</span></p>
        </div>
        <div>
          <p>单节点容量: <span>2G</span></p>
          <p>主节点数量: <span>10</span></p>
          <p>从节点数量: <span>10</span></p>
        </div>
        <div>
          <p>集群状态: <span>良好</span></p>
          <p>组名: <span>Group1</span></p>
          <p>监控连接: <span>http://1212312/asdasd</span></p>
        </div>
      </div>
    </div>
    <div class="cluster-panel">
      <div class="cluster-header">
        <span class="cluster-header__title">关联的 APPID 列表</span>
      </div>
      <div class="cluster-detail cluster-appid">
        <p>main.community.pikachu</p>
        <p>pika.chu.live.pika.pika</p>
        <p>kachu.kachu.kachu</p>
      </div>
    </div>
    <div class="cluster-panel">
      <div class="cluster-header">
        <span class="cluster-header__title">节点列表</span>
      </div>
      <div class="cluster-detail cluster-nodes">
        <div class="cluster-nodes__header">
          <el-button type="primary" size="mini" plain>批量重启</el-button>
          <el-button type="success" size="mini" plain>批量启动</el-button>
          <el-button type="danger" size="mini" plain>批量删除</el-button>
        </div>
          <el-table
            ref="multipleTable"
            :data="tableData3"
            border
            @selection-change="handleSelectionChange">
            <el-table-column
              type="selection"
              width="55">
            </el-table-column>
            <el-table-column
              label="地址"
              width="120">
              <template slot-scope="scope">{{ scope.row.date }}</template>
            </el-table-column>
            <el-table-column
              prop="name"
              label="alias"
              width="120">
            </el-table-column>
            <el-table-column
              prop="address"
              label="权重"
              show-overflow-tooltip>
            </el-table-column>
            <el-table-column
              prop="address"
              label="角色"
              show-overflow-tooltip>
            </el-table-column>
            <el-table-column
              prop="address"
              label="状态"
              show-overflow-tooltip>
            </el-table-column>
            <el-table-column label="操作" width="200">
              <template slot-scope="{ row }">
                <el-button type="text" @click="linkToSetting(row)">重启</el-button>
                <el-button type="text" @click="linkToSetting(row)">开关</el-button>
                <el-button type="text" @click="linkToSetting(row)">删除</el-button>
                <el-button type="text" @click="linkToSetting(row)">监控</el-button>
              </template>
            </el-table-column>
          </el-table>
      </div>
    </div>

    <div class="cluster-panel">
      <div class="cluster-header">
        <span class="cluster-header__title">集群操作（前方高能！）</span>
      </div>
      <div class="cluster-detail cluster-danger">
        <div class="cluster-danger__item">
          <p>扩容：我不管我就是要扩容我的集群我不管你必须得让我扩容 </p>
          <el-button type="danger">扩容</el-button>
        </div>
        <div class="cluster-danger__item">
          <p>再平衡: 我想要尝试 rebalance 一下  </p>
          <el-button type="danger">再平衡</el-button>
        </div>
        <div class="cluster-danger__item">
          <p>删除: 请看我的坚定的眼神(๑•̀ㅂ•́)و我就是要删掉这个集群( *・ω・)✄╰ひ╯</p>
          <el-button type="danger">删除</el-button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  data () {
    return {
      multipleSelection: [],
      tableData3: [{
        date: '2016-05-03',
        name: '王小虎',
        address: '上海市普陀区金沙江路 1518 弄'
      }]
    }
  },
  methods: {
    handleSelectionChange (val) {
      this.multipleSelection = val
    }
  }
}
</script>

<style lang="scss">
.cluster-page {
  padding: 5px 24px;
}
.cluster-panel {
  margin:  0 0 20px 0;
  background: #fff;
  .cluster-header {
    height: 40px;
    padding: 0 10px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-bottom: 1px solid #e9f2f7;
    font-weight: bold;
    font-size: 14px;
  }
  .cluster-detail {
  }
  .cluster-info {
    padding: 12px;
    display: flex;
    justify-content: space-between;
    font-size: 13px;
    > div p {
      margin: 10px 0;
    }
  }
  .cluster-appid {
    padding: 12px;
    & > p {
      margin: 10px 0;
    }
  }
  .cluster-nodes {
    padding: 12px;
    &__header {
      display: flex;
      margin-bottom: 10px;
    }
  }
  .cluster-danger {
    border: 1px solid #f56c6c;
    border-radius: 0 0 3px 3px;
    &__item {
      display: flex;
      align-items: center;
      justify-content: space-between;
      // margin: 10px 0;
      padding: 8px;
      border-bottom: 1px solid #eee;
      > p {
        font-size: 13px;
        margin: 10px;
      }
    }
  }
}
</style>
