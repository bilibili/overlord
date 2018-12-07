<template>
  <div>
    <div class="appid-container">
      <div class="appid-tree">
        <el-input
          placeholder="输入关键字进行过滤"
          v-model="filterText">
        </el-input>
        <el-tree
          class="filter-tree"
          :data="data2"
          :props="defaultProps"
          @node-click="handleNodeClick"
          default-expand-all
          :filter-node-method="filterNode"
          ref="tree2">
        </el-tree>

      </div>
      <div class="appid-info">
        <div class="appid-header">
          <p>APPID: main.platform.discovery</p>
          <el-button type="text" size="large">添加新的关联</el-button>
        </div>
        <div class="appid-group">
          <div class="appid-group__title">嘉定机房</div>
          <el-table :data="clusterList" border>
            <el-table-column prop="name" label="集群名字" width="150">
            </el-table-column>
            <el-table-column prop="cache_type" label="缓存类型">
            </el-table-column>
            <el-table-column prop="port" label="监听端口">
            </el-table-column>
            <el-table-column prop="max_memory" label="总容量">
            </el-table-column>
            <el-table-column prop="number" label="节点数">
            </el-table-column>
            <el-table-column label="详情" width="200">
              <template slot-scope="{ row }">
                <el-button type="text" @click="linkToSetting(row)">删除</el-button>
                <el-button type="text" @click="linkToSetting(row)">编辑关联</el-button>
                <el-button type="text" @click="linkToSetting(row)">集群详情</el-button>
              </template>
            </el-table-column>
          </el-table>
        </div>
        <div class="appid-group">
          <div class="appid-group__title">上海机房</div>
          <el-table :data="clusterList" border>
            <el-table-column prop="name" label="集群名字" width="150">
            </el-table-column>
            <el-table-column prop="cache_type" label="缓存类型">
            </el-table-column>
            <el-table-column prop="port" label="监听端口">
            </el-table-column>
            <el-table-column prop="max_memory" label="总容量">
            </el-table-column>
            <el-table-column prop="number" label="节点数">
            </el-table-column>
            <el-table-column label="详情" width="200">
              <template slot-scope="{ row }">
                <el-button type="text" @click="linkToSetting(row)">删除</el-button>
                <el-button type="text" @click="linkToSetting(row)">编辑关联</el-button>
                <el-button type="text" @click="linkToCluster(row)">集群详情</el-button>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  data () {
    return {
      filterText: '',
      data2: [{
        id: 1,
        label: 'main.platform',
        children: [{
          id: 4,
          label: 'discovery'
        }]
      }, {
        id: 2,
        label: 'main.community',
        children: [{
          id: 5,
          label: 'account'
        }, {
          id: 6,
          label: 'reply.service'
        }]
      }],
      defaultProps: {
        children: 'children',
        label: 'label'
      },
      clusterList: [{
        'appids': ['test.app1', 'test.app2'],
        'name': 'test-cluster',
        'max_memory': 2048,
        'cache_type': 'redis',
        'number': 20,
        'port': 1277
      }, {
        'appids': ['test.app1', 'test.app2'],
        'name': 'test-cluster',
        'max_memory': 2048,
        'cache_type': 'redis',
        'number': 20,
        'port': 1277
      }]

    }
  },
  watch: {
    filterText (val) {
      this.$refs.tree2.filter(val)
    }
  },
  methods: {
    filterNode (value, data) {
      if (!value) return true
      return data.label.indexOf(value) !== -1
    },
    handleNodeClick (data) {
      console.log(data)
    },
    linkToCluster ({ name }) {
      this.$router.push({ name: 'cluster', params: { name } })
    }
  }
}
</script>

<style lang="scss" scoped>
.appid-container {
  display: flex;
}
.appid-tree {
  width: 300px;
  min-height: 500px;
  background: #fff;
  padding: 10px;
  margin-right: 10px;
}
.appid-info {
  width: 100%;
  min-height: 500px;
}
.appid-header {
  height: 40px;
  width: 100%;
  background: #fff;
  font-size: 16px;
  font-weight: bold;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 10px;
}
.appid-group {
  padding: 10px;
  margin: 10px 0;
  background: #fff;
  &__title {
    font-size: 15px;
    font-weight: bold;
    margin: 10px 0 5px 2px;
  }
}
</style>
