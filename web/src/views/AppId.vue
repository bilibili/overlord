<template>
  <div>
    <div class="appid-container">
      <div class="appid-tree">
        <el-input
          placeholder="输入关键字进行过滤"
          clearable
          v-model="filterText">
        </el-input>

        <el-tree
          v-loading="appidLoading"
          class="filter-tree"
          :data="appidTree"
          :props="defaultProps"
          @node-click="handleNodeClick"
          default-expand-all
          :filter-node-method="filterNode"
          ref="appidTree">
        </el-tree>

      </div>
      <div class="appid-info" v-loading="clusterLoading">
        <div class="appid-header">
          <p>{{ appid }}</p>
          <el-button type="text" size="large" @click="dialogVisible = true">添加新的关联</el-button>
        </div>
        <div class="appid-group" v-for="(groupItem, index) in groupedClusters" :key="index">
          <div class="appid-group__title">{{ GROUP_MAP[groupItem.group] }}</div>
          <el-table :data="groupItem.clusters" border>
            <el-table-column prop="name" label="集群名字" min-width="100">
            </el-table-column>
            <el-table-column prop="cache_type" label="缓存类型">
            </el-table-column>
            <el-table-column prop="port" label="监听端口">
            </el-table-column>
            <el-table-column prop="max_memory" label="总容量">
            </el-table-column>
            <el-table-column prop="number" label="节点数">
            </el-table-column>
            <el-table-column label="详情" min-width="150">
              <template slot-scope="{ row }">
                <el-button type="text" @click="removeCorrelation(row)">解除关联</el-button>
                <!-- <el-button type="text" @click="linkToSetting(row)">编辑关联</el-button> -->
                <el-button type="text" @click="linkToCluster(row)">集群详情</el-button>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </div>
    </div>

    <el-dialog title="新关联" :visible.sync="dialogVisible" width="600px" custom-class="correlation-dialog">
      <el-input placeholder="输入集群关键字进行搜索" clearable v-model="clusterKeyword" @keyup.native="searchCluster">
      </el-input>
      <el-table :data="clusterList" border max-height="300px">
        <el-table-column prop="name" label="集群" width="150">
        </el-table-column>
        <el-table-column prop="group" label="机房">
          <template slot-scope="{ row }">
            {{ GROUP_MAP[row.group] }}
          </template>
        </el-table-column>
        <el-table-column label="详情" width="200">
          <template slot-scope="{ row }">
            <el-button type="text" @click="removeCorrelation(row)">关联到 {{ appid }}</el-button>
          </template>
        </el-table-column>
      </el-table>
      <span slot="footer" class="dialog-footer">
        <el-button @click="dialogVisible = false">取 消</el-button>
        <el-button type="primary" @click="dialogVisible = false">确 定</el-button>
      </span>
    </el-dialog>

  </div>
</template>

<script>
import GROUP_MAP from '@/constants/GROUP'

import { getClusterListByQueryApi, getAppidsApi, getAppidDetailApi, removeCorrelationApi } from '@/http/api'
import { throttle } from 'lodash'
export default {
  data () {
    return {
      GROUP_MAP,
      filterText: null,
      appidTree: [],
      defaultProps: {
        children: 'children',
        label: 'label'
      },
      appid: null,
      groupedClusters: [],
      appidLoading: true,
      clusterLoading: true,
      // dialog
      dialogVisible: false,
      clusterKeyword: null,
      clusterList: []
    }
  },
  created () {
    this.getAppids()
  },
  watch: {
    filterText (val) {
      this.$refs.appidTree.filter(val)
    },
    appid (nv) {
      this.$router.replace({ name: 'appId', query: { name: nv } })
    }
  },
  methods: {
    async getAppids () {
      this.appidLoading = true
      try {
        const { data } = await getAppidsApi({
          format: 'tree'
        })
        this.appidTree = data.items
        this.getClusterList(this.appidTree[0].children[0])
      } catch (error) {
      }
      this.appidLoading = false
    },
    filterNode (value, data) {
      if (!value) return true
      return data.label.indexOf(value) !== -1
    },
    async getClusterList ({ name }) {
      this.clusterLoading = true
      try {
        const { data } = await getAppidDetailApi(name)
        this.groupedClusters = data.grouped_clusters
        this.appid = name
      } catch (error) {
      }
      this.clusterLoading = false
    },
    handleNodeClick (data) {
      if (!data.children) {
        this.getClusterList(data)
      }
    },
    removeCorrelation ({ id, name }) {
      this.$confirm(`您将解除集群：【${name}】和 【${this.appid}】 的关联, 是否继续?`, '解除提示', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        this.confirmRemoveCorrelation(name)
      }).catch(() => {
      })
    },
    async confirmRemoveCorrelation (name) {
      try {
        const { data } = await removeCorrelationApi(name, {
          appid: this.appid
        })
        this.groupedClusters = data.grouped_clusters
        this.appid = data.name
      } catch (error) {
      }
    },
    linkToCluster ({ name }) {
      this.$router.push({ name: 'cluster', params: { name } })
    },
    linkToSetting () {

    },

    searchCluster: throttle(function searchCluster () {
      this.loadClusterData()
    }, 1000),
    async loadClusterData () {
      try {
        const { data } = await getClusterListByQueryApi()
        this.clusterList = data.items
      } catch (error) {
      }
    }
  }
}
</script>

<style lang="scss" scoped>
.appid-container {
  display: flex;
}

.appid-tree {
  flex-shrink: 0;
  width: 230px;
  min-height: 500px;
  background: #fff;
  padding: 10px;
  margin-right: 10px;

  .el-tree {
    margin: 10px 0;
  }
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

.correlation-dialog {
  .el-input {
    margin-bottom: 10px;
  }
}
</style>
