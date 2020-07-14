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
          :highlight-current="true"
          :props="defaultProps"
          @node-click="handleNodeClick"
          default-expand-all
          :filter-node-method="filterNode"
          ref="appidTree">
        </el-tree>
        <el-button size="mini" type="text" @click="addIdDialogVisible = true">添加 AppId</el-button>

      </div>
      <div class="appid-info" v-loading="clusterLoading">
        <div class="appid-header">
          <p>{{ appid }}</p>
          <el-button type="text" size="large" @click="dialogVisible = true">添加关联</el-button>
        </div>
        <div class="appid-group" v-for="(groupItem, index) in groupedClusters" :key="index">
          <div class="appid-group__title">{{ groupMap[groupItem.group] }}</div>
          <el-table :data="groupItem.clusters" border max-height="500">
            <el-table-column prop="name" label="集群名称" min-width="100">
            </el-table-column>
            <el-table-column prop="cache_type" label="缓存类型" min-width="90">
            </el-table-column>
            <el-table-column prop="front_end_port" label="前端端口">
            </el-table-column>
            <el-table-column prop="max_memory" label="总容量">
              <template slot-scope="{ row }">
                {{ row.max_memory }} MB
              </template>
            </el-table-column>
            <el-table-column prop="number" label="节点数" min-width="70">
            </el-table-column>
            <el-table-column label="详情" min-width="135">
              <template slot-scope="{ row }">
                <el-button type="text" @click="removeCorrelation(row)">解除关联</el-button>
                <!-- <el-button type="text" @click="linkToSetting(row)">编辑关联</el-button> -->
                <el-button type="text" @click="linkToCluster(row)">集群详情</el-button>
              </template>
            </el-table-column>
          </el-table>
        </div>
        <div v-if="!groupedClusters.length" class="appid-group appid-group--empty" >
          暂无集群，
          <el-button type="text" size="mini" @click="dialogVisible = true">去关联</el-button>
        </div>
      </div>
    </div>

    <el-dialog title="添加关联" :visible.sync="dialogVisible" width="600px" custom-class="correlation-dialog">
      <el-input
        placeholder="输入集群关键字进行搜索"
        clearable
        v-model="clusterKeyword"
        @keyup.native="searchCluster">
      </el-input>
      <el-table :data="clusterList" v-loading="queryLoading" border max-height="300px">
        <el-table-column prop="name" label="集群" width="150">
        </el-table-column>
        <el-table-column prop="group" label="机房">
          <template slot-scope="{ row }">
            {{ groupMap[row.group] }}
          </template>
        </el-table-column>
        <el-table-column label="详情" width="230">
          <template slot-scope="{ row }">
            <el-button type="text" @click="addCorrelation(row)">关联到 {{ appid }}</el-button>
          </template>
        </el-table-column>
      </el-table>
      <span slot="footer" class="dialog-footer">
        <el-button @click="dialogVisible = false">关闭</el-button>
      </span>
    </el-dialog>

    <el-dialog title="添加 AppId" :visible.sync="addIdDialogVisible" width="400px" custom-class="correlation-dialog">
      <div class="addid-panel">
        <el-input placeholder="部门" v-model="appidForm.department" size="mini">
        </el-input>
        -
        <el-input placeholder="服务" v-model="appidForm.service" size="mini">
        </el-input>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button @click="addIdDialogVisible = false">取 消</el-button>
        <el-button type="primary" @click="submitAddAppId">确 定</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import { getAppidsApi, getAppidDetailApi, getGroupsApi, removeCorrelationApi, addCorrelationApi, addAppIdApi } from '@/http/api'
import { mapState } from 'vuex'
import { throttle } from 'lodash'

export default {
  data () {
    return {
      groupMap: {},
      filterText: null,
      appidTree: [],
      defaultProps: {
        children: 'children',
        label: 'label'
      },
      appid: null,
      groupedClusters: [],
      appidLoading: false,
      clusterLoading: false,
      // dialog
      dialogVisible: false,
      clusterKeyword: null,
      // dialog
      addIdDialogVisible: false,
      appidForm: {
        department: null,
        service: null
      }
    }
  },
  created () {
    this.getGroups()
    this.getAppids()
  },
  computed: {
    ...mapState({
      clusterList: state => state.clusters.clusterResult,
      queryLoading: state => state.clusters.queryLoading
    })
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
        this.getClusterList(this.$route.query.name || this.appidTree[0].children[0].name)
      } catch ({ error }) {
        this.$message.error(`获取失败：${error}`)
      }
      this.appidLoading = false
    },
    async getGroups () {
      try {
        const { data } = await getGroupsApi()
        this.groupMap = data.items.reduce(function (map, obj) {
          map[obj.name] = obj.name_cn
          return map
        }, {})
      } catch (_) {
        this.$message.error('分组列表获取失败')
      }
    },
    filterNode (value, data) {
      if (!value) return true
      return data.label.indexOf(value) !== -1
    },
    async getClusterList (name) {
      if (!name) return
      this.clusterLoading = true
      try {
        const { data } = await getAppidDetailApi(name)
        this.groupedClusters = data.grouped_clusters
        this.appid = name
      } catch ({ error }) {
        this.$message.error(`获取失败：${error}`)
      }
      this.clusterLoading = false
    },
    handleNodeClick (data) {
      if (!data.children) {
        this.getClusterList(data.name)
      }
    },
    removeCorrelation ({ id, name }) {
      this.$confirm(`您将解除集群【${name}】和 AppId【${this.appid}】的关联, 是否继续?`, '解除提示', {
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
        await removeCorrelationApi(name, {
          appid: this.appid
        })
        this.getClusterList(this.appid)
        this.$message.success('解除成功')
      } catch ({ error }) {
        this.$message.error(`解除失败：${error}`)
      }
    },
    addCorrelation ({ name }) {
      this.$confirm(`将关联集群【${name}】和 AppId【${this.appid}】, 是否继续?`, '提示', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'info'
      }).then(() => {
        this.confirmAddCorrelation(name)
      }).catch(() => {
      })
    },
    async confirmAddCorrelation (name) {
      try {
        await addCorrelationApi(name, {
          appid: this.appid
        })
        this.dialogVisible = false
        this.getClusterList(this.appid)
        this.$message.success('关联成功')
      } catch ({ error }) {
        this.$message.error(`关联失败：${error}`)
      }
    },
    linkToCluster ({ name }) {
      this.$router.push({ name: 'cluster', params: { name } })
    },
    // 添加 appid
    async submitAddAppId () {
      const { department, service } = this.appidForm
      if (!department && !service) {
        this.$message.warning('请填写完整')
        return
      }
      try {
        await addAppIdApi({
          appid: `${department}.${service}`
        })
        this.addIdDialogVisible = false
        this.getAppids()
        this.$message.success('添加成功')
        this.appidForm.department = null
        this.appidForm.service = null
      } catch ({ error }) {
        this.$message.error(`添加失败：${error}`)
      }
    },
    // linkToSetting () {

    // },
    searchCluster: throttle(function searchCluster () {
      this.loadClusterData()
    }, 1000),
    async loadClusterData () {
      if (!this.clusterKeyword) return
      this.$store.dispatch('clusters/getClusterResult', {
        name: this.clusterKeyword
      })
    }
  }
}
</script>

<style lang="scss" scoped>
$hint-text-color: #909399;

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

  .el-button {
    width: 100%;
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

  &--empty {
    min-height: 250px;
    color: $hint-text-color;
    display: flex;
    align-items: center;
    justify-content: center;
  }
}

.correlation-dialog {
  .el-input {
    margin-bottom: 10px;
  }

  .addid-panel {
    display: flex;
    align-items: center;

    .el-input {
      margin: 0 5px;
    }
  }
}
</style>
