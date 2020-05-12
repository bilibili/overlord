<template>
  <div class="cluster-page">
    <el-breadcrumb separator="/" class="breadcrumb">
      <el-breadcrumb-item><a href="#" @click="$router.back()">返回</a></el-breadcrumb-item>
      <el-breadcrumb-item>{{ clusterData.name }}</el-breadcrumb-item>
    </el-breadcrumb>
    <div v-loading="loading" class="cluster-panel">
      <div class="cluster-header">
        <span class="cluster-header__title">集群信息</span>
        <el-tag :type="stateMap[clusterData.state]">
          <i v-if="clusterData.state === 'waiting'" class="el-icon-loading"></i>{{ clusterData.state }}
        </el-tag>
      </div>
      <div class="cluster-info">
        <div>
          <p>名称: <span class="cluster-info__name">{{ clusterData.name }}</span></p>
          <p>类型: <span>{{ clusterData.cache_type || '--' }}</span></p>
          <p>容量: <span>{{ clusterData.max_memory * clusterData.number || '--' }} MB</span></p>
          <p>前端端口: <span>{{ clusterData.front_end_port || '--' }}</span></p>
        </div>
        <div>
          <p>单节点容量: <span>{{ clusterData.max_memory }} MB</span></p>
          <p>主节点数量: <span>{{ clusterData.number }}</span></p>
          <p>从节点数量: <span>{{ clusterData.cache_type === 'redis_cluster' ? clusterData.number : 0 }}</span></p>
        </div>
        <div>
          <p>集群版本: <span>
            <el-tag size="mini">{{ clusterData.version || '--' }}</el-tag>
          </span>
          </p>
          <p>组名: <span>{{ groupMap[clusterData.group] }}</span></p>
          <p>监控连接: <span><a target="_blank" :href="clusterData.monitor">去查看</a></span></p>
        </div>
      </div>
    </div>
    <div v-loading="loading" class="cluster-panel">
      <div class="cluster-header">
        <span class="cluster-header__title">关联的 AppId 列表</span>
      </div>
      <div v-if="clusterData.appids && clusterData.appids.length" class="cluster-appid">
        <p v-for="(appid, index) in clusterData.appids" :key="index" >
          <router-link :to="{ name: 'appId', query: { name: appid } }">{{ appid }}</router-link>
        </p>
      </div>
      <div v-else class="cluster-appid hint">
        暂无数据
      </div>
    </div>
    <div v-loading="loading" class="cluster-panel">
      <div class="cluster-header">
        <span class="cluster-header__title">节点列表</span>
      </div>
      <div class="cluster-instances">
        <!-- TODO(feature): 二期开放 -->
        <!-- <div v-if="clusterData.instances && clusterData.instances.length" class="cluster-instances__header">
          <el-button type="primary" size="mini" plain>批量重启</el-button>
          <el-button type="success" size="mini" plain>批量启动</el-button>
          <el-button type="danger" size="mini" plain>批量删除</el-button>
        </div> -->
          <el-table
            ref="multipleTable"
            :data="clusterData.instances"
            border
            max-height="400"
            @selection-change="handleSelectionChange">
            <!-- TODO(feature): 二期开放 -->
            <!-- <el-table-column
              type="selection"
              width="55">
            </el-table-column> -->
            <el-table-column
              type="index"
              width="50">
            </el-table-column>
            <el-table-column
              label="IP"
              min-width="100">
              <template slot-scope="scope">{{ scope.row.ip }}</template>
            </el-table-column>
            <el-table-column
              prop="port"
              label="端口"
              min-width="80">
            </el-table-column>
            <el-table-column
              v-if="clusterData.cache_type === 'redis_cluster'"
              prop="role"
              label="角色"
              min-width="80">
              <template slot-scope="{ row }">
                <el-tag :type="row.role === 'master' ? 'warning' : 'info'">{{ row.role }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column
              v-if="clusterData.cache_type !== 'redis_cluster'"
              label="别名">
              <template slot-scope="{ row }">
                {{ row.alias || '--' }}
              </template>
            </el-table-column>
            <el-table-column
              v-if="clusterData.cache_type !== 'redis_cluster'"
              label="权重"
              width="150">
              <template slot-scope="{ row, $index }">
                <div v-if="row.weightInfo.type === 'view'" class="instance-weight-item">
                  {{ row.weight }}
                  <i v-if="clusterData.state !== 'waiting'" class="el-icon-edit-outline edit-weight-icon" @click="editInstanceWeight(row, $index)"></i>
                </div>
                <div v-if="row.weightInfo.type === 'edit'" class="instance-weight-item">
                  <el-input class="table-mini-input"
                    :value="row.weightInfo.value"
                    @input="updateInstance($event, $index)"
                    placeholder="weight"
                    size="mini"
                    type="number"></el-input>
                  <i class="el-icon-success edit-weight-icon" @click="saveInstanceWeight(row, $index)"></i>
                </div>
              </template>
            </el-table-column>
            <el-table-column
              prop="state"
              label="状态"
              width="80">
              <template slot-scope="{ row }">
                <el-tag :type="stateMap[row.state]">
                  <i v-if="row.state === 'waiting'" class="el-icon-loading"></i>{{ row.state }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="80">
              <template slot-scope="{ row }">
                <el-button type="text" @click="restartInstance(row)">重启</el-button>
                <!-- TODO(feature): 二期开放 -->
                <!-- <el-button type="text" @click="linkToSetting(row)">开关</el-button>
                <el-button type="text" @click="linkToSetting(row)">删除</el-button>
                <el-button type="text" @click="linkToSetting(row)">监控</el-button> -->
              </template>
            </el-table-column>
          </el-table>
      </div>
    </div>

    <div v-loading="loading" class="cluster-panel">
      <div class="cluster-header">
        <span class="cluster-header__title">集群操作（前方高能!!!）</span>
      </div>
      <div class="cluster-danger">
        <!-- TODO(feature): 二期开放 -->
        <!-- <div class="cluster-danger__item">
          <p>扩容：我不管我就是要扩容我的集群我不管你必须得让我扩容 </p>
          <el-button :disabled="clusterData.state === 'waiting'" type="danger" icon="el-icon-rank">扩容</el-button>
        </div>
        <div class="cluster-danger__item">
          <p>再平衡: 我想要尝试 rebalance 一下  </p>
          <el-button :disabled="clusterData.state === 'waiting'" type="danger" icon="el-icon-refresh">再平衡</el-button>
        </div> -->
        <div class="cluster-danger__item">
          <p>删除: 请看我的坚定的眼神(๑•̀ㅂ•́)و我就是要删掉这个集群( *・ω・)✄╰ひ╯</p>
          <el-button @click="deleteClusterDialogVisible = true"
            :disabled="clusterData.state === 'waiting'"
            type="danger"
            icon="el-icon-delete">删除</el-button>
        </div>
      </div>
    </div>

    <el-dialog title="你确定删除集群吗？" :visible.sync="deleteClusterDialogVisible" width="400px" custom-class="delete-dialog">
      <div class="delete-dialog__tips">
        <b><i class="el-icon-warning"></i>删除集群之前请先撤销所有与本集群关联的 AppId</b>
        <p><i class="el-icon-warning"></i>该操作无法撤销，将永久删除集群 {{  clusterData.name }} 及其节点等数据。
        请输入您要删除的集群名称进行确认：</p>
      </div>
      <el-input v-model.trim="confirmClusterName"></el-input>
      <span slot="footer" class="dialog-footer">
        <el-button @click="deleteClusterDialogVisible = false">取 消</el-button>
        <el-button type="danger" @click="confirmDeleteCluster" :disabled="confirmClusterName !== clusterData.name">确认删除集群</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import { patchInstanceWeightApi, deleteClusterApi, restartInstanceApi, getGroupsApi } from '@/http/api'
import { mapState } from 'vuex'

// mapGetters
export default {
  data () {
    return {
      groupMap: {},
      multipleSelection: [],
      stateMap: {
        running: 'success',
        waiting: 'warning',
        error: 'danger'
      },
      timer: null,
      deleteClusterDialogVisible: false,
      confirmClusterName: null

    }
  },
  computed: {
    ...mapState({
      clusterData: state => state.clusters.clusterDetail,
      loading: state => state.clusters.loading
    })
  },
  created () {
    this.getGroups()
    this.getClusterData()
  },
  methods: {
    updateInstance (value, index) {
      this.$store.dispatch('clusters/updateInstance', {
        changeType: 'value',
        index,
        weightValue: value
      })
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
    async getClusterData () {
      if (!this.$route.params.name) return
      this.$store.dispatch('clusters/getClusterDetail', {
        name: this.$route.params.name
      })
      if (this.clusterData.state === 'waiting') {
        this.timer = setTimeout(() => {
          this.getClusterData()
        }, 5000)
      }
    },
    handleSelectionChange (val) {
      this.multipleSelection = val
    },
    editInstanceWeight (item, index) {
      this.$store.dispatch('clusters/updateInstance', {
        changeType: 'display',
        index,
        item,
        newType: 'edit'
      })
    },
    async saveInstanceWeight (item, index) {
      this.$store.dispatch('clusters/getClusterDetail', {
        name: this.$route.params.name
      })
      const { weightInfo, ip, port } = item
      try {
        await patchInstanceWeightApi(this.clusterData.name, `${ip}:${port}`, {
          weight: Number(weightInfo.value)
        })
        this.$store.dispatch('clusters/updateInstance', {
          index,
          item,
          newType: 'view'
        })
        this.$message.success('修改成功')
        this.getClusterData()
      } catch ({ error }) {
        this.$message.error(error)
      }
    },
    async confirmDeleteCluster () {
      try {
        await deleteClusterApi(this.confirmClusterName)
        this.$message.success('删除成功')
        this.$router.back()
      } catch ({ error }) {
        this.$message.error(`删除失败：${error}`)
      }
    },
    async restartInstance ({ ip, port }) {
      try {
        await restartInstanceApi(this.clusterData.name, `${ip}:${port}`)
        this.$message.success('重启成功')
      } catch ({ error }) {
        this.$message.error(`重启失败：${error}`)
      }
    }
  },
  beforeRouteLeave (to, from, next) {
    if (this.timer) {
      clearInterval(this.timer)
    }
    next()
  }
}
</script>

<style lang="scss">
$hint-text-color: #909399;
$daner-color: #f56c6c;

.cluster-page {
  padding: 5px 24px;
}
.breadcrumb {
  margin: 10px 0;
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
  .cluster-info {
    padding: 12px;
    display: flex;
    justify-content: space-between;
    font-size: 13px;
    > div p {
      margin: 10px 0;
    }
    > div span {
      margin: 0 5px;
    }
    &__name {
      font-weight: bold;
    }
  }
  .cluster-appid {
    padding: 12px;
    font-size: 14px;
    & > p {
      margin: 10px 0;
    }
  }
  .cluster-instances {
    padding: 12px;
    &__header {
      display: flex;
      margin-bottom: 10px;
    }
    .instance-weight-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .edit-weight-icon {
      font-size: 16px;
      margin-left: 10px;
      cursor: pointer;
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

.hint {
  color: $hint-text-color;
}

.delete-dialog {
  &__tips {
    margin: 0 0 10px 0;
    line-height: 24px;
  }
  .el-icon-warning {
    color: $daner-color;
    margin-right: 10px;
  }
}
</style>
