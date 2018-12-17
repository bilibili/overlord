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
        <!-- <el-tag :type="stateMap[clusterData.state]">
          <i v-if="clusterData.state === 'waiting'"></i>集群创建中...
        </el-tag> -->
      </div>
      <div class="cluster-info">
        <div>
          <p>名称: <span class="cluster-info__name">{{ clusterData.name }}</span></p>
          <p>类型: <span>{{ clusterData.cache_type || '--' }}</span></p>
          <p>容量: <span>{{ clusterData.max_memory * clusterData.number || '--' }} MB</span></p>
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
          <p>组名: <span>{{ GROUP_MAP[clusterData.group] }}</span></p>
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
              label="别名">
              <template slot-scope="{ row }">
                {{ row.alias || '--' }}
              </template>
            </el-table-column>
            <el-table-column
              label="权重"
              width="150">
              <template slot-scope="{ row }">
                <div v-if="row.weightInfo.type === 'view'" class="instance-weight-item">
                  {{ row.weight }}
                  <i v-if="clusterData.state !== 'waiting'" class="el-icon-edit-outline edit-weight-icon" @click="editInstanceWeight(row)"></i>
                </div>
                <div v-if="row.weightInfo.type === 'edit'" class="instance-weight-item">
                  <el-input class="table-mini-input" v-model="row.weightInfo.value" placeholder="weight" size="mini" type="number"></el-input>
                  <i class="el-icon-success edit-weight-icon" @click="saveInstanceWeight(row)"></i>
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
            <!-- TODO(feature): 二期开放 -->
            <!-- <el-table-column label="操作" width="200">
              <template slot-scope="{ row }">
                <el-button type="text" @click="linkToSetting(row)">重启</el-button>
                <el-button type="text" @click="linkToSetting(row)">开关</el-button>
                <el-button type="text" @click="linkToSetting(row)">删除</el-button>
                <el-button type="text" @click="linkToSetting(row)">监控</el-button>
              </template>
            </el-table-column> -->
          </el-table>
      </div>
    </div>

    <div v-loading="loading" class="cluster-panel">
      <div class="cluster-header">
        <span class="cluster-header__title">集群操作（前方高能!!!）</span>
      </div>
      <div class="cluster-danger">
        <div class="cluster-danger__item">
          <p>扩容：我不管我就是要扩容我的集群我不管你必须得让我扩容 </p>
          <el-button :disabled="clusterData.state === 'waiting'" type="danger" icon="el-icon-rank">扩容</el-button>
        </div>
        <div class="cluster-danger__item">
          <p>再平衡: 我想要尝试 rebalance 一下  </p>
          <el-button :disabled="clusterData.state === 'waiting'" type="danger" icon="el-icon-refresh">再平衡</el-button>
        </div>
        <div class="cluster-danger__item">
          <p>删除: 请看我的坚定的眼神(๑•̀ㅂ•́)و我就是要删掉这个集群( *・ω・)✄╰ひ╯</p>
          <el-button :disabled="clusterData.state === 'waiting'" type="danger" icon="el-icon-delete">删除</el-button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { getClusterDetailApi, patchInstanceWeightApi } from '@/http/api'
import GROUP_MAP from '@/constants/GROUP'

export default {
  data () {
    return {
      GROUP_MAP,
      multipleSelection: [],
      clusterData: [],
      loading: true,
      weightInfo: {
        value: null,
        type: 'view'
      },
      stateMap: {
        running: 'success',
        waiting: 'warning',
        error: 'danger'
      },
      timer: null

    }
  },
  created () {
    this.getClusterData()
  },
  methods: {
    async getClusterData () {
      if (!this.$route.params.name) return
      this.loading = true
      try {
        const { data } = await getClusterDetailApi(this.$route.params.name)
        this.clusterData = data
        this.clusterData.instances.forEach(item => {
          this.$set(item, 'weightInfo', {
            value: item.weight,
            type: 'view'
          })
        })
        if (this.clusterData.state === 'waiting') {
          this.timer = setTimeout(() => {
            this.getClusterData()
          }, 5000) // 1 min
        }
      } catch (error) {
      }
      this.loading = false
    },
    handleSelectionChange (val) {
      this.multipleSelection = val
    },
    editInstanceWeight (item) {
      item.weightInfo.type = 'edit'
    },
    async saveInstanceWeight (item) {
      const { weightInfo, ip, port } = item
      try {
        await patchInstanceWeightApi(this.clusterData.name, `${ip}:${port}`, {
          weight: Number(weightInfo.value)
        })
        item.weightInfo.type = 'view'
        this.$message.success('修改成功')
        this.getClusterData()
      } catch (error) {
        this.$message.error('errors')
      }
    }
  }
}
</script>

<style lang="scss">
$hint-text-color: #909399;

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
</style>
