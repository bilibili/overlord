<template>
  <div class="home-page">
    <div class="search-panel">
      <div class="search-panel__header">Cluster Search</div>
      <div class="search-panel__input">
        <el-input v-model="clusterKeyword" placeholder="请输入集群名关键字进行搜索" size="large" @keyup.native="searchCluster">
          <i slot="prefix" class="el-input__icon el-icon-search"></i>
        </el-input>
      </div>
    </div>
    <transition name="slide-fade" mode="out-in" appear>
      <div v-if="clusterList.length" class="search-result">
        <el-table :data="clusterList" border>
          <el-table-column prop="name" label="集群名称" min-width="100">
          </el-table-column>
          <el-table-column prop="cache_type" label="缓存类型">
          </el-table-column>
          <el-table-column prop="max_memory" label="总容量">
            <template slot-scope="{ row }">
              {{ row.max_memory }} MB
            </template>
          </el-table-column>
          <el-table-column prop="number" label="节点数">
          </el-table-column>
          <el-table-column label="详情" width="150">>
            <template slot-scope="{ row }">
              <el-button type="text" @click="linkToMoni(row)">监控</el-button>
              <el-button type="text" @click="linkToClusterDetail(row)">集群详情</el-button>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </transition>
  </div>
</template>

<script>
import { getClusterListByQueryApi } from '@/http/api'
import { throttle } from 'lodash'

export default {
  name: 'home',
  data () {
    return {
      clusterKeyword: null,
      clusterList: []
    }
  },
  created () {
    this.clusterKeyword = this.$route.query.key
    this.loadClusterData()
  },
  methods: {
    searchCluster: throttle(function searchCluster () {
      this.loadClusterData()
    }, 1000),
    async loadClusterData () {
      if (!this.clusterKeyword) return
      try {
        const { data } = await getClusterListByQueryApi({
          name: this.clusterKeyword
        })
        this.clusterList = data.items
        this.$router.replace({ name: 'home', query: { key: this.clusterKeyword } })
      } catch ({ error }) {
        this.$message.error(error || '获取失败')
      }
    },
    linkToClusterDetail ({ name }) {
      this.$router.push({ name: 'cluster', params: { name } })
    },
    linkToMoni ({ monitor }) {
      window.open(monitor)
    }
  }
}
</script>

<style lang="scss" scoped>
@import '@/style/mixin.scss';

.home-page {
  width: 100%;
  height: calc(100vh - 200px);
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  @include slide-transition;

  .search-panel {
    width: 80%;
    margin-bottom: 20px;
    display: flex;
    flex-direction: column;
    align-items: center;
    flex-shrink: 0;

    &__header {
      font-size: 35px;
      margin-bottom: 20px;
    }

    &__input {
      display: flex;
      width: 100%;
    }
  }

  .search-result {
    width: 80%;
  }
}
</style>
