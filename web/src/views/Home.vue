<template>
  <div class="home-page">

    <div class="search-panel">
      <div class="search-panel__header">
        Cluster Search
      </div>
      <div class="search-panel__input">
        <el-input v-model="cluster" placeholder="集群名关键字" size="large" @keyup.native="searchCluster">
          <i slot="prefix" class="el-input__icon el-icon-search"></i>
        </el-input>
      </div>
    </div>
    <transition name="slide-fade" mode="out-in" appear>
      <div v-if="clusterList.length" class="search-result">
        <el-table :data="clusterList" border>
          <el-table-column prop="name" label="集群名字" width="150">
          </el-table-column>
          <el-table-column prop="appids" label="AppId" width="150">
            <template slot-scope="{ row }">
              <p v-for="(item, index) in row.appids" :key="index">{{ item }}</p>
            </template>
          </el-table-column>
          <el-table-column prop="cache_type" label="缓存类型">
          </el-table-column>
          <el-table-column prop="port" label="监听端口">
          </el-table-column>
          <el-table-column prop="max_memory" label="总容量">
          </el-table-column>
          <el-table-column prop="number" label="节点数">
          </el-table-column>
          <el-table-column label="详情" width="150">>
            <template slot-scope="{ row }">
              <el-button type="text" @click="linkToSetting(row)">监控</el-button>
              <el-button type="text" @click="linkToClusterDetail(row)">集群详情</el-button>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </transition>

  </div>
</template>

<script>
export default {
  name: 'home',
  data () {
    return {
      cluster: null,
      clusterList: []
    }
  },
  methods: {
    // searchCluster: throttle(function queryKeyUpHandler () {
    //   this.applyFilters();
    // }, 1000),
    searchCluster () {
      this.clusterList = [{
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
    },
    linkToClusterDetail ({ name }) {
      this.$router.push({ name: 'cluster', params: { name } })
    },
    linkToSetting () {

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

    &__header {
      font-size: 35px;
      margin-bottom: 20px;
    }

    &__input {
      display: flex;
      width: 100%;
    }

    .el-input {
      margin-right: 10px;
    }
  }

  .search-result {
    width: 80%;
  }
}
</style>
