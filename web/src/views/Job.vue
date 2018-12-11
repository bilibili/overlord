<template>
  <div>
    <p class="job-page__title">Job 列表</p>
    <el-table
      :data="jobList"
      v-loading="loading"
      border>
      <el-table-column
        prop="id"
        label="Id"
        width="220">
      </el-table-column>
      <el-table-column
        prop="param"
        label="Param">
        <template slot-scope="{ row }">
          <vue-json-pretty v-if="row.param" :data="JSON.parse(row.param)"></vue-json-pretty>
        </template>
      </el-table-column>
      <el-table-column
        prop="state"
        label="State"
        width="100">
      <template slot-scope="{ row }">
        <el-tag>{{ row.state }}</el-tag>
      </template>
      </el-table-column>
    </el-table>
  </div>
</template>

<script>
import { getJobsApi } from '@/http/api'
import VueJsonPretty from 'vue-json-pretty'

export default {
  data () {
    return {
      jobList: [],
      loading: true
    }
  },
  components: {
    VueJsonPretty
  },
  created () {
    this.loadData()
  },
  methods: {
    async loadData () {
      this.loading = true
      try {
        const { data } = await getJobsApi()
        this.jobList = data.items
      } catch (error) {
      }
      this.loading = false
    }
  }
}
</script>

<style lang="scss" scoped>
@import '@/style/mixin.scss';

.job-page__title {
  @include page-title-font;
  margin: 10px 0;
}
</style>

<style lang="scss">
.vjs__tree {
  font-size: 12px;
}
</style>
