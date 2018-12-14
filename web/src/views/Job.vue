<template>
  <div>
    <p class="job-page__title">Job 列表</p>
    <el-table :data="jobList" ref="dataTable" v-loading="loading" border @current-change="onSelectionChanged">
      <el-table-column type="expand">
        <template slot-scope="{ row }">
          <vue-json-pretty v-if="row.param" :data="JSON.parse(row.param)"></vue-json-pretty>
        </template>
      </el-table-column>
      <el-table-column type="index" width="80">
      </el-table-column>
      <el-table-column prop="id" label="Id">
      </el-table-column>
      <el-table-column prop="state" label="State">
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
  components: {
    VueJsonPretty
  },
  data () {
    return {
      jobList: [],
      loading: true
    }
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
    },
    onSelectionChanged (newRow) {
      const table = this.$refs.dataTable
      table.toggleRowExpansion(newRow)
      table.setCurrentRow()
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
