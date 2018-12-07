<template>
  <div>
    <p class="job-page__title">Job 列表</p>
    <el-table
      :data="tableData"
      border>
      <el-table-column
        prop="id"
        label="Id">
      </el-table-column>
      <el-table-column
        prop="param"
        label="Param">
      </el-table-column>
      <el-table-column
        prop="state"
        label="State">
      <template slot-scope="{ row }">
        <el-tag>{{ row.state }}</el-tag>
      </template>
      </el-table-column>
    </el-table>
  </div>
</template>

<script>
export default {
  data () {
    return {
      tableData: [{
        id: '000000000002',
        param: '{"abc":"def"}',
        state: 'running'
      }, {
        id: '000000000002',
        param: '{"abc":"def"}',
        state: 'running'
      }, {
        id: '000000000002',
        param: '{"abc":"def"}',
        state: 'running'
      }, {
        id: '000000000002',
        param: '{"abc":"def"}',
        state: 'running'
      }]
    }
  },
  created () {

  },
  methods: {
    loadData () {

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
