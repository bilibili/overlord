<template>
  <div class="side-bar">
    <el-menu
      :default-active="$route.path"
      class="el-menu"
      text-color="#94A0B4"
      active-text-color="#fff"
      router>
      <div class="side-bar__title">
        <h1>Overlord</h1>
      </div>
      <el-menu-item index="/">
        <i class="el-icon-menu"></i>
        <span slot="title">Homepage</span>
      </el-menu-item>
      <el-menu-item index="/appid">
        <i class="el-icon-document"></i>
        <span slot="title">Appid</span>
      </el-menu-item>
      <el-menu-item index="/task">
        <i class="el-icon-time"></i>
        <span slot="title">Task</span>
      </el-menu-item>
      <el-menu-item index="/cluster">
        <i class="el-icon-goods"></i>
        <span slot="title">Cluster</span>
      </el-menu-item>
      <el-menu-item index="/create">
        <i class="el-icon-circle-plus-outline"></i>
        <span slot="title">Create</span>
      </el-menu-item>
    </el-menu>
  </div>
</template>

<script>
export default {
  name: 'SideBar',
  methods: {
  }
}
</script>

<style lang="scss">
.side-bar {
  float: left;
  width: 240px;
  height: 100vh;

  &__title {
    height: 60px;
    text-align: center;
    line-height: 60px;
    color: #fff;
    border-bottom: 1px solid #3a5173;
  }
}

// reset el-menu
.el-menu {
  height: 100%;
  text-align: left;
  border: none;
  background: linear-gradient(to bottom, rgba(10,32,56,1) 0%,rgba(56,76,110,1) 16%,rgba(88,108,149,1) 56%,rgba(111,132,181,1) 100%);
  filter: progid:DXImageTransform.Microsoft.gradient( startColorstr='#0a2038', endColorstr='#6f84b5',GradientType=0 );
  .el-menu-item {
    border-left: 2px;
  }
  .el-menu-item i {
    color: #94A0B4 !important;
  }

  .el-menu-item:focus,
  .el-menu-item:hover {
    outline: 0;
    background-color: #e59ca4 !important;
    color: #fff !important;
    i {
      color: #fff !important;
    }
  }

  .el-menu-item.is-active {
    background: linear-gradient(to right, rgba(124,138,178,1) 0%,rgba(252,125,127,1) 100%);
    filter: progid:DXImageTransform.Microsoft.gradient( startColorstr='#7c8ab2', endColorstr='#fc7d7f',GradientType=1 );
    font-weight: bold;
    color: #ffffff !important;
    i {
      font-weight: bold;
      color: #fff !important;
    }
  }
}
</style>
