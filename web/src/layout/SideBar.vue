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
      <el-menu-item index="/job">
        <i class="el-icon-time"></i>
        <span slot="title">Job</span>
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
@import '@/style/mixin.scss';

.side-bar {
  float: left;
  width: 240px;
  height: 100vh;

  &__title {
    @include flex-horizon-justify-center;
    height: 60px;
    text-align: center;
    color: #fff;
    border-bottom: 1px solid #3a5173;
  }
}
</style>
