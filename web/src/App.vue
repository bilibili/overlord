<template>
  <div id="app">
    <SideBar/>
    <div class="layout">
      <Header/>
      <transition class="transition" name="slide-fade" mode="out-in" appear>
        <router-view/>
      </transition>
    </div>
  </div>
</template>

<script>
import Header from '@/layout/Header.vue'
import SideBar from '@/layout/SideBar.vue'

export default {
  components: {
    Header,
    SideBar
  }
}
</script>

<style lang="scss">
@import '@/style/mixin.scss';
@import '@/style/reset.scss';
@import '@/style/element-custom.scss';

$backgroud-color: #F8FAFC;

#app {
 background: $backgroud-color;
 min-height: 100vh;
 @include slide-transition;
}
.layout {
  margin-left: 240px;
  display: flex;
  flex-direction: column;
}
</style>
