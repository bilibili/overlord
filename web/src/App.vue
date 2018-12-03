<template>
  <div id="app">
    <SideBar/>
    <div class="layout">
      <Header/>
      <transition name="slide-fade" mode="out-in" appear>
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
@import './style/reset.scss';
$backgroud-color: #F8F9FD;

#app {
 background: $backgroud-color;
 min-height: 100vh;
}
.layout {
  margin-left: 240px;
  display: flex;
  flex-direction: column;
  .slide-fade-enter-active {
    transition: all .15s ease;
  }

  .slide-fade-leave-active {
    transition: all .15s cubic-bezier(1.0, 0.5, 0.8, 1.0);
  }

  .slide-fade-enter,
  .slide-fade-leave-to {
    transform: translateY(10px);
    opacity: 0;
  }
}
</style>
