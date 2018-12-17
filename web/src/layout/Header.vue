<template>
  <div class="header">
    <p class="header__user">Hello, 2233</p>
  </div>
</template>

<script>
export default {
  name: 'Header'
}
</script>

<style lang="scss" scoped>
.header {
  height: 40px;
  padding: 0 20px;
  display: flex;
  align-items: center;
  flex-direction: row-reverse;
  flex-shrink: 0;

  &__user {
    font-size: 15px;
  }
}
</style>
