import Vue from 'vue'
import App from './App.vue'
import router from './router'
import store from './store'
import ElementUI from 'element-ui'
import '@/style/element-variables.scss'

import http from '@/http/service'

Vue.config.productionTip = false

Vue.use(ElementUI, { size: 'small', zIndex: 3000 })

Vue.prototype.$http = http

new Vue({
  router,
  store,
  render: h => h(App)
}).$mount('#app')
