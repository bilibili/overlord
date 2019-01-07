import Vue from 'vue'
import Vuex from 'vuex'
import jobs from './modules/job'
import clusters from './modules/cluster'

Vue.use(Vuex)

const debug = process.env.NODE_ENV !== 'production'

export default new Vuex.Store({
  modules: {
    jobs,
    clusters
  },
  strict: debug
})
