import { getJobsApi } from '@/http/api'
import { Message } from 'element-ui'
import * as types from '../mutation-types'

// initial state
const state = {
  all: [],
  loading: false
}

// getters
const getters = {
  jobStateList: (state, getters) => {
    return state.all.map(job => job.state)
      .filter((item, index, arr) => arr.indexOf(item) === index)
      .map(stateItem => ({
        text: stateItem,
        value: stateItem
      }))
  }
}

// actions
const actions = {
  async getAllJobs ({ commit }) {
    commit(types.SAVE_JOB_LOADING, true)
    try {
      const { data: { items } } = await getJobsApi()
      commit(types.SAVE_JOB_LIST, items)
    } catch ({ error }) {
      Message.error(`获取失败：${error}`)
    }
    commit(types.SAVE_JOB_LOADING, false)
  }
}

// mutations
const mutations = {
  [types.SAVE_JOB_LIST] (state, jobs) {
    state.all = jobs
  },
  [types.SAVE_JOB_LOADING] (state, loading) {
    state.loading = loading
  }
}

export default {
  namespaced: true,
  state,
  getters,
  actions,
  mutations
}
