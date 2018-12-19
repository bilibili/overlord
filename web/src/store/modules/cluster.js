import Vue from 'vue'
import * as types from '../mutation-types'
import { Message } from 'element-ui'
import { getClusterDetailApi } from '@/http/api'

// initial state
const state = {
  clusterDetail: {},
  loading: false
}

// getters
const getters = {
}

// actions
const actions = {
  async getClusterDetail ({ commit }, { name }) {
    commit(types.SAVE_CLUSTER_DETAIL_LOADING, true)
    try {
      const { data } = await getClusterDetailApi(name)
      data.instances && data.instances.forEach(item => {
        Vue.set(item, 'weightInfo', {
          value: item.weight,
          type: 'view'
        })
      })
      commit(types.SAVE_CLUSTER_INFO, data)
    } catch ({ error }) {
      Message.error(`获取失败：${error}`)
    }
    commit(types.SAVE_CLUSTER_DETAIL_LOADING, false)
  },
  updateInstance ({ commit }, { changeType, index, item, newType, weightValue }) {
    commit(types.UPDATE_CLUSTER_INSTANCES, { changeType, index, item, newType, weightValue })
  }
}

// mutations
const mutations = {
  [types.SAVE_CLUSTER_INFO] (state, data) {
    state.clusterDetail = data
  },
  [types.SAVE_CLUSTER_DETAIL_LOADING] (state, loading) {
    state.loading = loading
  },
  [types.UPDATE_CLUSTER_INSTANCES] (state, { changeType, index, item, newType, weightValue }) {
    if (changeType === 'display') {
      state.clusterDetail.instances[index].weightInfo.type = newType
    } else {
      state.clusterDetail.instances[index].weightInfo.value = weightValue
    }
  }
}

export default {
  namespaced: true,
  state,
  getters,
  actions,
  mutations
}
