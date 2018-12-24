import Vue from 'vue'
import * as types from '../mutation-types'
import { Message } from 'element-ui'
import { getClusterDetailApi, getClusterListByQueryApi } from '@/http/api'

// initial state
const state = {
  clusterDetail: {},
  loading: false,
  clusterResult: [],
  queryLoading: false
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
  },
  async getClusterResult ({ commit }, { name }) {
    commit(types.SAVE_CLUSTER_DETAIL_LOADING, true)
    try {
      const { data: { items } } = await getClusterListByQueryApi({
        name
      })
      commit(types.SAVE_CLUSTER_BY_QUERY, items)
    } catch ({ error }) {
      Message.error(`获取失败：${error}`)
    }
    commit(types.SAVE_CLUSTER_DETAIL_LOADING, false)
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
  },
  [types.SAVE_CLUSTER_BY_QUERY] (state, data) {
    state.clusterResult = data
  },
  [types.SAVE_CLUSTER_BY_QUERY_LOADING] (state, queryLoading) {
    state.queryLoading = queryLoading
  }
}

export default {
  namespaced: true,
  state,
  getters,
  actions,
  mutations
}
