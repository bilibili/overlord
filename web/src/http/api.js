import http from '@/http/service'

const getClusterListByQueryApi = params => {
  return http.get(`api/v1/clusters`, {
    params
  })
}

const getAppidsApi = params => {
  return http.get(`api/v1/appids`, {
    params
  })
}

const getJobsApi = params => {
  return http.get('api/v1/jobs')
}

const getVersionsApi = params => {
  return http.get('api/v1/versions')
}

const getAppidDetailApi = params => {
  return http.get(`api/v1/appids/${params}`)
}

const getClusterDetailApi = params => {
  return http.get(`api/v1/clusters/${params}`)
}

const removeCorrelationApi = (clusterName, params) => {
  return http.delete(`api/v1/clusters/${clusterName}/appid`, {
    data: params
  })
}

const patchInstanceWeightApi = (clusterName, addr, params) => {
  return http.patch(`api/v1/clusters/${clusterName}/instances/${addr}`, params)
}

const createClusterApi = params => {
  return http.post('api/v1/clusters', params)
}

const addCorrelationApi = (clusterName, params) => {
  return http.post(`api/v1/clusters/${clusterName}/appid`, params)
}

const addAppIdApi = (params) => {
  return http.post('api/v1/appids', params)
}

export {
  getClusterListByQueryApi,
  getAppidsApi,
  getJobsApi,
  getVersionsApi,
  getAppidDetailApi,
  getClusterDetailApi,
  removeCorrelationApi,
  patchInstanceWeightApi,
  createClusterApi,
  addCorrelationApi,
  addAppIdApi
}
