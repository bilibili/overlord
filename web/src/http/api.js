import http from '@/http/service'

const getClusterListByQueryApi = params => {
  return http.get(`api/v1/clusters`, {
    params
  })
  // return http.get('https://easy-mock.com/mock/5c073aa78eae976e080b800f/overlord/search')
}

const getAppidsApi = params => {
  return http.get(`api/v1/appids`, {
    params
  })
}

const getJobsApi = params => {
  return http.get('api/v1/jobs')
}

const getAppidDetailApi = params => {
  return http.get(`api/v1/appids/${params}`)
  // return http.get('https://easy-mock.com/mock/5c073aa78eae976e080b800f/overlord/getClusterListByAppid')
}

const removeCorrelationApi = (clusterName, params) => {
  return http.delete(`/clusters/${clusterName}/appid`, {
    params
  })
}

const getClusterDetailApi = params => {
  return http.get(`api/v1/clusters/${params}`)
  // return http.get('https://easy-mock.com/mock/5c073aa78eae976e080b800f/overlord/cluster')
}

const patchInstanceWeightApi = (clusterName, addr, params) => {
  return http.patch(`/${clusterName}/instances/${addr}`, {
    params
  })
}

export { getClusterListByQueryApi, getAppidsApi, getAppidDetailApi, removeCorrelationApi, getClusterDetailApi, patchInstanceWeightApi, getJobsApi }
