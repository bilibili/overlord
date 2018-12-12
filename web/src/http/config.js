// import qs from 'qs'
// import { AxiosResponse, AxiosRequestConfig } from 'axios'

const axiosConfig = {
  baseURL: '/',
  // 请求后的数据处理
  // transformResponse: [function (data) {
  //   return data
  // }],
  // 超时设置s
  timeout: 30000,
  responseType: 'json'
}

export default axiosConfig
