import axios from 'axios'
import config from './config'

const service = axios.create(config)

// 添加请求拦截器
service.interceptors.request.use(
  req => {
    return req
  },
  error => {
    return Promise.reject(error)
  }
)

// 返回状态判断(添加响应拦截器) todo
service.interceptors.response.use(
  res => {
    return res
  },
  error => {
    return Promise.reject(error)
  }
)

export default service
