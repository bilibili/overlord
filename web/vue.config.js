module.exports = {
  devServer: {
    port: 8083,
    proxy: {
      '/api/v1': {
        target: 'http://172.22.33.198:8880'
      }
    }
  },
  configureWebpack: {
    performance: {
      hints: false
    }
  }
}
