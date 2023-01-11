const { createProxyMiddleware } = require('http-proxy-middleware');
module.exports = function (app) {
  app.use(createProxyMiddleware('/transpond', {
    target: process.env.REACT_APP_BASE_URL? process.env.REACT_APP_BASE_URL : '127.0.0.1:8080',
    changeOrigin: true,
    pathRewrite: {
      '^/transpond': '/api'
    }
  }))
}