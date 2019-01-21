# overlord 前端开发指南

overlord-platform 前端项目基于 vue cli 3 搭建的单页面应用。

## 环境要求

nodejs 8.x

## 安装编译

```base
cd web

# 安装依赖
npm install

# 启动
yarn run serve

# Lint 文件
yarn run lint
```

你可以在 `vue.config.js` 文件中配置 proxy，以及其他 webpack 相关的配置，详细配置文档请参考 [配置参考](https://cli.vuejs.org/zh/config/)。