# overlord 前端开发指南

overlord-platform 前端项目基于 vue cli 3 搭建的单页面应用。

## 环境要求

nodejs 8.x

## 安装编译

```bash
cd web

# 安装依赖
npm install

# 启动
yarn run serve

# Lint 文件
yarn run lint
```

## 文件结构

```bash
    ├── README.md
    ├── babel.config.js             // babel 配置
    ├── package.json
    ├── postcss.config.js           // postcss 配置
    ├── public
    │   ├── favicon.ico
    │   └── index.html
    ├── src
    │   ├── App.vue                 // 页面入口文件
    │   ├── assets                  // 资源文件
    │   │   ├── Starbounder-2.otf
    │   ├── constants               // 常量文件
    │   │   └── CREATE_TYPES.js
    │   ├── http
    │   │   ├── api.js              // API 方法
    │   │   ├── config.js           // axios 基本配置
    │   │   └── service.js          // axios 拦截器
    │   ├── layout                  // 页面布局组件
    │   │   ├── Header.vue
    │   │   └── SideBar.vue
    │   ├── main.js                 // 程序入口文件
    │   ├── router.js               // 路由配置
    │   ├── store                   // vuex 状态管理
    │   │   ├── index.js
    │   │   ├── modules
    │   │   │   ├── cluster.js
    │   │   │   └── job.js
    │   │   └── mutation-types.js
    │   ├── style                   // 全局样式
    │   │   ├── element-custom.scss
    │   │   ├── element-variables.scss
    │   │   ├── mixin.scss
    │   │   └── reset.scss
    │   └── views                   // 业务代码
    │       ├── AddCluster.vue      // 创建集群
    │       ├── AppId.vue           // AppId 管理
    │       ├── Cluster.vue         // 集群详情
    │       ├── Home.vue            // 首页（Cluster 搜索）
    │       └── Job.vue             // Job 列表
    ├── vue.config.js               // 全局 CLI 配置文件
    └── yarn.lock                   // 依赖 lock 文件
```

## 文件配置

你可以在 `vue.config.js` 文件中配置 proxy，以及其他 webpack 相关的配置，详细配置文档请参考 [配置参考](https://cli.vuejs.org/zh/config/)。
