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

## 文件结构

```bash
    ├── README.md
    ├── babel.config.js
    ├── package.json
    ├── postcss.config.js
    ├── public
    │   ├── favicon.ico
    │   └── index.html
    ├── src
    │   ├── App.vue
    │   ├── assets
    │   │   ├── Starbounder-2.otf
    │   ├── constants
    │   │   ├── CREATE_TYPES.js
    │   │   └── GROUP.js
    │   ├── http
    │   │   ├── api.js
    │   │   ├── config.js
    │   │   └── service.js
    │   ├── layout
    │   │   ├── Header.vue
    │   │   └── SideBar.vue
    │   ├── main.js
    │   ├── router.js
    │   ├── store
    │   │   ├── index.js
    │   │   ├── modules
    │   │   │   ├── cluster.js
    │   │   │   └── job.js
    │   │   └── mutation-types.js
    │   ├── style
    │   │   ├── element-custom.scss
    │   │   ├── element-variables.scss
    │   │   ├── mixin.scss
    │   │   └── reset.scss
    │   └── views
    │       ├── AddCluster.vue
    │       ├── AppId.vue
    │       ├── Cluster.vue
    │       ├── Home.vue
    │       └── Job.vue
    ├── vue.config.js
    └── yarn.lock
```

## 文件配置

你可以在 `vue.config.js` 文件中配置 proxy，以及其他 webpack 相关的配置，详细配置文档请参考 [配置参考](https://cli.vuejs.org/zh/config/)。