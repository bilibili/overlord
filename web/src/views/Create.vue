<template>
  <div class="create-page">
    <p class="create-page__title">集群创建</p>
    <div class="create-container">
      <el-steps :active="active" finish-status="success" align-center>
        <el-step title="集群名称"></el-step>
        <el-step title="选择类型/版本"></el-step>
        <el-step title="选择型号/容量"></el-step>
        <el-step title="选择分组"></el-step>
        <el-step title="关联 APPID"></el-step>
        <el-step title="确认"></el-step>
        <el-step title="完成"></el-step>
      </el-steps>

      <!-- step: 1 选择名称 -->
      <div v-if="active === 0" class="create-container__panel">
        <div class="create-container__panel--name">
          <el-input v-model="clusterForm.name" placeholder="集群名称"></el-input>
        </div>
      </div>
      <!-- step: 2 选择类型 -->
      <div v-if="active === 1" class="create-container__panel">
        <div class="create-container__panel--type">
          <el-radio v-model="clusterForm.cache_type"
            :label="item.value"
            v-for="(item, index) in typeOptions"
            class="card-item type-item"
            :class="{ 'card-item--active': item.value === clusterForm.cache_type }"
            :key="index">
            <img :src="item.url" :alt="item.name">
            <h2>{{ item.name }}</h2>
            <span>{{ item.desc }}</span>
          </el-radio>
        </div>
      </div>

      <!-- step: 3 选择型号 -->
      <div v-if="active === 2" class="create-container__panel">
        <div class="create-container__panel--spec">
          <el-radio v-model="clusterForm.spec"
            :label="item.value"
            v-for="(item, index) in specOptions"
            class="card-item spec-item"
            :class="{ 'card-item--active': item.value === clusterForm.spec }"
            :key="index">
            <h2>{{ item.name }}</h2>
            <p v-if="item.value !== 'custom'">{{ item.desc }}</p>
            <div v-else>
            <el-input class="mini-input" v-model="clusterForm.name" size="mini">
              <template slot="append">核</template>
            </el-input>
            <el-input class="mini-input" v-model="clusterForm.name" size="mini">
              <template slot="append">G</template>
            </el-input>
            </div>
          </el-radio>
        </div>
        <div class="create-container__panel--memory">
          <el-input v-model="clusterForm.total_memory" placeholder="总容量">
            <template slot="append">G</template>
          </el-input>
        </div>
      </div>

      <!-- step: 4 选择分组 -->
      <div v-if="active === 3" class="create-container__panel">
        <div class="create-container__panel--group">
          <el-radio v-model="clusterForm.group" :label="item.value" v-for="(item, index) in groupOptions"
          class="card-item group-item"
          :class="{ 'card-item--active': item.value === clusterForm.group }"
          :key="index">
            <span>{{ item.name }}</span>
          </el-radio>
        </div>
      </div>

      <!-- step: 4 关联 APPID -->
      <div v-if="active === 4" class="create-container__panel create-container__panel--appid">
        <el-select v-model="clusterForm.appids" multiple filterable placeholder="请选择需要关联 APPID">
          <el-option
            v-for="item in options"
            :key="item.name"
            :label="item.name"
            :value="item.name">
          </el-option>
        </el-select>
      </div>

      <!-- step: 6 提交 -->
      <div v-if="active === 5" class="create-container__panel create-container__panel--submit">
        <el-form label-width="120px" :model="clusterForm" size="mini">
          <el-form-item label="名称">
            {{clusterForm.name || '--'}}
          </el-form-item>
          <el-form-item label="类型">
            {{clusterForm.cache_type || '--'}}
          </el-form-item>
          <el-form-item label="型号">
            {{clusterForm.spec || '--'}}
          </el-form-item>
          <el-form-item label="总容量">
            {{clusterForm.total_memory || '--'}} M
          </el-form-item>
          <el-form-item label="机房">
            {{clusterForm.group || '--'}}
          </el-form-item>
          <el-form-item label="APPID">
            {{clusterForm.appids}}
          </el-form-item>
        </el-form>
      </div>

      <!-- step: 4 选择分组 -->
      <div v-if="active === 6" class="create-container__panel">
        完成
        去查看
      </div>

      <div v-if="active !== 6" class="create-container__footer">
        <el-button v-if="active !== 0" @click="last">上一步</el-button>
        <el-button type="primary" @click="next" :disabled="nextButtonDisabled">{{active === 5 ? '创建' : '下一步' }}</el-button>
      </div>

      <div v-else class="create-container__footer">
        <el-button>再创建一个</el-button>
        <el-button type="primary" @click="linkToClusterDetail">去查看集群</el-button>
      </div>
    </div>

  </div>
</template>

<script>
import { TYPE_OPTIONS, SPEC_OPTIONS, GROUP_OPTIONS } from '@/constants/CREATE_TYPES'
export default {
  data () {
    return {
      active: 0,
      maxMem: 1024,
      //  集群类型列表 todo 放 constant
      typeOptions: TYPE_OPTIONS,
      specOptions: SPEC_OPTIONS,
      groupOptions: GROUP_OPTIONS,
      clusterForm: {
        cache_type: 'redis_cluster',
        spec: '1c2g',
        total_memory: null,
        group: 'sh001',
        appids: []
      },
      options: [{
        'name': 'main.platform.overlord',
        'label': 'overlord'
      }, {
        'name': 'main.platform.discorvery',
        'label': 'discorvery'
      }, {
        'name': 'live.live.xreward-service',
        'label': 'xreward-service'
      }],
      appidKeyword: null
    }
  },
  computed: {
    nextButtonDisabled () {
      if (this.active === 0) {
        return !this.clusterForm.name
      }
      if (this.active === 1) {
        return !this.clusterForm.cache_type
      }
      if (this.active === 2) {
        return !this.clusterForm.total_memory
      }
      if (this.active === 3) {
        return !this.clusterForm.group
      }
      if (this.active === 4) {
        return !this.clusterForm.appids.length
      }
      if (this.active === 5) {
        return false
      }
      return false
    }
  },
  created () {
    this.active = this.$route.query.step - 1 || 0
    this.$router.replace({ name: 'create', query: { step: this.active + 1 } })
  },
  methods: {
    linkToClusterDetail () {
      this.$router.push({ name: 'cluster', params: { name: this.clusterForm.name } })
    },
    radioChnage (nv) {
      this.radio = nv
    },
    last () {
      if (this.active-- > 5) this.active = 0
      this.$router.replace({ name: 'create', query: { step: this.active + 1 } })
    },
    next () {
      if (this.active++ > 5) this.active = 0
      this.$router.replace({ name: 'create', query: { step: this.active + 1 } })
    },
    handleSelectAppIdChange () {
      // if (this.active === type) return
    }
  }
}
</script>

<style lang="scss" scoped>
@import '@/style/mixin.scss';

.create-page__title {
  @include page-title-font;
  margin: 10px 0;
}

.create-container {
  padding: 20px 0;
  margin-top: 20px;
  background: #fff;
  border-radius: 5px;
  &__panel {
    margin: 20px 5%;
    &--name {
      padding: 30px 100px;
      .el-input {
        min-width: 300px;
      }
    }
    // 类型
    &--type {
      @include flex-horizon-justify-center;
      .type-item {
        width: 30%;
        height: 330px;
        cursor: pointer;
        h2 {
          font-size: 16px;
          font-weight: bold;
          margin: 15px 0 10px 0;
        }
        span {
          color: #5c737f;
          font-size: 14px;
          margin: 10px 0;
          line-height: 1.5;
        }
        img {
          height: 100px;
        }
      }
    }
    &--spec {
      @include flex-horizon-justify-center;
      .spec-item {
        width: 23%;
        height: 100px;
        cursor: pointer;
        h2 {
          font-size: 16px;
          font-weight: bold;
          margin: 10px;
        }
      }
    }
    &--memory {
      margin: 30px 5px;
      display: flex;
      align-items: center;
      .el-input {
        width: 300px;
      }
    }
    &--group {
      @include flex-horizon-justify-center;
      .group-item {
        width: 130px;
      }
    }
    &--appid {
      @include flex-vertical-justify-center;
    }
    &--submit {
      @include flex-horizon-justify-center;
      .el-form {
        width: 400px;
      }
      .el-form-item--mini.el-form-item {
        margin-bottom: 8px;
      }
    }
  }
  .card-item {
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 20px 10px;
    margin: 10px;
    border-radius: 5px;
    transition: .3s;
    border: 2px solid rgba(101, 121, 162, 0);
    @include box-shadow(0.2);
    &:hover {
      @include box-shadow(0.3);
    }
    &--active {
      border: 2px solid rgba(101, 121, 162, 0.7);
      @include box-shadow(1);
    }
  }
  &__footer {
    margin: 50px 0 20px 0;
    @include flex-horizon-justify-center;
  }
}
</style>

<style lang="scss">
.create-page {
  .el-radio {
    .el-radio__input {
      display: none;
    }
    .el-radio__label {
      padding: 0;
      width: 100% !important;
      display: flex;
      flex-direction: column;
      align-items: center;
      span {
        white-space: normal;
      }
    }
  }
}
.mini-input {
  width: 90px;
  .el-input-group__append {
    padding: 0 8px;
  }
}
</style>
