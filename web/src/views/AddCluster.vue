<template>
  <div class="create-page">
    <p class="create-page__title">创建集群</p>
    <div class="create-container">
      <el-form :model="clusterForm" :rules="rules" ref="clusterForm" label-width="80px">

        <el-form-item label="名称" prop="name" required>
          <el-input v-model="clusterForm.name"></el-input>
        </el-form-item>

        <el-form-item label="总容量" prop="total_memory" required>
          <el-input v-model="clusterForm.total_memory" type="number">
            <template slot="append">
              <el-select v-model="memoryUnit" placeholder="请选择">
                <el-option
                  v-for="item in memoryUnitOptions"
                  :key="item"
                  :label="item"
                  :value="item">
                </el-option>
              </el-select>
            </template>
          </el-input>
        </el-form-item>

        <el-form-item label="类型" prop="cache_type" required>
          <el-radio-group v-model="clusterForm.cache_type" @change="typeChange">
            <el-radio :label="item.value" v-for="(item, index) in typeOptions" :key="index">{{ item.name }}</el-radio>
          </el-radio-group>
          <el-tooltip class="item" effect="light" placement="bottom-start">
            <div slot="content" class="type-tooltip">
              <h6>Redis Cluster<i>*</i></h6>
              <p>如果你不知道用什么缓存的话，<b>我们推荐选择 Redis Cluster</b>。</p>
              <p>Cluster 提供了比单节点更高的安全性和比 Memcache 更强、更多的第三方工具。</p>
              <p>Redis Cluster 的主从机制为您的数据保驾护航。</p>
              <h6>Redis</h6>
              <p>如果你有的项目有历史积累，必须要用 Redis 单节点，</p>
              <p>或者对 Redis 的稳定和安全性没有需求；并且你的运维非常抠门的话，请选择本项。</p>
              <h6>Memcache</h6>
              <p>Memcache 多线程的特性使得它特别适合于那些 value 超大的（例如 binary 文件）kv 存储</p>
              <p>但是配套的同步工具比较弱，需要慎重。</p>
            </div>
            <i class="el-icon-info type-icon"></i>
          </el-tooltip>
        </el-form-item>

        <el-form-item label="版本" prop="version" required>
          <el-radio-group v-model="clusterForm.version">
            <el-radio :label="item" v-for="(item, index) in versionOptions" :key="index">{{ item }}</el-radio>
          </el-radio-group>
        </el-form-item>

        <el-form-item label="型号" required>
          <el-radio-group v-model="clusterForm.spec">
            <el-radio :label="item.value" v-for="(item, index) in specOptions" :key="index">{{ item.name }}</el-radio>
          </el-radio-group>
        </el-form-item>

        <el-form-item label="规格" prop="spec" required>
          <div v-if="clusterForm.spec !== 'custom'">
            {{clusterForm.spec}}
          </div>
          <div v-else class="custom-spec">
            <el-input v-model="specCustomForm.core" size="mini" type="number">
              <template slot="append">核</template>
            </el-input>
            <el-input v-model="specCustomForm.memory" size="mini" type="number">
              <template slot="append">
                <el-select v-model="specMemoryUnit" placeholder="请选择">
                  <el-option
                    v-for="item in memoryUnitOptions"
                    :key="item"
                    :label="item"
                    :value="item">
                  </el-option>
                </el-select>
              </template>
            </el-input>
            </div>
        </el-form-item>

        <el-form-item label="分组" required>
          <el-select v-model="clusterForm.group" class="group-select" filterable placeholder="请选择分组">
            <el-option
              v-for="item in groupOptions"
              :key="item.name"
              :label="item.name_cn"
              :value="item.name">
            </el-option>
          </el-select>
        </el-form-item>

        <el-form-item label="APPID" prop="appids">
          <el-select v-model="clusterForm.appids" class="appid-select" multiple filterable placeholder="请选择需要关联 APPID">
            <el-option
              v-for="item in appidOptions"
              :key="item"
              :label="item"
              :value="item">
            </el-option>
          </el-select>
        </el-form-item>

        <el-form-item class="footer-item">
          <el-button @click="resetForm('clusterForm')">重置</el-button>
          <el-button type="primary" @click="submitForm('clusterForm')" :disabled="submitDisabled">立即创建</el-button>
        </el-form-item>
      </el-form>
    </div>

  </div>
</template>

<script>
import { getVersionsApi, getGroupsApi, createClusterApi, getAppidsApi } from '@/http/api'
import { TYPE_OPTIONS, SPEC_OPTIONS } from '@/constants/CREATE_TYPES'

export default {
  data () {
    const checkName = (rule, value, callback) => {
      if (!value) {
        callback(new Error('请输入名称'))
      }
      if (!/^\w+$/.test(value)) {
        callback(new Error('仅支持英文大小写、数字和下划线_'))
      }
      callback()
    }
    const checkTotalMemory = (rule, value, callback) => {
      if (!value) {
        callback(new Error('请输入总容量'))
      }
      if (value <= 0) {
        callback(new Error('请输入大于0的数值'))
      }
      callback()
    }
    const checCustomSpec = (rule, value, callback) => {
      if (value === 'custom' && (!this.specCustomForm.core || !this.specCustomForm.memory)) {
        callback(new Error('请输入规格'))
      }
      if (value === 'custom' && (this.specCustomForm.core <= 0 || this.specCustomForm.memory <= 0)) {
        callback(new Error('请输入大于0的数值'))
      }
      callback()
    }
    return {
      memoryUnit: 'G',
      specMemoryUnit: 'G',
      memoryUnitOptions: ['G', 'M'],
      rules: {
        name: [{
          validator: checkName,
          trigger: 'change'
        }],
        total_memory: [{
          validator: checkTotalMemory,
          trigger: 'change'
        }],
        spec: [{
          validator: checCustomSpec,
          trigger: 'change'
        }]
      },
      clusterForm: {
        name: null,
        cache_type: 'redis_cluster',
        spec: '0.25c2g',
        total_memory: null,
        version: null,
        group: null,
        appids: []
      },
      typeOptions: TYPE_OPTIONS,
      specOptions: SPEC_OPTIONS,
      groupOptions: [],
      allVersionOptions: [],
      versionOptions: [],
      specCustomForm: {
        core: null,
        memory: null
      },
      appidOptions: [],
      submitDisabled: false
    }
  },
  created () {
    this.getGroups()
    this.getAppids()
    this.getVersions()
  },
  methods: {
    async getAppids () {
      try {
        const { data } = await getAppidsApi({
          format: 'plain'
        })
        this.appidOptions = data.items
      } catch (_) {
        this.$message.error('AppId 列表获取失败')
      }
    },
    typeChange () {
      this.clusterForm.version = null
      this.versionOptions = []
      this.versionOptions = this.allVersionOptions.find(item => item.cache_type === this.clusterForm.cache_type).versions
      this.clusterForm.version = this.versionOptions[0]
    },
    async getVersions () {
      try {
        const { data } = await getVersionsApi()
        this.allVersionOptions = data.items
        this.versionOptions = this.allVersionOptions.find(item => item.cache_type === this.clusterForm.cache_type).versions
        this.clusterForm.version = this.versionOptions[0]
      } catch (_) {
        this.$message.error('版本列表获取失败')
      }
    },
    async getGroups () {
      try {
        const { data } = await getGroupsApi()
        this.groupOptions = data.items
        this.clusterForm.group = null
      } catch (_) {
        this.$message.error('分组列表获取失败')
      }
    },
    submitForm (formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          this.onSubmit()
        } else {
          return false
        }
      })
    },
    resetForm (formName) {
      this.$refs[formName].resetFields()
      // this.clusterForm.spec = null
      this.clusterForm.version = this.versionOptions[0]
      this.memoryUnit = 'G'
      this.specMemoryUnit = 'G'
    },
    async onSubmit () {
      let params = JSON.parse(JSON.stringify(this.clusterForm))
      params.total_memory = this.memoryUnit === 'G' ? Number(params.total_memory) * 1024 : Number(params.total_memory)
      if (params.spec === 'custom') {
        params.spec = `${this.specCustomForm.core}c${this.specCustomForm.memory}${this.specMemoryUnit === 'G' ? 'g' : 'm'}`
      }
      this.submitDisabled = true
      try {
        await createClusterApi(params)
        this.$message.success('创建中，请等待')
        setTimeout(() => {
          this.$router.push({ name: 'cluster', params: { name: this.clusterForm.name } })
        }, 3000)
      } catch ({ error }) {
        this.$message.error(`创建失败：${error}`)
      }
      this.submitDisabled = false
    }
  }
}
</script>

<style lang="scss" scoped>
@import '@/style/mixin.scss';
$edit-icon-color: #1890ff;
$green-color: #67C23A;

.create-page__title {
  @include page-title-font;
  margin: 10px 0;
}

.create-container {
  padding: 20px 40px;
  margin-top: 20px;
  background: #fff;
  border-radius: 5px;

  .el-form {
    max-width: 600px;
  }
}

.type-tooltip {
  i {
    color: $green-color;
    font-size: 18px;
  }

  h6 {
    margin: 8px 0 5px 0;
  }

  p {
    margin: 3px 0;
  }

  .emphasis-text {
    color: #000;
  }
}

.type-icon {
  margin: 2px 10px;
}

.appid-select,
.group-select {
  width: 100%;
}

.custom-spec {
  display: flex;

  .el-input {
    width: 150px;
    margin-right: 5px;

    .el-input-group__append {
      padding: 0 5px;
    }
  }
}

.footer-item {
  margin-top: 50px;
  display: flex;
  justify-content: flex-end;
}
</style>

<style lang="scss">
.create-page {
  .el-input-group__append {
    width: 60px;
  }
}
</style>
