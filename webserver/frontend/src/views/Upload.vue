<template>
  <div class="upload row">
    <div class="col-2"></div>
    <div class="col-8">
      <div v-if="error" class="alert alert-danger mb-3" role="alert">
        {{ error }}
      </div>
      <div class="row mb-3">
        <label class="col-sm-3 col-form-label">Document name</label>
        <div class="col-sm-9">
          <input type="text" class="form-control" :class="{ 'is-invalid': !!errors.name, 'is-valid': errors.name === null }" ref="inputName" v-debounce:500="validateName">
          <div class="invalid-feedback">{{ errors.name }}</div>
        </div>
      </div>
      <div class="row mb-3">
        <label class="col-sm-3 col-form-label">File</label>
        <div class="col-sm-9">
          <input type="file" class="form-control" :class="{ 'is-invalid': !!errors.file, 'is-valid': errors.file === null }" @change="validateFile" ref="inputFile">
          <div class="invalid-feedback">{{ errors.file }}</div>
        </div>
      </div>
      <button type="submit" class="btn btn-primary" @click="send">Send</button>
    </div>
  </div>
</template>

<script>
import { mapGetters } from 'vuex'

export default {
  name: 'Upload',
  label: 'Upload your document',
  data () {
    return {
      error: null,
      errors: { name: undefined, file: undefined }
    }
  },
  computed: mapGetters(['api']),
  methods: {
    validateName () {
      const name = this.$refs.inputName.value
      if (!name || !(typeof name === 'string' || name instanceof String) || name.length < 5) {
        this.errors.name = 'Name must have at least 5 characters'
      } else if (name.length > 120) {
        this.errors.name = 'Name cannot have more than 120 characters'
      } else {
        this.errors.name = null
      }
    },
    validateFile () {
      const file = this.$refs.inputFile.files
      if (file.length === 0) {
        this.errors.file = 'You must select file'
      } else if (file.length !== 1) {
        this.errors.file = 'You must select only one file'
      } else if (!['application/pdf', 'text/html'].includes(file[0].type)) {
        this.errors.file = 'File must be PDF or HTML'
      } else {
        this.errors.file = null
      }
    },
    validate () {
      this.validateName()
      this.validateFile()
      return Object.values(this.errors).reduce((agg, v) => agg && v === null, true)
    },
    send () {
      if (!this.validate()) {
        this.error = 'Please fix all issues below fields before sending'
      } else {
        this.error = null
      }

      const data = new FormData()
      data.append('file', this.$refs.inputFile.files[0])
      data.append('name', this.$refs.inputName.value)
      fetch(this.api + '/jobs/', { method: 'POST', body: data })
        .then(response => {
          if (response.status === 413) return { error: 'File too large' }
          return response.json()
        })
        .then(response => {
          if (response.error) {
            this.error = response.error
          } else if (response.document) {
            this.$router.push({ path: 'job', query: { document: response.document } })
          }
        })
        .catch(e => {
          this.error = 'Unexpected error'
        })
    }
  }
}
</script>
<style lang="sass">
</style>
