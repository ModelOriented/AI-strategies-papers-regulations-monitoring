<template>
  <div class="report">
    <div class="row form">
      <span class="col-md-4 text-end">Show</span>
      <div class="col-md-4">
        <select class="form-select form-select-sm col-4" v-model="count">
          <option v-for="val in [3, 5, 10, 50, 100]" :key="val" :value="val">{{ val }}</option>
        </select>
      </div>
      <span class="col-md-4 test-start">definitions</span>
    </div>
    <SegmentsHighlightsViewer :segments="segments" :highlighted="definitions" />
  </div>
</template>

<script>
import { mapGetters } from 'vuex'
import SegmentsHighlightsViewer from '@/components/SegmentsHighlightsViewer.vue'

export default {
  name: 'Report',
  label: 'Report',
  data () {
    return {
      segments: [],
      definitions: [],
      count: 10
    }
  },
  watch: {
    count () {
      this.loadData()
    }
  },
  computed: {
    ...mapGetters(['api'])
  },
  methods: {
    updateCount (v) {
      console.log(v)
    },
    loadData () {
      const docKey = this.$route.query.document
      if (!docKey) return
      fetch(this.api + '/documents/' + docKey + '/sentences', { method: 'GET' })
        .then(response => response.json())
        .then(response => {
          this.segments = response
        })
        .catch(console.error)
      fetch(this.api + '/documents/' + docKey + '/definitions?n=' + this.count, { method: 'GET' })
        .then(response => response.json())
        .then(response => {
          this.definitions = response
        })
        .catch(console.error)
    }
  },
  created () {
    this.loadData()
  },
  components: { SegmentsHighlightsViewer }
}
</script>
<style lang="sass">
.report .form
  margin: 20px 0 40px 0
  > span
    line-height: 29px
</style>
