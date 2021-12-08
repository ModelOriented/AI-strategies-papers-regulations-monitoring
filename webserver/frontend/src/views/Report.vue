<template>
  <div class="report">
    <nav class="nav nav-pills nav-fill">
      <a class="nav-link" v-for="tab in tabs" :key="tab.id" href="#" :class="{ active: tab.active }" @click="activeTab = tab.id">{{ tab.name }}</a>
    </nav>
    <div v-if="activeTab === 'text'" class="tab-text">
      <DocumentViewer :segments="segments" :highlighted="[]" />
    </div>
    <div v-else-if="activeTab === 'definitions'">
      <div class="row form">
        <span class="col-md-4 text-end">Show</span>
        <div class="col-md-4">
          <select class="form-select form-select-sm col-4" v-model="definitionsCount">
            <option v-for="val in [3, 5, 10, 50, 100]" :key="val" :value="val">{{ val }}</option>
          </select>
        </div>
        <span class="col-md-4 test-start">definitions</span>
      </div>
    </div>
    <div v-else-if="activeTab === 'issues'">
      <div class="row form">
        <span class="col-md-1 text-end">Model:</span>
        <div class="col-md-3">
          <select class="form-select form-select-sm col-4" v-model="issuesModel">
            <option v-for="val in issuesModels" :key="val" :value="val">{{ val }}</option>
          </select>
        </div>
        <span class="col-md-1 text-end">Issue:</span>
        <div class="col-md-3">
          <select class="form-select form-select-sm col-4" v-model="issue" :disabled="!issuesModel">
            <option v-for="val in issuesNames" :key="val" :value="val">{{ val + ' (' + issues[val].length + ')' }}</option>
          </select>
        </div>
        <span class="col-md-1 text-end">Count:</span>
        <div class="col-md-3">
          <select class="form-select form-select-sm col-4" v-model="issuesCount">
            <option v-for="val in [3, 5, 10, 25, 50, 75, 100]" :key="val" :value="val">{{ val }}</option>
          </select>
        </div>
      </div>
    </div>
    <SentencesHighlightsViewer v-if="highlighted && viewer==='sentences'" :segments="segments" :highlighted="highlighted" />
    <SegmentsHighlightsViewer v-else-if="highlighted" :segments="segments" :highlighted="highlighted" />
  </div>
</template>

<script>
import { mapGetters } from 'vuex'
import SegmentsHighlightsViewer from '@/components/SegmentsHighlightsViewer.vue'
import SentencesHighlightsViewer from '@/components/SentencesHighlightsViewer.vue'
import DocumentViewer from '@/components/DocumentViewer.vue'

export default {
  name: 'Report',
  label: 'Report',
  data () {
    return {
      segments: [],
      definitions: [],
      definitionsCount: 5,
      activeTab: 'text',
      viewer: 'sentences',
      issuesModels: [],
      issuesModel: null,
      issues: null,
      issue: null,
      issuesCount: 5
    }
  },
  watch: {
    docKey () {
      this.segments = []
      this.definitions = []
      this.definitionsCount = 5
      this.issuesModels = []
      this.issuesModel = null
      this.issues = null
      this.issue = null
      this.issuesCount = 5
      this.loadData()
    },
    issuesModel: 'loadIssues',
    issuesNames () {
      if (this.issuesNames.length > 0 && !this.issuesNames.includes(this.issue)) this.issue = null
    }
  },
  computed: {
    tabs () {
      return [
        { name: 'Document text', id: 'text' },
        { name: 'Definitions', id: 'definitions' },
        { name: 'Key issues', id: 'issues' }
      ].map(x => ({ ...x, active: this.activeTab === x.id }))
    },
    docKey () {
      return this.$route.query.document
    },
    issuesNames () {
      return Object.keys(this.issues || {})
    },
    highlighted () {
      if (this.activeTab === 'issues' && this.issues && this.issue) {
        return this.getTopSentences(this.issues[this.issue], this.issuesCount)
      } else if (this.activeTab === 'definitions' && this.definitions) {
        return this.getTopSentences(this.definitions, this.definitionsCount)
      }
      return null
    },
    ...mapGetters(['api'])
  },
  methods: {
    getTopSentences (list, n) {
      return list
        .map((value, index) => ({ value, index }))
        .sort((a, b) => b.value.probability - a.value.probability)
        .slice(0, n)
        .sort((a, b) => a.index - b.index)
        .map(x => x.value)
    },
    loadData () {
      if (!this.docKey) return
      fetch(this.api + '/documents/' + this.docKey + '/sentences', { method: 'GET' })
        .then(response => response.json())
        .then(response => {
          this.segments = response
        })
        .catch(console.error)
      fetch(this.api + '/documents/' + this.docKey + '/definitions?n=100', { method: 'GET' })
        .then(response => response.json())
        .then(response => {
          this.definitions = response
        })
        .catch(console.error)
      fetch(this.api + '/documents/' + this.docKey + '/issues/models', { method: 'GET' })
        .then(response => response.json())
        .then(response => {
          this.issuesModels = response
        })
        .catch(console.error)
    },
    loadIssues () {
      const model = this.issuesModel
      this.issues = null
      if (!model) {
        return
      }
      fetch(this.api + '/documents/' + this.docKey + '/issues/' + model + '?n=100&threshold=0.75', { method: 'GET' })
        .then(response => response.json())
        .then(response => {
          if (this.issuesModel === model) this.issues = response
        })
        .catch(console.error)
    }
  },
  created () {
    this.loadData()
  },
  components: { SegmentsHighlightsViewer, SentencesHighlightsViewer, DocumentViewer }
}
</script>
<style lang="sass">
.report > .tab-text
  padding-top: 20px
  > .document-viewer > .window
    height: 600px
.report .form
  margin: 20px 0 40px 0
  > span
    line-height: 29px
</style>
