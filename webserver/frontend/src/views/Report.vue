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
    <div class="group" v-for="group in groups" :key="getGroupIdentifier(group)" :class="{ visible: group.visible, hidden: !group.visible }">
      <button v-if="group.visible && group.prev" class="expand-group btn btn-outline-secondary btn-sm" @click="showPrev(group)">Show previous segment</button>
      <div class="segment" v-for="segmentIndex in group.visible ? group.segments : []" :key="segmentIndex">
        <span class="sentence" v-for="sentence, sentenceIndex in segments[segmentIndex]" :class="{ marked: definitionsKeys[segmentIndex + '_' + sentenceIndex] }" :key="sentenceIndex">{{ sentence }}</span>
      </div>
      <button v-if="group.visible && group.next" class="expand-group btn btn-outline-secondary btn-sm" @click="showNext(group)">Show next segment</button>
      <button v-if="!group.visible" class="btn btn-outline-secondary btn-sm" @click="show(group)">Expand</button>
    </div>
  </div>
</template>

<script>
export default {
  name: 'Report',
  label: 'Report',
  data () {
    return {
      segments: [],
      definitions: [],
      groups: [],
      count: 10
    }
  },
  watch: {
    count () {
      this.loadData()
    },
    defaultVisibleSegments () {
      if (this.segments.length === 0) {
        this.groups = []
        return
      }
      const isVisible = index => this.defaultVisibleSegments.includes(index)
      let group = { prev: null, next: null, segments: [0], visible: isVisible(0) }
      const groups = [group]
      for (let i = 1; i < this.segments.length; i++) {
        if (group.visible === isVisible(i)) group.segments.push(i)
        else {
          const newGroup = { prev: group, next: null, segments: [i], visible: isVisible(i) }
          group.next = newGroup
          group = newGroup
          groups.push(group)
        }
      }
      this.groups = groups
    }
  },
  computed: {
    definitionsKeys () {
      return this.definitions.map(d => d.segment + '_' + d.sentence).reduce((agg, v) => ({ ...agg, [v]: true }), {})
    },
    defaultVisibleSegments () {
      return [...new Set(this.definitions.map(d => d.segment).map(x => [x - 1, x, x + 1]).flat())].sort()
    }
  },
  methods: {
    updateCount (v) {
      console.log(v)
    },
    loadData () {
      const docKey = this.$route.query.document
      if (!docKey) return
      fetch('/api/documents/' + docKey + '/sentences', { method: 'GET' })
        .then(response => response.json())
        .then(response => {
          this.segments = response
        })
        .catch(console.error)
      fetch('/api/documents/' + docKey + '/definitions?n=' + this.count, { method: 'GET' })
        .then(response => response.json())
        .then(response => {
          this.definitions = response
        })
        .catch(console.error)
    },
    getGroupIdentifier (group) {
      return group.segments[0] + '_' + group.segments[group.length - 1]
    },
    mergeGroups (group1, group2, visible, base) {
      let group = { prev: group1.prev, next: group2.next, visible, segments: [...group1.segments, ...group2.segments] }
      group = Object.assign(base, group)
      if (group.prev) group.prev.next = group
      if (group.next) group.next.prev = group
      return group
    },
    showPrev (group) {
      group.segments.unshift(group.prev.segments.pop())
      const prev = group.prev
      if (prev.segments.length === 0) {
        this.mergeGroups(prev, group, group.visible, group)
        if (prev.prev) this.mergeGroups(prev.prev, group, group.visible, group)
        this.groups = this.groups.filter(g => g !== prev && g !== prev.prev)
      }
      this.$set(this, 'groups', this.groups)
    },
    showNext (group) {
      group.segments.push(group.next.segments.shift())
      const next = group.next
      if (next.segments.length === 0) {
        this.mergeGroups(group, next, group.visible, group)
        if (next.next) this.mergeGroups(group, next.next, group.visible, group)
        this.groups = this.groups.filter(g => g !== next && g !== next.next)
      }
      this.$set(this, 'groups', this.groups)
    },
    show (group) {
      if (group.prev && group.next) {
        this.mergeGroups(group, group.next, group.next.visible, group.next)
        this.mergeGroups(group.prev, group.next, group.next.visible, group.next)
        this.groups = this.groups.filter(g => g !== group && g !== group.prev)
      } else if (group.next) {
        this.mergeGroups(group, group.next, group.next.visible, group.next)
        this.groups = this.groups.filter(g => g !== group)
      } else if (group.prev) {
        this.mergeGroups(group.prev, group, group.prev.visible, group.prev)
        this.groups = this.groups.filter(g => g !== group)
      }
    }
  },
  created () {
    this.loadData()
  }
}
</script>
<style lang="sass">
.report .form
  margin: 20px 0 40px 0
  > span
    line-height: 29px
.report .sentence.marked
  font-weight: 600
.report .group.visible
  margin: 10px
  padding: 10px
  border: 1px solid #ccc
.report .group.hidden
  text-align: center
.report .expand-group
  position: relative
  transform: translateX(-50%)
  left: 50%
</style>
