<template>
  <div class="segments-highlights-viewer">
    <div class="group" v-for="group in groups" :key="getGroupIdentifier(group)" :class="{ visible: group.visible, hidden: !group.visible }">
      <button v-if="group.visible && group.prev" class="expand-group btn btn-outline-secondary btn-sm" @click="showPrev(group)">Show previous segment</button>
      <div class="segment" v-for="segmentIndex in group.visible ? group.segments : []" :key="segmentIndex">
        <span class="sentence" v-for="sentence, sentenceIndex in segments[segmentIndex]" :class="{ marked: highlightedKeys[segmentIndex + '_' + sentenceIndex] }" :key="sentenceIndex">
          {{ (sentenceIndex === 0 ? '' : ' ') + sentence }}
        </span>
      </div>
      <button v-if="group.visible && group.next" class="expand-group btn btn-outline-secondary btn-sm" @click="showNext(group)">Show next segment</button>
      <button v-if="!group.visible" class="btn btn-outline-secondary btn-sm" @click="show(group)">Expand</button>
    </div>
  </div>
</template>
<script>
export default {
  name: 'SegmentsHighlightsViewer',
  props: {
    segments: Array,
    highlighted: Array
  },
  data () {
    return {
      groups: []
    }
  },
  watch: {
    defaultVisibleSegments: {
      handler: 'updateGroups',
      immediate: true
    },
    segments: {
      handler: 'updateGroups',
      immediate: true
    }
  },
  computed: {
    defaultVisibleSegments () {
      return [...new Set(this.highlighted.map(this.getVisibleRangeForSentence).flat())].sort()
    },
    highlightedKeys () {
      return this.highlighted.map(d => d.segment + '_' + d.sentence).reduce((agg, v) => ({ ...agg, [v]: true }), {})
    }
  },
  methods: {
    getVisibleRangeForSentence ({ segment, sentence }) {
      const range = [segment]
      if (sentence === 0 && segment > 0) range.push(segment - 1)
      if (sentence === this.segments[segment].length - 1 && segment < this.segments.length - 1) {
        range.push(segment + 1)
      }
      range.sort()
      return range
    },
    updateGroups () {
      if (this.segments.length === 0) {
        this.groups = []
        return
      }
      const isVisible = index => this.defaultVisibleSegments.includes(index)
      let group = { prev: null, next: null, segments: [0], visible: isVisible(0), expanded: false }
      const groups = [group]
      for (let i = 1; i < this.segments.length; i++) {
        if (group.visible === isVisible(i)) group.segments.push(i)
        else {
          const newGroup = { prev: group, next: null, segments: [i], visible: isVisible(i), expanded: false }
          group.next = newGroup
          group = newGroup
          groups.push(group)
        }
      }
      this.groups = groups
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
  }
}
</script>
<style lang="sass">
.segments-highlights-viewer .sentence.marked
  font-weight: 600
  background: #ecdf6f
.segments-highlights-viewer .group.visible
  margin: 10px
  padding: 10px
  border: 1px solid #ccc
.segments-highlights-viewer .group.hidden
  text-align: center
.segments-highlights-viewer .expand-group
  position: relative
  transform: translateX(-50%)
  left: 50%
</style>
