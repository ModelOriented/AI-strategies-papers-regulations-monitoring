<template>
  <div class="document-viewer">
    <div class="menu">
      <span @click="scrollToHighlight">Find highlighted</span>
      <span @click="$emit('exit')">Exit</span>
    </div>
    <div class="window" ref="window">
      <div class="segment" v-for="seg, segIndex in segments" :key="segIndex">
        <span class="sentence" v-for="sent, sentIndex in segments[segIndex]" :class="{ highlighted: highlightedKeys[segIndex + '_' + sentIndex] }"
          :key="sentIndex" :ref="segIndex">
          {{ (sentIndex === 0 ? '' : ' ') + sent }}
        </span>
      </div>
    </div>
  </div>
</template>
<script>
export default {
  name: 'DocumentViewer',
  props: {
    segments: Array,
    highlighted: Array,
    scroll: Boolean
  },
  computed: {
    highlightedKeys () {
      return this.highlighted.map(d => d.segment + '_' + d.sentence).reduce((agg, v) => ({ ...agg, [v]: true }), {})
    }
  },
  methods: {
    scrollToHighlight () {
      if (!this.scroll || this.highlighted.length === 0) return
      const hl = this.highlighted[0]
      if (this.segments.length <= hl.segment || this.segments[hl.segment].length <= hl.sentence) return
      const ref = this.$refs[hl.segment][hl.sentence]
      if (!ref) return
      const frame = this.$refs.window
      this.$refs.window.scrollTop = ref.offsetTop - (frame.offsetHeight / 2) + (ref.offsetHeight / 2)
    }
  },
  mounted () {
    this.scrollToHighlight()
  },
  updated () {
    this.scrollToHighlight()
  }
}
</script>
<style lang="sass">
.document-viewer
  position: relative
.document-viewer > .menu
  position: absolute
  right: 0px
  top: -32px
.document-viewer > .menu > span
  margin: 0 5px
  font-size: 14px
  font-weight: 600
  cursor: pointer
.document-viewer > .menu > span:hover
  color: #0d6efd
.document-viewer > .window
  position: relative
  height: 400px
  overflow-y: auto
.document-viewer > .window > .segment > .sentence.highlighted
  font-weight: 600
  background: #ecdf6f
</style>
