<template>
  <div class="sentences-highlights-viewer">
    <div class="fragment" v-for="frag in fragments" :key="frag.index" :class="{ closed: !opened[frag.index] }">
      <div v-if="!opened[frag.index]" @click="$set(opened, frag.index, true)" class="preview">
        <span class="sentence-before" v-for="s, i in frag.before" :key="'b_' + i">{{ s }}</span>
        <span class="sentence-highlighted" v-for="s, i in frag.highlighted" :key="'h_' + i">{{ s }}</span>
        <span class="sentence-after" v-for="s, i in frag.after" :key="'a_' + i">{{ s }}</span>
      </div>
      <DocumentViewer v-else :segments="segments" :highlighted="[frag.original]" scroll @exit="opened[frag.index] = false" />
    </div>
  </div>
</template>
<script>
import DocumentViewer from '@/components/DocumentViewer.vue'

export default {
  name: 'SentencesHighlightsViewer',
  props: {
    segments: Array,
    highlighted: Array
  },
  data () {
    return {
      opened: {} // index props of opened fragments
    }
  },
  watch: {
    highlighted: 'clear',
    segments: 'clear'
  },
  computed: {
    // Segments flatted into one array of sentences with new line
    // added to each last sentence of segment and space to other sentences
    sentences () {
      return this.segments.map(seg => [...seg.slice(0, -1).map(middle => middle + ' '), ...seg.slice(-1).map(last => last + '\n')]).flat()
    },
    // indexes of senteces where segments starts
    segmentsStart () {
      // eslint-disable-next-line
      const cumsum = () => ((sum => value => sum += value)(0))
      return [0, ...this.segments.map(x => x.length).map(cumsum()).slice(0, -1)]
    },
    fragments () {
      const range = 2
      return this.highlighted
        .map(h => ({ index: this.segmentsStart[h.segment] + h.sentence, original: h }))
        .sort((a, b) => a.index - b.index)
        .map(({ index, original }) => ({
          index,
          original,
          before: this.sentences.slice(index - range, index),
          after: this.sentences.slice(index + 1, index + 1 + range),
          highlighted: [this.sentences[index]]
        }))
    }
  },
  methods: {
    clear () {
      this.opened = {}
    }
  },
  components: { DocumentViewer }
}
</script>
<style lang="sass">
.sentences-highlights-viewer > .fragment > .preview > .sentence-highlighted
  font-weight: 600
  background: #ecdf6f
.sentences-highlights-viewer > .fragment
  margin: 40px 10px
  padding: 7px
  border: 1px solid #ddd
  border-radius: 7px
.sentences-highlights-viewer > .fragment.closed
  cursor: zoom-in
.sentences-highlights-viewer > .fragment.closed:hover
  box-shadow: 0 0 5px 0 #ccc
</style>
