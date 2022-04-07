<template>
  <div class="your-documents">
    <table class="table">
      <thead>
        <tr>
          <th scope="col">Name</th>
          <th scope="col">Status</th>
          <th scope="col">Actions</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="doc in data" :key="doc.key">
          <td>{{ doc.name }}</td>
          <td :class="{['status-' + doc.status.toLowerCase()]: true}">{{ doc.status }}</td>
          <td>
            <button v-if="doc.status !== 'Done'" type="button" class="btn btn-outline-primary btn-sm" @click="openStatus(doc.key)">See status</button>
            <button v-if="doc.status === 'Done'" type="button" class="btn btn-outline-success btn-sm" @click="open(doc.key)">Open</button>
            <button v-if="doc.status !== 'Processing'" type="button" class="btn btn-outline-danger btn-sm" @click="deleteDoc(doc.key)">Delete</button>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script>
import { mapGetters } from 'vuex'

export default {
  name: 'YourDocuments',
  label: 'Your documents',
  data () {
    return {
      data: [],
      updaterId: null
    }
  },
  computed: mapGetters(['api']),
  methods: {
    update () {
      fetch(this.api + '/jobs/', { method: 'GET' })
        .then(response => response.json())
        .then(response => {
          this.data = response
        })
        .catch(console.error)
    },
    openStatus (key) {
      this.$router.push({ path: '/job', query: { document: key } })
    },
    open (key) {
      this.$router.push({ path: '/report', query: { document: key } })
    },
    deleteDoc (key) {
      fetch(this.api + '/jobs/' + key, { method: 'DELETE' }).catch(console.error)
    }
  },
  created () {
    this.update()
    this.updaterId = setInterval(this.update, 1000)
  },
  beforeDestroy () {
    if (this.updaterId !== null) {
      clearInterval(this.updaterId)
    }
  }
}
</script>
<style lang="sass">
.your-documents > .table
  margin-bottom: 0
  border-bottom: rgba(0, 0, 0, 0)
  button
    margin-right: 10px
  tr
    vertical-align: middle
  .status-done
    color: var(--bs-green)
    font-weight: 400
  .status-processing
    color: var(--bs-blue)
    font-weight: 400
  .status-failed
    color: var(--bs-red)
    font-weight: 400
</style>
