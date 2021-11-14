<template>
  <div class="job">
    <table class="table">
      <thead>
        <tr>
          <th scope="col">Step</th>
          <th scope="col">Status</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="step in data" :key="step.job">
          <td>{{ step.name }}</td>
          <td :class="{['status-' + step.status]: true}">{{ step.status }}</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script>
export default {
  name: 'Job',
  label: 'Processing status',
  data () {
    return {
      data: []
    }
  },
  watch: {
    data (newValue, oldValue) {
      const docKey = this.$route.query.document
      if (!oldValue.every(x => x.status === 'done') && newValue.every(x => x.status === 'done')) {
        this.$router.push({ path: '/report', query: { document: docKey } })
      }
    }
  },
  methods: {
    update () {
      const docKey = this.$route.query.document
      if (!docKey) return
      fetch('/api/jobs/' + docKey + '/status', { method: 'GET' })
        .then(response => response.json())
        .then(response => {
          this.data = response
        })
        .catch(console.error)
    }
  },
  created () {
    this.update()
    setInterval(this.update, 1000)
  }
}
</script>
<style lang="sass">
.job > .table
  margin-bottom: 0
  border-bottom: rgba(0, 0, 0, 0)
  button
    margin-right: 10px
  tr
    vertical-align: middle
  .status-done
    color: var(--bs-green)
    font-weight: 400
  .status-running
    color: var(--bs-blue)
    font-weight: 400
  .status-failed
    color: var(--bs-red)
    font-weight: 400
  .status-waiting
    font-weight: 400
</style>
