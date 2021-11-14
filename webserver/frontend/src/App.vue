<template>
  <div id="app">
    <Sidebar />
    <MainContainer />
  </div>
</template>
<script>
import Sidebar from '@/components/Sidebar.vue'
import MainContainer from '@/components/MainContainer.vue'

export default {
  name: 'App',
  components: { Sidebar, MainContainer }
}
</script>
<style lang="sass">
body
  height: 100vh

#app
  display: flex
  flex-wrap: nowrap
  height: 100vh
  max-height: 100vh
  overflow-x: auto
  overflow-y: hidden
  background-color: rgba(0, 0, 0, .1)
</style>
