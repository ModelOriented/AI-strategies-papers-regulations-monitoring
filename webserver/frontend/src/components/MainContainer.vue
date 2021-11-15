<template>
  <div class="main-container">
    <div class="container">
      <div class="row">
        <div class="col-lg-1"></div>
        <span class="page-title col-lg-10">{{ title }}</span>
      </div>
      <div class="row">
        <div class="col-lg-1"></div>
        <div class="page-content col-lg-10 bg-light">
          <router-view />
        </div>
      </div>
    </div>
  </div>
</template>
<script>
export default {
  name: 'MainContainer',
  computed: {
    title () {
      return this.$route.meta.label || this.$route.name
    }
  }
}
</script>
<style lang="sass">
.main-container
  flex-grow: 1
  padding: 30px 0
  overflow-y: auto
.main-container > .container > .row > .page-title
  font-size: 2em
  padding-left: 0
.main-container > .container > .row > .page-content
  margin-top: 30px
  padding: 15px
  box-shadow: 0 0 1.5em rgba(0, 0, 0, .1)
  border-radius: 3px
</style>
