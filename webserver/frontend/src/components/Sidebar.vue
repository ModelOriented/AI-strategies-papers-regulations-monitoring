<template>
  <div class="sidebar d-flex flex-column p-3 bg-light">
    <a href="/" class="d-flex align-items-center mb-3 mb-md-0 me-md-auto link-dark text-decoration-none">
      <svg class="icon me-2" viewBox="0 0 20 20" width="90" height="40">
        <path d="M4.68,13.716v-0.169H4.554C4.592,13.605,4.639,13.658,4.68,13.716z M11.931,6.465 c-0.307-0.087-0.623,0.106-0.706,0.432l-1.389,5.484c-0.901,0.084-1.609,0.833-1.609,1.757c0,0.979,0.793,1.773,1.773,1.773 c0.979,0,1.773-0.794,1.773-1.773c0-0.624-0.324-1.171-0.812-1.486l1.377-5.439C12.422,6.887,12.239,6.552,11.931,6.465z M10.591,14.729H9.408v-1.182h1.183V14.729z M15.32,13.716c0.04-0.058,0.087-0.11,0.126-0.169H15.32V13.716z M10,3.497 c-3.592,0-6.503,2.911-6.503,6.503H4.68c0-2.938,2.382-5.32,5.32-5.32s5.32,2.382,5.32,5.32h1.182 C16.502,6.408,13.591,3.497,10,3.497z M10,0.542c-5.224,0-9.458,4.234-9.458,9.458c0,5.224,4.234,9.458,9.458,9.458 c5.224,0,9.458-4.234,9.458-9.458C19.458,4.776,15.224,0.542,10,0.542z M15.32,16.335v0.167h-0.212 c-1.407,1.107-3.179,1.773-5.108,1.773c-1.93,0-3.701-0.666-5.108-1.773H4.68v-0.167C2.874,14.816,1.724,12.543,1.724,10 c0-4.571,3.706-8.276,8.276-8.276c4.57,0,8.275,3.706,8.275,8.276C18.275,12.543,17.126,14.816,15.32,16.335z"></path>
      </svg>
      <span class="fs-4">Monitor of AI Regulations</span>
    </a>
    <hr>
    <ul class="nav nav-pills flex-column mb-auto">
      <li class="nav-item">
        <router-link to="/upload" class="nav-link link-dark" :class="{ active: active == '/upload' }">
          <svg class="icon me-2" viewBox="0 0 20 20" width="16" height="16">
            <path d="M17.206,5.45l0.271-0.27l-4.275-4.274l-0.27,0.269V0.9H3.263c-0.314,0-0.569,0.255-0.569,0.569v17.062 c0,0.314,0.255,0.568,0.569,0.568h13.649c0.313,0,0.569-0.254,0.569-0.568V5.45H17.206z M12.932,2.302L16.08,5.45h-3.148V2.302z M16.344,17.394c0,0.314-0.254,0.569-0.568,0.569H4.4c-0.314,0-0.568-0.255-0.568-0.569V2.606c0-0.314,0.254-0.568,0.568-0.568 h7.394v4.55h4.55V17.394z"></path>
          </svg>
          Upload document
        </router-link>
      </li>
      <li class="nav-item">
        <router-link to="/your-documents" class="nav-link link-dark" :class="{ active: active == '/your-documents' }">
          <svg class="icon me-2" viewBox="0 0 20 20" width="16" height="16">
            <path d="M18.208,2.958H8.875V1.792c0-0.644-0.522-1.167-1.167-1.167H1.875c-0.644,0-1.167,0.522-1.167,1.167v16.333
              c0,0.645,0.522,1.166,1.167,1.166h16.333c0.645,0,1.167-0.521,1.167-1.166v-14C19.375,3.48,18.853,2.958,18.208,2.958z
              M18.208,17.542c0,0.322-0.261,0.583-0.583,0.583H2.458c-0.323,0-0.583-0.261-0.583-0.583V6.458h16.333V17.542z M17.625,5.292
              H1.875V2.375c0-0.323,0.261-0.583,0.583-0.583h4.667c0.323,0,0.583,0.261,0.583,0.583v1.75h9.917c0.322,0,0.583,0.261,0.583,0.583
              C18.208,5.031,17.947,5.292,17.625,5.292z">
            </path>
          </svg>
          Your documents
        </router-link>
      </li>
      <li class="nav-item">
        <router-link to="/about" class="nav-link link-dark" :class="{ active: active == '/about' }">
          <svg class="icon me-2" viewBox="0 0 20 20" width="16" height="16">
            <path d="M9.896,3.838L0.792,1.562v14.794l9.104,2.276L19,16.356V1.562L9.896,3.838z M9.327,17.332L1.93,15.219V3.27 l7.397,1.585V17.332z M17.862,15.219l-7.397,2.113V4.855l7.397-1.585V15.219z"></path>
          </svg>
          About project
        </router-link>
      </li>
    </ul>
  </div>
</template>
<script>
export default {
  name: 'Sidebar',
  data () {
    return {
    }
  },
  computed: {
    active () {
      return this.$route.path
    }
  },
  methods: {},
  components: {}
}
</script>
<style lang="sass">
.sidebar
  box-shadow: 0 0 1.5em rgba(0, 0, 0, .1), 0 0 .5em rgba(0, 0, 0, .15)
  width: 280px

.sidebar .nav-link.active
  font-weight: 600
  background: unset !important
  color: unset !important

.sidebar .nav-link:hover
  color: white !important
  background: #0d6efd !important

.icon
  vertical-align: -.125em
  pointer-events: none
  fill: currentColor
</style>
