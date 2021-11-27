import Vue from 'vue'
import App from './App.vue'
import router from './router'
import store from './store'
import vueDebounce from 'vue-debounce'
import 'bootstrap/dist/css/bootstrap.min.css'
import 'bootstrap'

Vue.config.productionTip = false
Vue.use(vueDebounce)

new Vue({
  router,
  store,
  render: h => h(App)
}).$mount('#app')
