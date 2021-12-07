import Vue from 'vue'
import Vuex from 'vuex'

Vue.use(Vuex)

const state = {
  api: '/api/'
}

const getters = {
  api (state) { return state.api }
}

const mutations = {
  setApi (state, api) {
    Vue.set(state, 'api', api.endsWith('/') ? api.slice(0, -1) : api)
  }
}

const actions = {
}

export default new Vuex.Store({
  state, getters, mutations, actions
})
