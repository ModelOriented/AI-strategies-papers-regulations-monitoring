import Vue from 'vue'
import VueRouter from 'vue-router'
import About from '../views/About.vue'
import Upload from '../views/Upload.vue'
import YourDocuments from '../views/YourDocuments.vue'
import Report from '../views/Report.vue'
import Job from '../views/Job.vue'

Vue.use(VueRouter)

const routes = [
  { path: '/', redirect: '/upload' },
  { path: '/upload', name: Upload.name, component: Upload, meta: { label: Upload.label } },
  { path: '/your-documents', name: YourDocuments.name, component: YourDocuments, meta: { label: YourDocuments.label } },
  { path: '/about', name: About.name, component: About, meta: { label: About.label } },
  { path: '/job', name: Job.name, component: Job, meta: { label: Job.label } },
  { path: '/report', name: Report.name, component: Report, meta: { label: Report.label } }
]

const router = new VueRouter({
  mode: 'history',
  base: process.env.BASE_URL,
  routes
})

export default router
