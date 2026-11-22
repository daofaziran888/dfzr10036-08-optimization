import { createRouter, createWebHistory } from 'vue-router'

import HandoffView from '@/views/HandoffView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/compaction-handoff', name: 'compaction-handoff', component: HandoffView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
