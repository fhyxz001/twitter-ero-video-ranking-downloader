import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  {
    path: '/',
    name: 'home',
    component: () => import('@/views/HomeView.vue'),
  },
  {
    path: '/poster',
    name: 'poster',
    component: () => import('@/views/PosterView.vue'),
  },
  {
    path: '/waterfall',
    name: 'waterfall',
    component: () => import('@/views/WaterfallView.vue'),
  },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

export default router
