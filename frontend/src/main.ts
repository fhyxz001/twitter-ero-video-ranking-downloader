import { createApp } from 'vue'
import { createPinia } from 'pinia'
import { ElLoading } from 'element-plus'
import 'element-plus/theme-chalk/base.css'
import 'element-plus/es/components/loading/style/css'
import 'element-plus/es/components/message/style/css'
import 'element-plus/es/components/message-box/style/css'
import App from './App.vue'
import router from './router'
import './styles/index.css'

const app = createApp(App)
app.use(createPinia())
app.use(router)
// 组件样式由 unplugin-vue-components 按需引入；这里只注册指令
app.use(ElLoading)
app.mount('#app')
