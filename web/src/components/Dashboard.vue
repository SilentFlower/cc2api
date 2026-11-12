<script setup lang="ts">
import { computed, ref, onMounted } from 'vue';
import { useRoute } from 'vue-router';
import { api, type Dashboard as DashboardData } from '../api';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { logout } from '../router';
import { useToast } from '../composables/useToast';

const route = useRoute();
const embedded = computed(() => ['environments', 'tasks'].includes(String(route.name)));
const links = [{name: 'updates', label: '系统更新'}, {name: 'environments', label: '环境'},
  {name: 'tasks', label: '任务'}, {name: 'accounts', label: '账号'}, {name: 'requests', label: '请求记录'},
  {name: 'tokens', label: '令牌'}, {name: 'settings', label: '设置'}];
const toast = useToast();
const loggingOut = ref(false);

/** 完成统一退出；服务异常时展示错误并允许重试。@return 退出操作完成。 */
async function handleLogout(): Promise<void> {
  if (loggingOut.value) return;
  loggingOut.value = true;
  try {
    await logout();
  } catch (error) {
    toast.show(error instanceof Error ? error.message : '退出失败，请重试');
  } finally {
    loggingOut.value = false;
  }
}

/** 仪表盘统计数据 */
const dashboard = ref<DashboardData | null>(null);

/** 加载仪表盘数据 */
async function loadDashboard() {
  try {
    dashboard.value = await api.getDashboard();
  } catch {
    // 忽略瞬态错误
  }
}

/** 格式化大数字为千分位 */
function formatNum(n: number): string {
  return n.toLocaleString();
}

onMounted(loadDashboard);
</script>

<template>
  <div :class="embedded ? 'h-dvh flex flex-col overflow-hidden' : 'min-h-screen'">
    <!-- 顶部导航栏 -->
    <header class="sticky top-0 z-40 shrink-0 bg-card/95 backdrop-blur-md border-b border-border px-4 sm:px-6 py-3">
      <div class="max-w-7xl mx-auto flex items-center justify-between gap-3">
        <div class="flex min-w-0 flex-wrap items-center gap-x-6 gap-y-2">
          <div class="flex min-w-0 items-center gap-2">
            <img src="/favicon.svg" alt="Logo" class="w-6 h-6" />
            <h1 class="text-lg font-semibold text-foreground tracking-tight">Claude Code Gateway</h1>
          </div>
          <nav class="flex flex-wrap items-center gap-1" aria-label="主导航">
            <router-link v-for="link in links" :key="link.name" :to="{name: link.name}"
              class="px-3 py-1.5 text-sm rounded-lg transition-colors"
              :class="route.name === link.name ? 'bg-primary/10 text-primary font-medium' : 'text-muted-foreground hover:text-foreground hover:bg-muted'"
              :aria-current="route.name === link.name ? 'page' : undefined">{{ link.label }}</router-link>
          </nav>
        </div>
        <Button
          variant="ghost"
          size="sm"
          @click="handleLogout"
          :disabled="loggingOut"
          class="text-muted-foreground hover:text-foreground hover:bg-muted"
        >
          退出
        </Button>
      </div>
    </header>

    <!-- bench 自己管理内容滚动；外层只分配导航下方的空间，避免双重滚动和宽度压缩。 -->
    <main :class="embedded ? 'flex-1 min-h-0 w-full overflow-hidden' : 'max-w-7xl mx-auto px-4 sm:px-6 py-6 space-y-6'">
      <!-- 统计卡片 -->
      <div v-if="dashboard && !['environments', 'tasks', 'updates', 'requests'].includes(String(route.name))" class="grid grid-cols-2 md:grid-cols-5 gap-4">
        <Card class="bg-card border-border rounded-xl hover:shadow-md transition-all duration-200 !py-0 !gap-0">
          <CardContent class="py-3 px-4">
            <p class="text-muted-foreground text-xs mb-1">总账号</p>
            <p class="text-2xl font-bold text-foreground">{{ formatNum(dashboard.accounts.total) }}</p>
          </CardContent>
        </Card>
        <Card class="bg-card border-border rounded-xl hover:shadow-md transition-all duration-200 !py-0 !gap-0">
          <CardContent class="py-3 px-4">
            <p class="text-muted-foreground text-xs mb-1">活跃</p>
            <p class="text-2xl font-bold text-emerald-600">{{ formatNum(dashboard.accounts.active) }}</p>
          </CardContent>
        </Card>
        <Card class="bg-card border-border rounded-xl hover:shadow-md transition-all duration-200 !py-0 !gap-0">
          <CardContent class="py-3 px-4">
            <p class="text-muted-foreground text-xs mb-1">异常</p>
            <p class="text-2xl font-bold text-red-500">{{ formatNum(dashboard.accounts.error) }}</p>
          </CardContent>
        </Card>
        <Card class="bg-card border-border rounded-xl hover:shadow-md transition-all duration-200 !py-0 !gap-0">
          <CardContent class="py-3 px-4">
            <p class="text-muted-foreground text-xs mb-1">停用</p>
            <p class="text-2xl font-bold text-muted-foreground">{{ formatNum(dashboard.accounts.disabled) }}</p>
          </CardContent>
        </Card>
        <Card class="bg-card border-border rounded-xl hover:shadow-md transition-all duration-200 !py-0 !gap-0">
          <CardContent class="py-3 px-4">
            <p class="text-muted-foreground text-xs mb-1">令牌</p>
            <p class="text-2xl font-bold text-foreground">{{ formatNum(dashboard.tokens) }}</p>
          </CardContent>
        </Card>
      </div>

      <!-- 子路由内容 -->
      <router-view @refresh="loadDashboard" />
    </main>
  </div>
</template>
