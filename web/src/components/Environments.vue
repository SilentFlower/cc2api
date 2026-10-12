<script setup lang="ts">
import { onMounted, ref } from 'vue';
import { api } from '../api';

const ready = ref(false);
const error = ref('');

/** 建立统一后台会话后再加载环境管理。@return 无返回值。 */
async function connect(): Promise<void> {
  try {
    await api.openRuntime();
    ready.value = true;
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : '环境管理暂不可用';
  }
}

onMounted(connect);
</script>

<template>
  <div class="flex h-full min-h-0 w-full flex-col">
    <p v-if="error" class="p-4" role="alert">{{ error }}</p>
    <iframe v-if="ready" src="/runtime/" title="账号环境、任务与抓包管理" class="block min-h-0 w-full flex-1 border-0" />
    <p v-else-if="!error" class="p-4">正在连接环境管理…</p>
  </div>
</template>
