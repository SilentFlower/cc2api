<script setup lang="ts">
import { computed } from 'vue';
import type { WindowUsageStats } from '../api';

const props = defineProps<{
  stats?: WindowUsageStats | null;
  label: string;
}>();

/** 当前统计是否仍属于未到期的额度周期或最近滚动时间段。 */
const current = computed(() => {
  const stats = props.stats;
  if (stats?.basis === 'quota' && Date.parse(stats.ends_at) <= Date.now()) return null;
  return stats;
});

/** 提示费用的覆盖范围，帮助判断能否与官方百分比对照。 */
const description = computed(() => {
  const stats = current.value;
  if (!stats) return '尚未提供本地用量统计';
  const range = stats.basis === 'quota' ? '当前额度周期' : `最近 ${props.label}`;
  const notes = [`${range}内经过网关的请求，费用按模型 API 单价折算`];
  if (!stats.coverage_complete) notes.push('记录可能未覆盖整个周期');
  if (stats.unpriced_requests) notes.push(`${stats.unpriced_requests} 次请求费用未知`);
  if (stats.partial_requests) notes.push(`${stats.partial_requests} 次请求用量不完整`);
  return notes.join('；');
});

/**
 * 紧凑显示请求数或 Token，不改变用于汇总的原始精度。
 * @param value 原始数量。
 * @return K／M 缩写或完整整数。
 */
function compact(value: number): string {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1).replace(/\.0$/, '')}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1).replace(/\.0$/, '')}K`;
  return String(value);
}
</script>

<template>
  <div v-if="current" class="flex flex-wrap items-center gap-1 pb-1 text-[10px] text-[#8c8475]" :title="description">
    <span v-if="current.basis === 'rolling'" class="text-[#b5b0a6]">最近 {{ label }}</span>
    <span class="rounded bg-[#f6f3ee] px-1.5 py-0.5">{{ compact(current.requests) }} 请求</span>
    <span class="rounded bg-[#f6f3ee] px-1.5 py-0.5">{{ compact(current.tokens) }} Token</span>
    <span v-if="current.requests > 0 && current.unpriced_requests === current.requests" class="rounded bg-amber-50 px-1.5 py-0.5 text-amber-700">费用未知</span>
    <span v-else class="rounded bg-[#f6f3ee] px-1.5 py-0.5">
      {{ current.unpriced_requests || current.partial_requests ? '已知' : '折算' }} ${{ current.cost_usd.toFixed(2) }}
    </span>
    <span v-if="current.estimated_total_cost_usd != null" class="rounded bg-[#f6f3ee] px-1.5 py-0.5"
      title="按本窗口网关费用除以官方已用比例估算；同账号其它入口的消费可能使估算偏低，并非官方美元配额">
      估算总额 ${{ current.estimated_total_cost_usd.toFixed(2) }}
    </span>
    <span v-if="!current.coverage_complete" class="text-[#b5b0a6]">部分记录</span>
  </div>
  <p v-else class="pb-1 text-[10px] text-[#b5b0a6]">统计待更新</p>
</template>
