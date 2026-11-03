<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue';
import { api, type SchedulingDetail, type SchedulingSession, type SchedulingSessions } from '../api';
import { schedulingReasonLabel } from '../lib/scheduling';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog';

const props = defineProps<{ open: boolean; accountId: number; accountName: string }>();
const emit = defineEmits<{ 'update:open': [value: boolean] }>();
const detail = ref<SchedulingDetail | null>(null);
const sessions = ref<SchedulingSessions | null>(null);
const error = ref('');
const loading = ref(false);
const page = ref(1);
const state = ref('all');
const prefix = ref('');
const appliedPrefix = ref('');
const rangeMs = ref(300000);
const localNow = ref(Date.now());
const receivedAt = ref(0);
let generation = 0;
let controller: AbortController | null = null;
let refreshTimer: ReturnType<typeof setInterval> | null = null;
let clockTimer: ReturnType<typeof setInterval> | null = null;

const summary = computed(() => detail.value?.summary);
const estimatedServerNow = computed(() => (detail.value?.snapshot.observed_at_ms ?? 0) + Math.max(0, localNow.value - receivedAt.value));
const stale = computed(() => Boolean(error.value) || (receivedAt.value > 0 && localNow.value - receivedAt.value > 12000));
const nextLabel = computed(() => {
  if (!summary.value || stale.value) return '待刷新';
  const next = summary.value.next_send_at_ms;
  if (next == null) return '未配置发送间隔';
  const remaining = next - estimatedServerNow.value;
  return remaining > 0 ? `${(remaining / 1000).toFixed(1)} 秒后（预计）` : '已到预计时刻，待刷新';
});
const chartStart = computed(() => (detail.value?.snapshot.observed_at_ms ?? 0) - rangeMs.value);
const chartEnd = computed(() => (detail.value?.snapshot.observed_at_ms ?? 0) + 60000);
const events = computed(() => detail.value?.snapshot.events.filter(event => event.submitted_at_ms >= chartStart.value) ?? []);
const factors = computed(() => summary.value ? [
  ['负载', summary.value.factors.load], ['额度', summary.value.factors.quota],
  ['健康', summary.value.factors.health], ['首输出延迟（汇总）', summary.value.factors.latency],
] as const : []);

/** 将绝对提交时间映射到同一时间轴。@param time UTC 毫秒。@return SVG 横坐标。 */
function x(time: number): number {
  return 30 + Math.min(1, Math.max(0, (time - chartStart.value) / Math.max(1, chartEnd.value - chartStart.value))) * 940;
}

/** 格式化采集时间，缺失不伪装为当前时间。@param time UTC 毫秒。@return 中文本地时间。 */
function timeLabel(time?: number): string {
  return time ? new Date(time).toLocaleTimeString('zh-CN', { hour12: false }) : '未采集';
}

/** 判断某绑定当前是否有请求。@param row 会话行。@return 活跃或闲置标签。 */
function sessionState(row: SchedulingSession): string {
  return Object.values(row.phases).some(n => n > 0) ? '活动中续租' : `${Math.max(0, Math.ceil((row.expires_at_ms - estimatedServerNow.value) / 60000))} 分钟`;
}

/** 只在详情可见时读取，避免旧响应覆盖新账号或新筛选。@return 当前轮加载结束。 */
async function refresh(): Promise<void> {
  if (!props.open || document.hidden || loading.value || props.accountId <= 0) return;
  const current = generation;
  const accountId = props.accountId;
  const requestedPage = page.value;
  const requestedState = state.value;
  const requestedPrefix = appliedPrefix.value;
  const currentController = new AbortController();
  controller = currentController;
  loading.value = true;
  try {
    let [snapshot, rows] = await Promise.all([
      api.accountScheduling(accountId, currentController.signal),
      api.accountSchedulingSessions(accountId, requestedPage, requestedState, requestedPrefix, currentController.signal),
    ]);
    if (current !== generation || !props.open || accountId !== props.accountId) return;
    let resolvedPage = requestedPage;
    if (requestedPage > 1 && (requestedPage - 1) * rows.page_size >= rows.total) {
      // 会话随到期或状态变化减少；最多回读一次第一页，避免留在无法返回的空页。
      rows = await api.accountSchedulingSessions(accountId, 1, requestedState, requestedPrefix, currentController.signal);
      if (current !== generation || !props.open || accountId !== props.accountId) return;
      resolvedPage = 1;
    }
    detail.value = snapshot;
    sessions.value = rows;
    page.value = resolvedPage;
    receivedAt.value = Date.now();
    localNow.value = receivedAt.value;
    error.value = '';
  } catch (cause) {
    if (current === generation && !currentController.signal.aborted) {
      error.value = cause instanceof Error ? cause.message : '读取调度状态失败';
    }
  } finally {
    if (current === generation) {
      loading.value = false;
      controller = null;
    }
  }
}

/** 取消旧读取并清理计时器。@return 无。 */
function stop(): void {
  generation++;
  controller?.abort();
  controller = null;
  loading.value = false;
  if (refreshTimer) clearInterval(refreshTimer);
  if (clockTimer) clearInterval(clockTimer);
  refreshTimer = null;
  clockTimer = null;
}

/** 跟随可见性管理刷新；本地倒计时不发送后端请求。@return 无。 */
function syncVisibility(): void {
  stop();
  if (!props.open || document.hidden) return;
  void refresh();
  refreshTimer = setInterval(() => { void refresh(); }, 5000);
  clockTimer = setInterval(() => { localNow.value = Date.now(); }, 1000);
}

/** 应用有界筛选，不把任意文本送入摘要查询。@return 无。 */
function applyFilter(): void {
  const value = String(prefix.value).trim().toLowerCase();
  if (!/^[0-9a-f]{0,64}$/.test(value)) { error.value = '请输入十六进制会话摘要前缀'; return; }
  appliedPrefix.value = value;
  page.value = 1;
  syncVisibility();
}

/** 翻页并取消旧页的读取。@param value 目标页。@return 无。 */
function changePage(value: number): void {
  if (value < 1 || !sessions.value || value > Math.max(1, Math.ceil(sessions.value.total / 20))) return;
  page.value = value;
  syncVisibility();
}

watch(() => [props.open, props.accountId] as const, () => {
  detail.value = null;
  sessions.value = null;
  error.value = '';
  receivedAt.value = 0;
  page.value = 1;
  prefix.value = '';
  appliedPrefix.value = '';
  state.value = 'all';
  syncVisibility();
}, { immediate: true });
onMounted(() => document.addEventListener('visibilitychange', syncVisibility));
onUnmounted(() => { stop(); document.removeEventListener('visibilitychange', syncVisibility); });
</script>

<template>
  <Dialog :open="open" @update:open="emit('update:open', $event)">
    <DialogContent class="bg-white border-[#e8e2d9] text-[#29261e] rounded-2xl sm:max-w-5xl max-h-[90vh] overflow-y-auto">
      <DialogHeader>
        <DialogTitle>{{ accountName }} · 调度详情</DialogTitle>
        <DialogDescription>只读查看 API 网关绑定与真实提交，每 5 秒刷新。会话数、请求数和上游池槽位分别统计。</DialogDescription>
      </DialogHeader>
      <p v-if="error" role="alert" class="text-sm text-amber-700 bg-amber-50 rounded-lg p-3">{{ error }}<span v-if="detail"> · 保留上次快照，数据已陈旧</span></p>
      <p v-if="!detail && loading" class="text-sm text-[#8c8475]">正在读取调度快照…</p>
      <template v-if="detail && summary">
        <div class="flex flex-wrap justify-between gap-2 text-xs text-[#8c8475]">
          <span>{{ summary.config.mode === 'smart' ? '智能排序' : '旧评分排序' }} · {{ schedulingReasonLabel(summary.capacity.reason) }} · 常规请求容量快照</span>
          <span>{{ stale ? '快照已陈旧' : '观测于 ' + timeLabel(summary.observed_at_ms) }}</span>
        </div>
        <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <div class="rounded-xl border border-[#e8e2d9] p-3"><p class="text-xs text-[#8c8475]">已绑定／活跃会话</p><p class="text-xl mt-1">{{ summary.bound_sessions }} / {{ summary.active_sessions }}</p></div>
          <div class="rounded-xl border border-[#e8e2d9] p-3"><p class="text-xs text-[#8c8475]">正在执行的请求</p><p class="text-xl mt-1">{{ summary.phases.running }}</p></div>
          <div class="rounded-xl border border-[#e8e2d9] p-3"><p class="text-xs text-[#8c8475]">最近 60 秒提交</p><p class="text-xl mt-1">{{ summary.submissions_last_60s }}</p><p v-if="summary.observed_at_ms - summary.history_available_from_ms < 60000" class="text-[10px] text-amber-700">采集不足 60 秒</p></div>
          <div class="rounded-xl border border-[#e8e2d9] p-3"><p class="text-xs text-[#8c8475]">按上次快照最早可发</p><p class="text-sm mt-2">{{ nextLabel }}</p></div>
        </div>
        <div class="rounded-xl bg-[#f9f6f1] p-3 flex flex-wrap gap-x-5 gap-y-2 text-xs">
          <span>等并发 {{ summary.phases.waiting_concurrency }}</span><span>准备环境 {{ summary.phases.preparing }}</span><span>等发送 {{ summary.phases.waiting_send }}</span><span>退避 {{ summary.phases.backoff }}</span><span>无当前绑定请求 {{ summary.unbound_requests }}</span>
        </div>
        <div class="space-y-2">
          <div class="flex flex-wrap justify-between gap-2 items-center">
            <h3 class="text-sm font-semibold">真实提交时间轴</h3>
            <div class="flex gap-2 text-xs"><button :class="rangeMs === 60000 ? 'text-[#c4704f]' : 'text-[#8c8475]'" @click="rangeMs = 60000">最近 1 分钟</button><button :class="rangeMs === 300000 ? 'text-[#c4704f]' : 'text-[#8c8475]'" @click="rangeMs = 300000">最近 5 分钟</button></div>
          </div>
          <svg viewBox="0 0 1000 115" role="img" aria-label="真实提交点和预计最早许可时刻" class="w-full rounded-xl border border-[#e8e2d9] bg-[#fcfaf7]">
            <line x1="30" y1="55" x2="970" y2="55" stroke="#d6cec1" stroke-width="2" />
            <line :x1="x(detail.snapshot.observed_at_ms)" y1="20" :x2="x(detail.snapshot.observed_at_ms)" y2="80" stroke="#8c8475" stroke-dasharray="4 4" />
            <circle v-for="(event, index) in events" :key="event.request_id + ':' + event.submitted_at_ms + ':' + index" :cx="x(event.submitted_at_ms)" cy="55" r="3.5" fill="#059669"><title>{{ timeLabel(event.submitted_at_ms) }} · 已提交</title></circle>
            <template v-if="summary.next_send_at_ms != null && summary.next_send_at_ms > detail.snapshot.observed_at_ms">
              <line :x1="x(summary.next_send_at_ms)" y1="25" :x2="x(summary.next_send_at_ms)" y2="80" stroke="#8b5cf6" stroke-dasharray="3 4" />
              <circle :cx="x(summary.next_send_at_ms)" cy="55" r="6" fill="#fcfaf7" stroke="#8b5cf6" stroke-width="2"><title>预计最早可发，不代表已预订</title></circle>
            </template>
            <text x="30" y="100" fill="#8c8475" font-size="12">{{ rangeMs / 60000 }} 分钟前</text><text :x="x(detail.snapshot.observed_at_ms)" y="100" fill="#8c8475" font-size="12" text-anchor="middle">快照时刻</text><text x="970" y="100" fill="#8c8475" font-size="12" text-anchor="end">未来许可参考</text>
          </svg>
          <p class="text-[11px] text-[#8c8475]">实心点＝Gateway 向 sender 发起提交；空心点＝预计最早许可，不是未来预订。历史从 {{ timeLabel(detail.snapshot.history_available_from_ms) }} 起采集，重启后留空。</p>
          <p v-if="detail.snapshot.history_truncated" class="text-xs text-amber-700">事件缓冲已截断 {{ detail.snapshot.dropped_events }} 条，时间轴不是完整历史；60 秒统计使用独立时间桶。</p>
          <p v-if="events.length === 0" class="text-xs text-[#8c8475]">当前采集窗口内没有记录到提交点。</p>
        </div>
        <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <div v-for="factor in factors" :key="factor[0]" class="text-xs"><div class="flex justify-between mb-1"><span>{{ factor[0] }}</span><span>{{ (factor[1] * 100).toFixed(0) }}</span></div><div class="h-1.5 rounded bg-[#eee7dc]"><div class="h-full rounded bg-[#c4704f]" :style="{ width: Math.max(0, Math.min(100, factor[1] * 100)) + '%' }" /></div></div>
        </div>
        <p class="text-[11px] text-[#8c8475]">归一化偏好越高越优，仅影响新会话。延迟取成功请求的首个有效文字、思考或工具输出；图中为账号汇总，实际选号按模型、思考参数和流式方式分别比较。未知额度或反馈保持中性。</p>
        <div class="space-y-3 border-t border-[#e8e2d9] pt-4">
          <h3 class="text-sm font-semibold">网关绑定会话</h3>
          <div class="flex flex-wrap gap-2">
            <select v-model="state" aria-label="会话活跃状态" class="border border-[#e8e2d9] rounded-md px-2 text-sm" @change="applyFilter"><option value="all">全部绑定</option><option value="active">活跃</option><option value="idle">闲置</option></select>
            <Input v-model="prefix" placeholder="脱敏摘要前缀" aria-label="会话摘要前缀" class="max-w-56 border-[#e8e2d9]" @keyup.enter="applyFilter" />
            <Button variant="outline" @click="applyFilter">筛选</Button>
          </div>
          <div class="overflow-x-auto">
            <table class="w-full text-xs text-left whitespace-nowrap"><thead class="text-[#8c8475]"><tr><th class="p-2">会话摘要</th><th class="p-2">等槽／准备／等发／执行／退避</th><th class="p-2">首次绑定</th><th class="p-2">最近活动</th><th class="p-2">闲置有效期</th></tr></thead><tbody>
              <tr v-for="row in sessions?.items ?? []" :key="row.session_key" class="border-t border-[#f0ebe4]"><td class="p-2 font-mono">{{ row.session_key.slice(0, 12) }}</td><td class="p-2">{{ row.phases.waiting_concurrency }} / {{ row.phases.preparing }} / {{ row.phases.waiting_send }} / {{ row.phases.running }} / {{ row.phases.backoff }}</td><td class="p-2">{{ timeLabel(row.created_at_ms) }}</td><td class="p-2">{{ timeLabel(row.last_seen_at_ms) }}</td><td class="p-2">{{ sessionState(row) }}</td></tr>
              <tr v-if="sessions?.items.length === 0"><td colspan="5" class="p-5 text-center text-[#8c8475]">当前筛选没有有效绑定</td></tr>
            </tbody></table>
          </div>
          <div class="flex items-center justify-between text-xs text-[#8c8475]"><span>共 {{ sessions?.total ?? '—' }} 个绑定；每页 20 个</span><div class="flex items-center gap-2"><Button variant="outline" size="sm" :disabled="page <= 1 || loading" @click="changePage(page - 1)">上一页</Button><span>{{ page }}</span><Button variant="outline" size="sm" :disabled="!sessions || page * 20 >= sessions.total || loading" @click="changePage(page + 1)">下一页</Button></div></div>
          <p class="text-[11px] text-[#8c8475]">摘要按 API Token 隔离；一个会话可有多个请求。无 ID、尚未绑定或已换绑定但仍在旧账号执行的请求计入无当前绑定请求；此处不展示官方聊天内容。</p>
        </div>
      </template>
    </DialogContent>
  </Dialog>
</template>
