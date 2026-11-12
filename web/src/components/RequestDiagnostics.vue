<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue';
import { Copy, Filter, RefreshCw, Search, FileText, ArrowUpRight } from 'lucide-vue-next';
import { formatLocalTime } from '../lib/time';
import { useToast } from '../composables/useToast';
import NativeStageDetails from './NativeStageDetails.vue';
import { api, type DiagnosticRequest, type DiagnosticAttempt, type NativeCapture } from '../api';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog';

const toast = useToast();
const filtersOpen = ref(false);
const outcome = ref('');
const since = ref('');
const until = ref('');
const tabs = [{id: 'overview', label: '概览'}, {id: 'chain', label: '请求链路'}, {id: 'performance', label: '性能'}, {id: 'raw', label: '原始数据'}] as const;
const activeTab = ref<typeof tabs[number]['id']>('overview');
const account = ref('');
const correlation = ref('');
const page = ref(1);
const total = ref(0);
const rows = ref<DiagnosticRequest[]>([]);
const selected = ref<DiagnosticRequest | null>(null);
const attempts = ref<DiagnosticAttempt[]>([]);
const capture = ref<NativeCapture | null>(null);
const selectedAttempt = ref('');
const health = ref<Record<string, number>>({});
const error = ref('');
const loading = ref(false);
const dialogOpen = ref(false);
const detailLoading = ref(false);
const detailError = ref('');
const mode = ref<'info' | 'debug' | 'unknown'>('unknown');
const modeSaving = ref(false);
const modeError = ref('');
let listController: AbortController | null = null;
let detailController: AbortController | null = null;
const settingsController = new AbortController();
let listGeneration = 0;
let detailGeneration = 0;
let opener: HTMLElement | null = null;
let mounted = true;

/** 开始读取并取消旧请求，防止旧账号内容覆盖当前选择。@return 读取归属。 */
function begin(): { version: number; signal: AbortSignal } {
  detailController?.abort();
  detailController = new AbortController();
  detailLoading.value = true;
  detailError.value = '';
  return { version: ++detailGeneration, signal: detailController.signal };
}

/** 加载有界列表。@param target 目标页。@return 读取结束。 */
async function load(target = 1): Promise<void> {
  const accountId = String(account.value).trim();
  if (accountId && !/^[1-9][0-9]*$/.test(accountId)) { error.value = '请输入正整数账号 ID'; return; }
  const query = new URLSearchParams({ page: String(target), page_size: '20' });
  if (accountId) query.set('account_id', accountId);
  if (outcome.value) query.set('outcome', outcome.value);
  for (const [key, value] of [['since', since.value], ['until', until.value]]) {
    if (value) {
      const time = new Date(value);
      if (!Number.isFinite(time.getTime())) { error.value = '请输入有效时间'; return; }
      query.set(key!, time.toISOString());
    }
  }
  if (correlation.value.trim()) query.set('correlation_id', correlation.value.trim());
  listController?.abort(); listController = new AbortController();
  const signal = listController.signal, version = ++listGeneration;
  loading.value = true; error.value = '';
  try {
    const result = await api.requestDiagnostics(query.toString(), signal);
    if (version !== listGeneration) return;
    rows.value = result.items; total.value = result.total; health.value = result.health; page.value = target;
  } catch (cause) {
    if (version === listGeneration && !signal.aborted) error.value = cause instanceof Error ? cause.message : '请求列表读取失败';
  } finally { if (version === listGeneration) loading.value = false; }
}

/** 打开实际请求详情。@param row 当前列表行。@return 尝试读取结束。 */
async function open(row: DiagnosticRequest, event: MouseEvent): Promise<void> {
  opener = event.currentTarget instanceof HTMLElement ? event.currentTarget : null;
  activeTab.value = 'overview';
  dialogOpen.value = true;
  const { version, signal } = begin();
  selected.value = row; attempts.value = []; capture.value = null; selectedAttempt.value = '';
  try {
    const result = await api.requestDiagnosticDetail(row.request_id, signal);
    if (version === detailGeneration && dialogOpen.value) { selected.value = result.request; attempts.value = result.attempts; }
  } catch (cause) {
    if (version === detailGeneration && !signal.aborted) detailError.value = cause instanceof Error ? cause.message : '请求详情读取失败';
  } finally { if (version === detailGeneration) detailLoading.value = false; }
}

/** 按需读取完整组装内容。@param attempt 当前请求的尝试。@return 快照读取结束。 */
async function inspect(attempt: DiagnosticAttempt): Promise<void> {
  if (!selected.value || !canInspect(attempt)) return;
  activeTab.value = 'raw';
  const requestId = selected.value.request_id;
  const { version, signal } = begin();
  capture.value = null; selectedAttempt.value = attempt.attempt_id;
  try {
    // 后台副本允许晚于业务响应；只重读同次私有证据，沿原取消代次阻止迟到覆盖。
    for (let poll = 0; poll < 6; poll++) {
      const result = await api.nativeCapture(requestId, attempt.attempt_id, signal);
      if (version !== detailGeneration || !dialogOpen.value || signal.aborted) return;
      capture.value = result;
      const pending = result.snapshot?.wire_final?.reason === 'CAPTURE_PENDING'
        || (attempt.capture_status === 'queued' && result.state === 'missing');
      if (!pending || poll === 5) break;
      await new Promise(resolve => setTimeout(resolve, 500));
      if (version !== detailGeneration || !dialogOpen.value || signal.aborted) return;
    }
  } catch (cause) {
    if (version === detailGeneration && !signal.aborted) detailError.value = cause instanceof Error ? cause.message : '采集内容读取失败';
  } finally { if (version === detailGeneration) detailLoading.value = false; }
}

/** 同步弹窗状态并作废关闭前的读取。@param value 新状态。@return 无返回值。 */
function setDialogOpen(value: boolean): void {
  dialogOpen.value = value;
  if (!value) {
    detailGeneration++; detailController?.abort(); detailLoading.value = false;
    selected.value = null; attempts.value = []; capture.value = null; selectedAttempt.value = ''; detailError.value = '';
  }
}

/** 将焦点返回打开详情的列表按钮。@param event 组件焦点事件。@return 无返回值。 */
function restoreFocus(event: Event): void {
  event.preventDefault();
  if (opener?.isConnected) opener.focus();
}

/** 读取实际设置，首次失败保持未知状态。@return 设置读取结束。 */
async function loadMode(): Promise<void> {
  try {
    const settings = await api.getSettings(settingsController.signal);
    if (mounted) mode.value = settings.request_diagnostics_log_mode === 'debug' ? 'debug' : 'info';
  } catch (cause) {
    if (mounted && !settingsController.signal.aborted) modeError.value = cause instanceof Error ? cause.message : '日志模式读取失败';
  }
}

/** 保存后续请求使用的模式。@param event 原生选择事件。@return 保存结束。 */
async function saveMode(event: Event): Promise<void> {
  const target = event.target as HTMLSelectElement;
  const value = target.value;
  if (value !== 'info' && value !== 'debug') return;
  modeSaving.value = true; modeError.value = '';
  try {
    await api.updateSettings({ request_diagnostics_log_mode: value });
    if (mounted) mode.value = value;
  } catch (cause) {
    if (mounted) modeError.value = cause instanceof Error ? cause.message : '日志模式保存失败';
  } finally {
    if (mounted) { modeSaving.value = false; target.value = mode.value; }
  }
}

/** 展示每次请求冻结的模式。@param value 实际模式。@return 中文标签。 */
function modeLabel(value?: string | null): string {
  return value === 'info' ? 'info（摘要）' : value === 'debug' ? 'debug（脱敏内容）' : '历史模式未知';
}

/** 显示持久化的采集状态。@param status 尝试采集状态。@return 中文标签。 */
function captureLabel(status?: string | null): string {
  return ({ queued: '后台采集已排队', saved: '已采集', disabled: '采集已关闭', dropped: '采集繁忙，已丢弃', failed: '采集失败', timeout: '采集超时，保存结果待核对' } as Record<string, string>)[status ?? ''] ?? '采集状态缺失';
}


const currentAttempt = computed(() => attempts.value.find(item => item.attempt_id === selectedAttempt.value) ?? attempts.value[0]);

/** 按冻结状态判断是否提供私有文件读取。@param attempt 当前尝试。@return 是否可读取。 */
function canInspect(attempt: DiagnosticAttempt): boolean {
  return attempt.profile_selection === 'native293_cc_prepared' && attempt.log_mode !== 'info'
    && [null, undefined, 'saved', 'queued', 'timeout'].includes(attempt.capture_status);
}

/** 显示响应结果，保留未知服务端状态。@param value 持久结果。@return 中文标签。 */
function outcomeLabel(value: string): string {
  return ({success: '成功', in_progress: '处理中', incomplete: '未完整结束', cancelled: '已取消',
    upstream_error: '上游错误', local_rejected: '本地拒绝', body_error: '响应读取失败',
    timeout: '已超时', mock: '模拟响应', cache_hit: '命中缓存'} as Record<string, string>)[value] ?? value;
}

/** 结果色只依据持久终态。@param value 持久结果。@return 样式类。 */
function outcomeTone(value: string): string {
  return ['success', 'cache_hit'].includes(value) ? 'diagnostic-success' : value === 'in_progress' ? 'diagnostic-pending' : 'diagnostic-error';
}

/** 从发生时起止时间计算请求总时长。@param row 请求记录。@return 毫秒或未知。 */
function elapsed(row: DiagnosticRequest): number | null {
  if (!row.finished_at) return null;
  const duration = Date.parse(row.finished_at) - Date.parse(row.started_at);
  return Number.isFinite(duration) && duration >= 0 ? duration : null;
}

/** 格式化实测耗时并区分空值。@param value 毫秒。@return 可读耗时。 */
function milliseconds(value?: number | null): string {
  return value == null ? '未观测' : value >= 1000 ? `${(value / 1000).toFixed(2)} s` : `${value} ms`;
}

/** 复制当前可见的安全标识。@param value 已脱敏标识。@return 复制结束。 */
async function copy(value: string): Promise<void> {
  try { await navigator.clipboard.writeText(value); toast.show('已复制', 'success'); }
  catch { toast.show('复制失败，请选择文本手动复制'); }
}

/** 切换尝试时清理原快照与在途读取。@return 无返回值。 */
function changeAttempt(): void {
  detailGeneration++; detailController?.abort(); detailLoading.value = false;
  capture.value = null; detailError.value = '';
}

/** 页签支持左右方向键和首尾导航。@param event 键盘事件。@return 无返回值。 */
function tabKeys(event: KeyboardEvent): void {
  const index = tabs.findIndex(tab => tab.id === activeTab.value);
  const next = event.key === 'ArrowRight' ? (index + 1) % tabs.length
    : event.key === 'ArrowLeft' ? (index + tabs.length - 1) % tabs.length
    : event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1 : -1;
  if (next < 0) return;
  event.preventDefault(); activeTab.value = tabs[next]!.id;
  (event.currentTarget as HTMLElement).querySelectorAll<HTMLButtonElement>('[role="tab"]')[next]?.focus();
}

onMounted(() => { void load(); void loadMode(); });
onUnmounted(() => { mounted = false; listGeneration++; detailGeneration++; listController?.abort(); detailController?.abort(); settingsController.abort(); });
</script>

<template>
  <section class="space-y-5 text-sm text-foreground" aria-label="请求记录">
    <header class="flex flex-wrap items-start justify-between gap-4">
      <div><h1 class="text-2xl font-bold tracking-tight">请求记录</h1><p class="mt-1 text-muted-foreground">查看请求结果、处理链路与脱敏阶段证据</p></div>
      <label class="flex items-center gap-2 text-xs text-muted-foreground">后续请求日志
        <select :value="mode" :disabled="modeSaving || mode === 'unknown'" class="h-9 rounded-lg border bg-card px-3 text-foreground" aria-label="日志模式" @change="saveMode">
          <option value="unknown" disabled>读取中</option><option value="info">info · 摘要</option><option value="debug">debug · 脱敏内容</option>
        </select><span v-if="modeSaving" role="status">保存中</span>
      </label>
    </header>
    <p v-if="modeError" role="alert" class="text-destructive">{{ modeError }}</p>
    <form class="space-y-3" @submit.prevent="load()">
      <div class="flex flex-wrap items-center gap-2">
        <div class="relative min-w-48 flex-1 sm:max-w-md"><Search class="pointer-events-none absolute left-3 top-2.5 size-4 text-muted-foreground" /><Input v-model="correlation" placeholder="搜索请求 ID / 响应关联 ID" class="h-9 pl-9 bg-card" aria-label="请求关联 ID" /></div>
        <Button type="button" variant="outline" class="h-9 shadow-none" :aria-expanded="filtersOpen" aria-controls="request-filters" @click="filtersOpen = !filtersOpen"><Filter />筛选<span v-if="account || outcome || since || until" class="size-1.5 rounded-full bg-primary"></span></Button>
        <Button type="submit" variant="outline" :disabled="loading" class="h-9 shadow-none"><RefreshCw class="size-4" :class="loading ? 'animate-spin motion-reduce:animate-none' : ''" />刷新</Button>
        <span class="ml-auto text-xs text-muted-foreground tabular-nums">{{ total }} 条记录 · 保留 7 天</span>
      </div>
      <div v-if="filtersOpen" id="request-filters" class="flex flex-wrap gap-3 rounded-xl border bg-muted/40 p-4">
        <label class="space-y-1 text-xs text-muted-foreground">账号 ID<Input v-model="account" class="w-28 bg-card" aria-label="账号 ID" placeholder="全部账号" /></label>
        <label class="space-y-1 text-xs text-muted-foreground">请求结果<select v-model="outcome" class="block h-9 rounded-md border bg-card px-3 text-foreground" aria-label="请求结果"><option value="">全部结果</option><option value="success">成功</option><option value="in_progress">处理中</option><option value="incomplete">未完整结束</option><option value="local_rejected">本地拒绝</option></select></label>
        <label class="space-y-1 text-xs text-muted-foreground">开始时间<input v-model="since" type="datetime-local" aria-label="开始时间" class="block h-9 rounded-md border bg-card px-2 text-foreground" /></label>
        <label class="space-y-1 text-xs text-muted-foreground">结束时间<input v-model="until" type="datetime-local" aria-label="结束时间" class="block h-9 rounded-md border bg-card px-2 text-foreground" /></label>
        <Button type="button" variant="ghost" class="self-end" @click="account = ''; outcome = ''; since = ''; until = ''; load()">重置</Button>
      </div>
    </form>
    <p v-if="Object.values(health).some(value => value > 0)" role="status" class="rounded-lg border border-amber-200 bg-amber-50 p-3 text-amber-800">诊断采集健康：{{ health }}</p>
    <p v-if="error" role="alert" class="text-destructive">{{ error }}</p>
    <div class="relative overflow-x-auto rounded-xl border bg-card">
      <table class="w-full text-left sm:whitespace-nowrap"><thead class="bg-muted/40 text-xs text-muted-foreground"><tr><th class="px-4 py-3 font-medium">时间 / 请求</th><th class="hidden px-3 py-3 font-medium sm:table-cell">账号</th><th class="px-3 py-3 font-medium">结果</th><th class="hidden px-3 py-3 font-medium sm:table-cell">总耗时</th><th class="hidden px-3 py-3 font-medium sm:table-cell">尝试</th><th class="px-4 py-3 font-medium"><span class="sr-only">操作</span></th></tr></thead>
        <tbody class="divide-y"><tr v-for="row in rows" :key="row.request_id" class="hover:bg-muted/35">
          <td class="max-w-48 px-3 py-3 sm:max-w-none sm:px-4"><time :datetime="row.started_at" :title="row.started_at" class="block whitespace-normal break-words text-[11px] sm:whitespace-nowrap sm:text-xs">{{ formatLocalTime(row.started_at) }}</time><div class="mt-1 flex items-center gap-2"><code class="text-xs text-muted-foreground" :title="row.request_id">{{ row.request_id.slice(0, 8) }}…</code><button type="button" :aria-label="`复制请求 ${row.request_id}`" class="text-muted-foreground hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring" @click="copy(row.request_id)"><Copy class="size-3" /></button><span class="text-[11px] text-muted-foreground">{{ row.log_mode ?? '历史模式未知' }}</span><span class="text-[11px] text-muted-foreground sm:hidden">账号 {{ row.account_id ?? '未知' }}</span></div></td>
          <td class="hidden px-3 font-mono text-xs sm:table-cell">{{ row.account_id ?? '未知' }}</td>
          <td class="px-3"><button type="button" class="diagnostic-badge" :class="outcomeTone(row.outcome)" @click="open(row, $event)">{{ row.status_code ?? '待终结' }} · {{ outcomeLabel(row.outcome) }}</button></td>
          <td class="hidden px-3 font-mono text-xs sm:table-cell">{{ milliseconds(elapsed(row)) }}</td><td class="hidden px-3 font-mono text-xs sm:table-cell">{{ row.attempt_count }}</td>
          <td class="px-4 text-right"><Button variant="ghost" size="sm" aria-label="详情" @click="open(row, $event)"><span class="hidden sm:inline">详情</span><ArrowUpRight class="size-3.5" /></Button></td>
        </tr></tbody>
      </table>
      <div v-if="!rows.length" class="flex flex-col items-center gap-2 p-12 text-muted-foreground"><FileText class="size-7 opacity-50" /><p>{{ loading ? '正在读取记录…' : '当前筛选下没有请求记录' }}</p></div>
    </div>
    <div class="flex flex-wrap items-center justify-between gap-3"><p class="text-xs text-muted-foreground">info 保存摘要；debug 配合账号内容采集开关保存脱敏原文。</p><div class="flex items-center gap-3"><Button size="sm" variant="outline" :disabled="loading || page <= 1" @click="load(page - 1)">上一页</Button><span class="text-xs tabular-nums text-muted-foreground">{{ page }} / {{ Math.max(1, Math.ceil(total / 20)) }}</span><Button size="sm" variant="outline" :disabled="loading || page * 20 >= total" @click="load(page + 1)">下一页</Button></div></div>
    <Dialog :open="dialogOpen" @update:open="setDialogOpen">
      <DialogContent class="sm:max-w-4xl max-h-[88dvh] flex flex-col gap-0 overflow-hidden p-0" @close-auto-focus="restoreFocus">
        <DialogHeader class="shrink-0 border-b px-5 py-4 pr-12"><DialogTitle class="flex items-center gap-2 text-lg">请求详情<span v-if="selected" class="diagnostic-badge text-xs" :class="outcomeTone(selected.outcome)">{{ selected.status_code ?? '待终结' }} · {{ outcomeLabel(selected.outcome) }}</span></DialogTitle><DialogDescription class="flex items-center gap-2 min-w-0"><code class="min-w-0 truncate text-xs" :title="selected?.request_id">{{ selected?.request_id }}</code><button v-if="selected" type="button" aria-label="复制完整请求 ID" class="shrink-0 hover:text-foreground" @click="copy(selected.request_id)"><Copy class="size-3" /></button></DialogDescription></DialogHeader>
        <div role="tablist" aria-label="请求详情分类" class="flex shrink-0 gap-1 overflow-x-auto border-b px-4" @keydown="tabKeys"><button v-for="tab in tabs" :id="`request-tab-${tab.id}`" :key="tab.id" type="button" role="tab" :aria-selected="activeTab === tab.id" :aria-controls="`request-panel-${tab.id}`" :tabindex="activeTab === tab.id ? 0 : -1" class="whitespace-nowrap border-b-2 px-3 py-3 text-sm transition-colors" :class="activeTab === tab.id ? 'border-primary font-medium text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'" @click="activeTab = tab.id">{{ tab.label }}</button></div>
        <div class="min-h-0 overflow-y-auto px-5 py-5 text-sm">
          <p v-if="detailLoading" role="status" class="mb-3 text-muted-foreground">读取中…</p><p v-if="detailError" role="alert" class="mb-3 text-destructive">{{ detailError }}</p>
          <section v-if="selected" :id="`request-panel-${activeTab}`" role="tabpanel" :aria-labelledby="`request-tab-${activeTab}`" tabindex="0" class="space-y-4 outline-none">
            <template v-if="activeTab === 'overview'">
              <div class="grid grid-cols-2 gap-3 sm:grid-cols-4"><div class="diagnostic-metric"><span>请求总耗时</span><strong>{{ milliseconds(elapsed(selected)) }}</strong></div><div class="diagnostic-metric"><span>尝试数</span><strong>{{ selected.attempt_count }}</strong></div><div class="diagnostic-metric"><span>账号</span><strong>{{ selected.account_id ?? '未知' }}</strong></div><div class="diagnostic-metric"><span>日志模式</span><strong>{{ selected.log_mode ?? '未知' }}</strong></div></div>
              <dl class="diagnostic-definition"><dt>开始时间</dt><dd :title="selected.started_at">{{ formatLocalTime(selected.started_at) }}</dd><dt>结束时间</dt><dd :title="selected.finished_at ?? undefined">{{ formatLocalTime(selected.finished_at) }}</dd><dt>响应关联 ID</dt><dd><code>{{ selected.response_request_id ?? '未观测' }}</code></dd><dt>错误阶段</dt><dd>{{ selected.error_stage ?? '未记录错误阶段' }}</dd></dl>
              <div v-for="attempt in attempts" :key="attempt.attempt_id" class="rounded-xl border p-4"><div class="flex flex-wrap items-center justify-between gap-2"><h3 class="font-medium">尝试 {{ attempt.ordinal }} · 账号 {{ attempt.account_id }}</h3><span class="text-xs text-muted-foreground">{{ captureLabel(attempt.capture_status) }}</span></div><p class="mt-2 text-xs text-muted-foreground">{{ attempt.parameters?.model ?? attempt.response_model ?? '模型未观测' }} · {{ modeLabel(attempt.log_mode) }}</p><p v-if="attempt.capture_status === 'disabled'" class="mt-3 text-muted-foreground">本次请求未保存阶段原文。请核对全局 debug 日志级别与账号内容采集设置。</p><router-link :to="{name: 'environments', query: {gateway_account: String(attempt.account_id)}}" class="mt-2 inline-flex items-center gap-1 text-xs text-primary">管理该账号内容采集<ArrowUpRight class="size-3" /></router-link><Button v-if="canInspect(attempt)" variant="outline" size="sm" class="ml-3 mt-2" :disabled="detailLoading" @click="inspect(attempt)">查看脱敏处理详情</Button></div>
              <p v-if="!attempts.length && !detailLoading" class="text-muted-foreground">此请求没有已记录的上游尝试。</p>
            </template>
            <template v-else-if="activeTab === 'chain'">
              <article v-for="attempt in attempts" :key="attempt.attempt_id" class="rounded-xl border p-4"><div class="flex flex-wrap justify-between gap-2"><h3 class="font-medium">尝试 {{ attempt.ordinal }} · 账号 {{ attempt.account_id }}</h3><span class="diagnostic-badge" :class="outcomeTone(attempt.outcome)">{{ attempt.status_code ?? '未观测' }} · {{ outcomeLabel(attempt.outcome) }}</span></div><dl class="diagnostic-definition mt-4"><dt>转发方式</dt><dd>{{ attempt.profile_selection }}</dd><dt>画像版本</dt><dd>{{ attempt.profile_version ?? '未观测' }}</dd><dt>上游模型</dt><dd>{{ attempt.response_model ?? '未观测' }}</dd><dt>上游请求 ID</dt><dd>{{ attempt.upstream_request_id ?? '未观测' }}</dd><dt>重试原因</dt><dd>{{ attempt.retry_reason ?? '未记录重试' }}</dd><dt>错误阶段</dt><dd>{{ attempt.error_stage ?? '未记录错误阶段' }}</dd><dt>真实会话摘要</dt><dd>{{ attempt.sessions?.real_session_hash ?? '未观测' }}</dd><dt>上游会话摘要</dt><dd>{{ attempt.sessions?.upstream_session_hash ?? '未观测' }}</dd></dl></article>
            </template>
            <template v-else-if="activeTab === 'performance'">
              <article v-for="attempt in attempts" :key="attempt.attempt_id" class="space-y-4 rounded-xl border p-4"><h3 class="font-medium">尝试 {{ attempt.ordinal }} · 账号 {{ attempt.account_id }}</h3><div class="grid grid-cols-2 gap-3 sm:grid-cols-3"><div class="diagnostic-metric"><span>响应头耗时</span><strong>{{ milliseconds(attempt.ttft_ms) }}</strong></div><div class="diagnostic-metric"><span>首次有效输出</span><strong>{{ milliseconds(attempt.first_output_ms) }}</strong></div><div class="diagnostic-metric"><span>本次尝试总耗时</span><strong>{{ milliseconds(attempt.duration_ms) }}</strong></div></div>
                <div v-if="attempt.runtime" class="space-y-2 rounded-lg bg-muted/50 p-3 text-xs" data-native-runtime><p class="font-medium">原生进程：{{ attempt.runtime.warm_hit ? '命中热进程' : '本次等待初始化' }} · PID {{ attempt.runtime.worker_pid }}</p><p>准备等待 {{ attempt.runtime.preparation_wait_ms }} ms · 获取进程 {{ attempt.runtime.checkout_ms }} ms · 进程启动 {{ attempt.runtime.worker_start_ms == null ? '沿用现有进程' : `${attempt.runtime.worker_start_ms} ms` }}</p><p>本地准备合计 {{ attempt.runtime.prepared_ms }} ms（包含上述等待，计至组装完成）</p><p class="text-muted-foreground">本地阶段存在包含关系；准备耗时单独展示。</p></div><p v-else class="text-xs text-muted-foreground">原生进程与本地阶段：未观测</p>
                <dl class="diagnostic-definition"><dt>输入 tokens</dt><dd>{{ attempt.tokens?.input_tokens ?? '未观测' }}</dd><dt>输出 tokens</dt><dd>{{ attempt.tokens?.output_tokens ?? '未观测' }}</dd><dt>缓存读取 tokens</dt><dd>{{ attempt.tokens?.cache_read_tokens ?? '未观测' }}</dd><dt>缓存写入 tokens</dt><dd>{{ attempt.tokens?.cache_write_tokens ?? '未观测' }}</dd></dl>
              </article>
            </template>
            <template v-else>
              <div class="flex flex-wrap items-center gap-2"><label class="text-xs text-muted-foreground">当前尝试<select :value="currentAttempt?.attempt_id" class="ml-2 h-8 rounded-lg border bg-card px-2 text-foreground" aria-label="当前尝试" @change="selectedAttempt = ($event.target as HTMLSelectElement).value; changeAttempt()"><option v-for="attempt in attempts" :key="attempt.attempt_id" :value="attempt.attempt_id">尝试 {{ attempt.ordinal }} · 账号 {{ attempt.account_id }}</option></select></label><Button v-if="currentAttempt && canInspect(currentAttempt)" size="sm" variant="outline" :disabled="detailLoading" @click="inspect(currentAttempt)">{{ capture ? '刷新阶段证据' : '读取阶段证据' }}</Button></div>
              <div v-if="currentAttempt && !canInspect(currentAttempt)" class="rounded-xl border bg-muted/40 p-4"><h3 class="font-medium">{{ currentAttempt.log_mode === 'info' ? '本次仅保存摘要' : captureLabel(currentAttempt.capture_status) }}</h3><p class="mt-2 text-muted-foreground">完整阶段原文需要账号内容采集开启及 debug 日志模式。设置对后续请求生效。</p><router-link :to="{name: 'environments', query: {gateway_account: String(currentAttempt.account_id)}}" class="mt-3 inline-flex text-primary">前往该账号环境配置 →</router-link></div>
              <section v-if="capture" class="space-y-3"><h3 class="font-medium">阶段证据 · {{ capture.state === 'missing' ? '文件缺失或已清理' : capture.state === 'expired' ? '已超过保留期' : capture.state === 'available' ? '已读取' : capture.state }}</h3><p v-if="capture.message" role="status" class="text-muted-foreground">{{ capture.message }}</p><NativeStageDetails v-if="capture.snapshot" :key="selectedAttempt" :snapshot="capture.snapshot" /></section>
              <details class="rounded-xl border"><summary class="cursor-pointer px-4 py-3 text-xs text-muted-foreground">已脱敏诊断字段 · 原始时间</summary><pre class="max-h-80 overflow-auto border-t bg-muted/30 p-4 text-xs">{{ JSON.stringify({ request: selected, attempts }, null, 2) }}</pre></details>
            </template>
          </section>
        </div>
      </DialogContent>
    </Dialog>
  </section>
</template>

<style scoped>
.diagnostic-badge { display: inline-flex; align-items: center; gap: .35rem; border: 1px solid var(--border); border-radius: 6px; padding: 3px 8px; font-size: 12px; font-weight: 500; white-space: nowrap; }
button.diagnostic-badge { cursor: pointer; }
.diagnostic-badge:focus-visible { outline: 2px solid var(--ring); outline-offset: 2px; }
.diagnostic-success { background: #effaf3; border-color: #d5eedf; color: #237348; }
.diagnostic-pending { background: #eff6ff; border-color: #dbeafe; color: #2563eb; }
.diagnostic-error { background: #fff4f2; border-color: #fee0d8; color: #b4533e; }
.diagnostic-metric { min-width: 0; border: 1px solid var(--border); border-radius: 10px; padding: 12px; background: var(--card); }
.diagnostic-metric span { display: block; color: var(--muted-foreground); font-size: 11px; }
.diagnostic-metric strong { display: block; margin-top: 6px; font: 600 16px var(--font-mono); overflow-wrap: anywhere; }
.diagnostic-definition { display: grid; grid-template-columns: max-content minmax(0, 1fr); gap: 12px 20px; font-size: 12px; }
.diagnostic-definition dt { color: var(--muted-foreground); }
.diagnostic-definition dd { min-width: 0; overflow-wrap: anywhere; }
@media (max-width: 480px) { .diagnostic-definition { grid-template-columns: 1fr; gap: 4px; } .diagnostic-definition dd { margin-bottom: 8px; } }
</style>
