<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue';
import NativeStageDetails from './NativeStageDetails.vue';
import { api, type DiagnosticRequest, type DiagnosticAttempt, type NativeCapture } from '../api';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog';

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
  if (!selected.value) return;
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
  return ({ queued: '后台采集已排队', saved: '已采集', disabled: '采集关闭', dropped: '采集繁忙，已丢弃', failed: '采集失败', timeout: '采集超时，保存结果待核对' } as Record<string, string>)[status ?? ''] ?? '采集状态缺失';
}

onMounted(() => { void load(); void loadMode(); });
onUnmounted(() => { mounted = false; listGeneration++; detailGeneration++; listController?.abort(); detailController?.abort(); settingsController.abort(); });
</script>

<template>
  <section class="space-y-4 text-sm text-[#29261e]">
    <h2 class="text-xl font-semibold">请求记录</h2>
    <label class="flex items-center gap-2">日志模式
      <select :value="mode" :disabled="modeSaving || mode === 'unknown'" class="rounded-md border bg-white px-3 py-2" aria-label="日志模式" @change="saveMode">
        <option value="unknown" disabled>读取中</option><option value="info">info · 摘要</option><option value="debug">debug · 脱敏内容</option>
      </select><span v-if="modeSaving" role="status">保存中</span>
    </label>
    <p v-if="modeError" role="alert" class="text-red-700">{{ modeError }}</p>
    <form class="flex flex-wrap items-center gap-2" @submit.prevent="load()">
      <Input v-model="account" placeholder="账号 ID" class="w-28" aria-label="账号 ID" />
      <Input v-model="correlation" placeholder="请求 ID / 响应关联 ID" class="max-w-sm" aria-label="请求关联 ID" />
      <Button type="submit" :disabled="loading">查询 / 刷新</Button>
    </form>
    <p class="text-xs text-[#8c8475]">日志保留 7 天。info 保存摘要；debug 为已开启采集的原生账号追加脱敏阶段原文及处理明细。模式对后续请求生效。每账号最多 50 份、100 MiB，正文最多 2 MiB。</p>
    <p v-if="Object.values(health).some(value => value > 0)" class="text-amber-700">诊断采集健康：{{ health }}</p>
    <p v-if="error" role="alert" class="text-red-700">{{ error }}</p>
    <div class="overflow-auto rounded-xl border border-[#e8e2d9] bg-white">
      <table class="w-full text-left"><thead><tr class="border-b"><th class="p-3">时间 / 请求 ID</th><th>账号</th><th>状态</th><th>尝试</th><th></th></tr></thead>
        <tbody><tr v-for="row in rows" :key="row.request_id" class="border-b"><td class="p-3"><div>{{ row.started_at }}</div><code class="text-xs">{{ row.request_id }}</code><div class="text-xs text-[#8c8475]">{{ modeLabel(row.log_mode) }}</div></td><td>{{ row.account_id ?? '未知' }}</td><td>{{ row.status_code ?? '待终结' }} · {{ row.outcome }}</td><td>{{ row.attempt_count }}</td><td class="p-2"><Button variant="outline" @click="open(row, $event)">详情</Button></td></tr></tbody>
      </table>
      <p v-if="!rows.length && !loading" class="p-4">当前筛选下没有请求记录。</p>
    </div>
    <div class="flex gap-3 items-center"><Button variant="outline" :disabled="loading || page <= 1" @click="load(page - 1)">上一页</Button><span>{{ page }} / {{ Math.max(1, Math.ceil(total / 20)) }} · {{ total }} 条</span><Button variant="outline" :disabled="loading || page * 20 >= total" @click="load(page + 1)">下一页</Button></div>
    <Dialog :open="dialogOpen" @update:open="setDialogOpen">
      <DialogContent class="sm:max-w-5xl max-h-[85vh] flex flex-col" @close-auto-focus="restoreFocus">
        <DialogHeader><DialogTitle class="break-all pr-5">请求详情 · {{ selected?.request_id }}</DialogTitle><DialogDescription>本次请求 {{ modeLabel(selected?.log_mode) }} · 请求分阶段诊断</DialogDescription></DialogHeader>
        <div class="min-h-0 overflow-auto space-y-3 text-sm">
          <p v-if="detailLoading" role="status">读取中…</p>
          <p v-if="detailError" role="alert" class="text-red-700">{{ detailError }}</p>
          <dl v-if="selected" class="grid grid-cols-1 gap-3 rounded-lg border border-[#e8e2d9] p-3 sm:grid-cols-2">
            <div><dt class="text-xs text-[#8c8475]">开始时间</dt><dd>{{ selected.started_at }}</dd></div>
            <div><dt class="text-xs text-[#8c8475]">账号</dt><dd>{{ selected.account_id ?? '未知' }}</dd></div>
            <div><dt class="text-xs text-[#8c8475]">状态 / 结果</dt><dd>{{ selected.status_code ?? '待终结' }} · {{ selected.outcome }}</dd></div>
            <div><dt class="text-xs text-[#8c8475]">尝试数</dt><dd>{{ selected.attempt_count }}</dd></div>
          </dl>
          <p v-if="!attempts.length && !detailLoading && !detailError">此请求没有已记录的上游尝试。</p>
          <div v-for="attempt in attempts" :key="attempt.attempt_id" class="flex flex-wrap gap-3 items-center">
            <span>尝试 {{ attempt.ordinal }} · 账号 {{ attempt.account_id }} · {{ attempt.status_code ?? '未知状态' }} · {{ modeLabel(attempt.log_mode) }} · {{ captureLabel(attempt.capture_status) }}</span>
            <div v-if="attempt.runtime" class="w-full rounded-md border bg-white p-3 space-y-1" data-native-runtime>
              <p>原生进程：{{ attempt.runtime.warm_hit ? '命中热进程' : '本次等待初始化' }} · PID {{ attempt.runtime.worker_pid }}</p>
              <p>准备等待 {{ attempt.runtime.preparation_wait_ms }} ms · 获取进程 {{ attempt.runtime.checkout_ms }} ms · 进程启动 {{ attempt.runtime.worker_start_ms == null ? '沿用现有进程' : `${attempt.runtime.worker_start_ms} ms` }}</p>
              <p>本地准备合计 {{ attempt.runtime.prepared_ms }} ms（包含上述等待，计至组装完成）</p>
            </div>
            <p v-else-if="attempt.profile_selection === 'native293_cc_prepared'" class="w-full text-[#8c8475]">原生进程复用与本地阶段：未观测</p>
            <Button v-if="attempt.profile_selection === 'native293_cc_prepared' && attempt.log_mode !== 'info'" variant="outline" :disabled="detailLoading" @click="inspect(attempt)">查看请求处理详情</Button>
          </div>
          <section v-if="capture" class="space-y-3">
            <h4 class="font-semibold">请求处理详情 · {{ selectedAttempt }}</h4>
            <p v-if="capture.message" role="status">{{ capture.message }}</p>
            <NativeStageDetails v-if="capture.snapshot" :key="selectedAttempt" :snapshot="capture.snapshot" />
          </section>
        </div>
      </DialogContent>
    </Dialog>
  </section>
</template>
