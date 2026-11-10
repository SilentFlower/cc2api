<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue';
import { api, type DiagnosticRequest, type DiagnosticAttempt, type NativeCapture } from '../api';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';

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
let controller: AbortController | null = null;
let generation = 0;

/** 开始读取并取消旧请求，防止旧账号内容覆盖当前选择。@return 读取归属。 */
function begin(): { version: number; signal: AbortSignal } {
  controller?.abort();
  controller = new AbortController();
  loading.value = true;
  error.value = '';
  return { version: ++generation, signal: controller.signal };
}

/** 加载有界列表。@param target 目标页。@return 读取结束。 */
async function load(target = 1): Promise<void> {
  const accountId = String(account.value).trim();
  if (accountId && !/^[1-9][0-9]*$/.test(accountId)) { error.value = '请输入正整数账号 ID'; return; }
  const query = new URLSearchParams({ page: String(target), page_size: '20' });
  if (accountId) query.set('account_id', accountId);
  if (correlation.value.trim()) query.set('correlation_id', correlation.value.trim());
  const { version, signal } = begin();
  selected.value = null; attempts.value = []; capture.value = null;
  try {
    const result = await api.requestDiagnostics(query.toString(), signal);
    if (version !== generation) return;
    rows.value = result.items; total.value = result.total; health.value = result.health; page.value = target;
  } catch (cause) {
    if (version === generation && !signal.aborted) error.value = cause instanceof Error ? cause.message : '请求列表读取失败';
  } finally { if (version === generation) loading.value = false; }
}

/** 打开实际请求详情。@param row 当前列表行。@return 尝试读取结束。 */
async function open(row: DiagnosticRequest): Promise<void> {
  const { version, signal } = begin();
  selected.value = row; attempts.value = []; capture.value = null; selectedAttempt.value = '';
  try {
    const result = await api.requestDiagnosticDetail(row.request_id, signal);
    if (version === generation) { selected.value = result.request; attempts.value = result.attempts; }
  } catch (cause) {
    if (version === generation && !signal.aborted) error.value = cause instanceof Error ? cause.message : '请求详情读取失败';
  } finally { if (version === generation) loading.value = false; }
}

/** 按需读取完整组装内容。@param attempt 当前请求的尝试。@return 快照读取结束。 */
async function inspect(attempt: DiagnosticAttempt): Promise<void> {
  if (!selected.value) return;
  const requestId = selected.value.request_id;
  const { version, signal } = begin();
  capture.value = null; selectedAttempt.value = attempt.attempt_id;
  try {
    const result = await api.nativeCapture(requestId, attempt.attempt_id, signal);
    if (version === generation) capture.value = result;
  } catch (cause) {
    if (version === generation && !signal.aborted) error.value = cause instanceof Error ? cause.message : '采集内容读取失败';
  } finally { if (version === generation) loading.value = false; }
}

/** 格式化完整 JSON，截断正文保持原样。@param body 脱敏正文。@return 展示文本。 */
function bodyText(body: string): string {
  try { return JSON.stringify(JSON.parse(body), null, 2); } catch { return body; }
}

/** 显示持久化的采集状态。@param status 尝试采集状态。@return 中文标签。 */
function captureLabel(status?: string | null): string {
  return ({ saved: '已采集', disabled: '采集关闭', dropped: '采集繁忙，已丢弃', failed: '采集失败', timeout: '采集超时，保存结果待核对' } as Record<string, string>)[status ?? ''] ?? '采集状态缺失';
}

onMounted(() => { void load(); });
onUnmounted(() => { generation++; controller?.abort(); });
</script>

<template>
  <section class="space-y-4 text-sm text-[#29261e]">
    <h2 class="text-xl font-semibold">请求记录</h2>
    <form class="flex flex-wrap items-center gap-2" @submit.prevent="load()">
      <Input v-model="account" placeholder="账号 ID" class="w-28" aria-label="账号 ID" />
      <Input v-model="correlation" placeholder="请求 ID / 响应关联 ID" class="max-w-sm" aria-label="请求关联 ID" />
      <Button type="submit" :disabled="loading">查询 / 刷新</Button>
    </form>
    <p class="text-xs text-[#8c8475]">仅为开启采集的原生账号保存 CC 组装后的请求。凭据脱敏。每账号最多 50 份、100 MiB，正文最多 2 MiB。</p>
    <p v-if="Object.values(health).some(value => value > 0)" class="text-amber-700">诊断采集健康：{{ health }}</p>
    <p v-if="error" role="alert" class="text-red-700">{{ error }}</p>
    <div class="overflow-auto rounded-xl border border-[#e8e2d9] bg-white">
      <table class="w-full text-left"><thead><tr class="border-b"><th class="p-3">时间 / 请求 ID</th><th>账号</th><th>状态</th><th>尝试</th><th></th></tr></thead>
        <tbody><tr v-for="row in rows" :key="row.request_id" class="border-b"><td class="p-3"><div>{{ row.started_at }}</div><code class="text-xs">{{ row.request_id }}</code></td><td>{{ row.account_id ?? '未知' }}</td><td>{{ row.status_code ?? '待终结' }} · {{ row.outcome }}</td><td>{{ row.attempt_count }}</td><td class="p-2"><Button variant="outline" @click="open(row)">详情</Button></td></tr></tbody>
      </table>
      <p v-if="!rows.length && !loading" class="p-4">当前筛选下没有请求记录。</p>
    </div>
    <div class="flex gap-3 items-center"><Button variant="outline" :disabled="loading || page <= 1" @click="load(page - 1)">上一页</Button><span>{{ page }} / {{ Math.max(1, Math.ceil(total / 20)) }} · {{ total }} 条</span><Button variant="outline" :disabled="loading || page * 20 >= total" @click="load(page + 1)">下一页</Button></div>
    <section v-if="selected" class="space-y-3 rounded-xl border border-[#e8e2d9] bg-white p-4">
      <h3 class="font-semibold break-all">请求详情 · {{ selected.request_id }}</h3>
      <p v-if="!attempts.length && !loading">此请求没有已记录的上游尝试。</p>
      <div v-for="attempt in attempts" :key="attempt.attempt_id" class="flex flex-wrap gap-3 items-center">
        <span>尝试 {{ attempt.ordinal }} · 账号 {{ attempt.account_id }} · {{ attempt.status_code ?? '未知状态' }} · {{ captureLabel(attempt.capture_status) }}</span>
        <Button v-if="attempt.profile_selection === 'native293_cc_prepared'" variant="outline" :disabled="loading" @click="inspect(attempt)">查看 CC 组装后内容</Button>
      </div>
      <section v-if="capture" class="space-y-3">
        <h4 class="font-semibold">CC 组装后 · {{ selectedAttempt }}</h4>
        <p v-if="capture.message" role="status">{{ capture.message }}</p>
        <template v-if="capture.snapshot">
          <p class="text-xs text-[#8c8475]">原正文 {{ capture.snapshot.original_body_bytes }} 字节 · SHA256 {{ capture.snapshot.original_body_sha256 }}</p>
          <p v-if="capture.snapshot.body_truncated || capture.snapshot.headers_truncated" role="status" class="text-amber-700">内容已截断。正文：{{ capture.snapshot.body_truncated ? '已截断' : '完整' }}，请求头：{{ capture.snapshot.headers_truncated ? '已截断' : '完整' }}。</p>
          <h5 class="font-medium">请求头</h5>
          <div class="overflow-auto"><table class="w-full text-left"><tbody><tr v-for="(header, index) in capture.snapshot.headers" :key="index" class="border-b align-top"><th class="p-2 whitespace-nowrap font-mono font-normal">{{ header[0] }}</th><td class="p-2 break-all whitespace-pre-wrap font-mono">{{ header[1] }}</td></tr></tbody></table></div>
          <h5 class="font-medium">请求正文</h5>
          <pre class="max-h-[60vh] overflow-auto whitespace-pre-wrap break-all rounded-lg bg-[#f7f4ef] p-3 text-xs">{{ bodyText(capture.snapshot.body) }}</pre>
        </template>
      </section>
    </section>
  </section>
</template>
