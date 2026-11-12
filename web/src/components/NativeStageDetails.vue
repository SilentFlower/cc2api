<script setup lang="ts">
import { computed, ref } from 'vue';
import type { NativeCapture, NativeCaptureStage } from '../api';

const props = defineProps<{ snapshot: NonNullable<NativeCapture['snapshot']> }>();
const action = ref('all');
const rawStage = ref<'ingress' | 'cc_prepared'>('cc_prepared');
const decisions = computed(() => (props.snapshot.header_policy?.decisions ?? []).filter(item => action.value === 'all' || item.action === action.value));
const preview = computed(() => rawStage.value === 'ingress' ? props.snapshot.ingress : props.snapshot);
const stages = computed<NativeCaptureStage[]>(() => props.snapshot.stages ?? [
  { stage: 'ingress', source: 'unavailable', representation: 'unavailable', status: 'unavailable', note: '历史记录未保存入口阶段' },
  { stage: 'header_policy', source: 'unavailable', representation: 'unavailable', status: 'unavailable', note: '历史记录未保存逐字段决策' },
  { stage: 'cc_prepared', source: '历史组装快照', representation: 'json_before_native_transport', status: props.snapshot.body_truncated || props.snapshot.headers_truncated ? 'truncated' : 'complete' },
  { stage: 'wire_final', source: 'unavailable', representation: 'unavailable', status: 'unavailable', note: '本次未采集独立最终TLS写出' },
]);

/** 展示实际处理动作。@param value 持久动作。@return 中文标签。 */
function actionLabel(value: string): string {
  return ({ keep: '保留', drop: '删除', runtime: '运行时处理', context: '提取上下文', added: '新增', removed: '删除', changed: '改写' } as Record<string, string>)[value] ?? value;
}

/** 展示阶段及证据完整性。@param value 持久状态或阶段。@return 中文标签。 */
function label(value: string): string {
  return ({ ingress: '入口原文', header_policy: '头部处理', cc_prepared: '原生组装后', wire_final: '最终TLS写出', complete: '完整', truncated: '已截断', unavailable: '未采集', failed: '采集失败', incomplete: '采集不完整' } as Record<string, string>)[value] ?? value;
}

/** 只格式化完整JSON，截断预览保持原始表示。@return 脱敏展示文本。 */
function bodyText(): string {
  const value = preview.value;
  if (!value) return '';
  if (value.body_truncated) return value.body;
  try { return JSON.stringify(JSON.parse(value.body), null, 2); } catch { return value.body; }
}
</script>

<template>
  <div class="space-y-5" data-native-stages>
    <section class="space-y-3 rounded-lg border border-[#e8e2d9] p-3">
      <div class="flex flex-wrap items-center justify-between gap-2">
        <h5 class="font-semibold">头部处理</h5>
        <select v-model="action" aria-label="筛选头部处理动作" class="rounded-md border bg-white px-2 py-1 text-xs">
          <option value="all">全部动作</option><option value="drop">只看删除</option><option value="runtime">运行时处理</option><option value="context">提取上下文</option><option value="keep">保留</option>
        </select>
      </div>
      <template v-if="snapshot.header_policy && snapshot.header_policy.status !== 'unavailable'">
        <p class="text-xs text-[#8c8475]">{{ label(snapshot.header_policy.status) }} · 保存 {{ snapshot.header_policy.stored_count ?? '未观测' }} / 实际 {{ snapshot.header_policy.observed_count ?? '未观测' }} 项 · 来源：内核实际决策</p>
        <div class="overflow-auto"><table class="w-full text-left text-xs"><thead><tr class="border-b"><th class="p-2">字段</th><th class="p-2">动作</th><th class="p-2">原因</th></tr></thead><tbody>
          <tr v-for="(item, index) in decisions" :key="index" class="border-b align-top" :class="item.action === 'drop' ? 'bg-rose-50/60' : ''"><td class="p-2 break-all font-mono">{{ item.name }}</td><td class="p-2 whitespace-nowrap">{{ actionLabel(item.action) }}</td><td class="p-2 break-words">{{ item.reason }}</td></tr>
        </tbody></table></div>
        <p v-if="!decisions.length" class="text-xs text-[#8c8475]">当前筛选下没有已保存的处理项。</p>
      </template>
      <p v-else class="text-xs text-[#8c8475]">{{ snapshot.header_policy?.note ?? '历史记录未保存逐字段处理明细。' }}</p>
    </section>
    <section class="space-y-3 rounded-lg border border-[#e8e2d9] p-3">
      <h5 class="font-semibold">正文变化</h5>
      <template v-if="snapshot.body_changes && snapshot.body_changes.status !== 'unavailable'">
        <p class="text-xs text-[#8c8475]">{{ label(snapshot.body_changes.status) }} · 来源：入口与组装结果的观察差异。路径使用 JSON Pointer。</p>
        <div class="overflow-auto"><table class="w-full text-left text-xs"><thead><tr class="border-b"><th class="p-2">字段路径</th><th class="p-2">变化</th><th class="p-2">原因</th></tr></thead><tbody>
          <tr v-for="(item, index) in snapshot.body_changes.items" :key="index" class="border-b align-top"><td class="p-2 break-all font-mono">{{ item.path || '/' }}</td><td class="p-2 whitespace-nowrap">{{ actionLabel(item.action) }}</td><td class="p-2 break-words">{{ item.reason }}</td></tr>
        </tbody></table></div>
        <p v-if="!snapshot.body_changes.items.length" class="text-xs text-[#8c8475]">本次观察到的正文内容保持一致。</p>
      </template>
      <p v-else class="text-xs text-[#8c8475]">{{ snapshot.body_changes?.note ?? '历史记录未保存正文变化证据。' }}</p>
    </section>
    <section class="space-y-3">
      <h5 class="font-semibold">分阶段采集</h5>
      <div class="grid grid-cols-1 gap-2 sm:grid-cols-2">
        <article v-for="stage in stages" :key="stage.stage" class="min-w-0 rounded-lg border border-[#e8e2d9] bg-[#fbfaf7] p-3 text-xs">
          <div class="flex justify-between gap-2"><strong>{{ label(stage.stage) }}{{ stage.stage === 'cc_prepared' ? '（中间值）' : '' }}</strong><span>{{ label(stage.status) }}</span></div>
          <p v-if="stage.note" class="mt-2 text-[#8c8475]">{{ stage.note }}</p>
          <p class="mt-2 break-all text-[#8c8475]">来源：{{ stage.source }} · {{ stage.representation }}</p>
        </article>
      </div>
    </section>
    <section class="space-y-3 border-t border-[#e8e2d9] pt-4">
      <label class="flex items-center gap-2 text-xs">查看脱敏原文
        <select v-model="rawStage" aria-label="原文阶段" class="rounded-md border bg-white px-2 py-1"><option value="ingress">入口原文</option><option value="cc_prepared">原生组装后（中间值）</option></select>
      </label>
      <template v-if="preview">
        <p class="break-all text-xs text-[#8c8475]">原正文 {{ preview.original_body_bytes ?? '未观测' }} 字节 · SHA256 {{ preview.original_body_sha256 ?? '未观测' }}</p>
        <p v-if="preview.body_truncated || preview.headers_truncated" role="status" class="text-xs text-amber-700">内容已截断。正文：{{ preview.body_truncated ? '已截断' : '完整' }}，请求头：{{ preview.headers_truncated ? '已截断' : '完整' }}。</p>
        <h5 class="text-xs font-medium">请求头</h5>
        <div class="overflow-auto"><table class="w-full text-left text-xs"><tbody><tr v-for="(header, index) in preview.headers" :key="index" class="border-b align-top"><th class="p-2 font-mono font-normal break-all">{{ header[0] }}</th><td class="p-2 break-all whitespace-pre-wrap font-mono">{{ header[1] }}</td></tr></tbody></table></div>
        <h5 class="text-xs font-medium">请求正文</h5>
        <pre class="max-h-[50vh] overflow-auto whitespace-pre-wrap break-all rounded-lg bg-[#f7f4ef] p-3 text-xs">{{ bodyText() }}</pre>
      </template>
      <p v-else class="text-xs text-[#8c8475]">本次未保存所选阶段的原文。</p>
    </section>
  </div>
</template>
