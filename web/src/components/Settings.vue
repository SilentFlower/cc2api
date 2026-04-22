<script setup lang="ts">
import { ref, onMounted, computed } from 'vue';
import { api, type PrimeLogEntry } from '../api';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  TableEmpty,
} from '@/components/ui/table';
import { useToast } from '../composables/useToast';

const { show: toast } = useToast();

/** 权重表单 */
const w7d = ref('0.5');
const w5h = ref('0.3');
const wconc = ref('0.2');

/** 峰值预热表单 */
const primeEnabled = ref(true);
const primeHours = ref('4,5,6');
const primeModel = ref('claude-haiku-4-5-20251001');

/** 预热历史记录 */
const primeLogs = ref<PrimeLogEntry[]>([]);
const logsLoading = ref(false);

const saving = ref(false);
const loaded = ref(false);

/** 权重总和 */
const totalWeight = computed(() => {
  return parseFloat(w7d.value || '0') + parseFloat(w5h.value || '0') + parseFloat(wconc.value || '0');
});

/** 单个权重是否合法（非负有限数） */
function isValidWeight(v: string): boolean {
  const n = parseFloat(v);
  return !isNaN(n) && isFinite(n) && n >= 0;
}

/** 所有权重是否合法 */
const allValid = computed(() => {
  return isValidWeight(w7d.value) && isValidWeight(w5h.value) && isValidWeight(wconc.value);
});

/** 总和是否为 1.0 */
const isValidTotal = computed(() => {
  return Math.abs(totalWeight.value - 1.0) < 0.001;
});

/** 预热小时输入是否合法(逗号分隔的 0-23,允许空) */
const isValidHours = computed(() => {
  const raw = primeHours.value.trim();
  if (!raw) return true;
  return raw.split(',').every((s) => {
    const t = s.trim();
    if (!t) return true;
    const n = parseInt(t, 10);
    return !isNaN(n) && n >= 0 && n <= 23 && String(n) === t;
  });
});

/** 预热模型不能为空 */
const isValidModel = computed(() => primeModel.value.trim().length > 0);

/** 加载设置 */
async function loadSettings() {
  try {
    const data = await api.getSettings();
    w7d.value = data.score_weight_7d ?? '0.5';
    w5h.value = data.score_weight_5h ?? '0.3';
    wconc.value = data.score_weight_concurrency ?? '0.2';
    primeEnabled.value = (data.peak_prime_enabled ?? 'true') === 'true';
    primeHours.value = data.peak_prime_hours ?? '4,5,6';
    primeModel.value = data.peak_prime_model ?? 'claude-haiku-4-5-20251001';
    loaded.value = true;
  } catch (e) {
    toast((e as Error).message || '加载设置失败');
  }
}

/** 加载最近预热记录 */
async function loadPrimeLogs() {
  logsLoading.value = true;
  try {
    primeLogs.value = await api.getPrimeLogs();
  } catch (e) {
    toast((e as Error).message || '加载预热记录失败');
  } finally {
    logsLoading.value = false;
  }
}

/** 保存设置 */
async function saveSettings() {
  if (!allValid.value) {
    toast('权重必须为非负数');
    return;
  }
  if (!isValidHours.value) {
    toast('预热小时必须是逗号分隔的 0-23 整数');
    return;
  }
  if (!isValidModel.value) {
    toast('预热模型不能为空');
    return;
  }
  saving.value = true;
  try {
    await api.updateSettings({
      score_weight_7d: String(w7d.value),
      score_weight_5h: String(w5h.value),
      score_weight_concurrency: String(wconc.value),
      peak_prime_enabled: primeEnabled.value ? 'true' : 'false',
      peak_prime_hours: primeHours.value.trim(),
      peak_prime_model: primeModel.value.trim(),
    });
    toast('保存成功');
  } catch (e) {
    toast((e as Error).message || '保存失败');
  } finally {
    saving.value = false;
  }
}

/** 格式化时间戳 */
function formatTime(raw: string): string {
  if (!raw) return '-';
  const d = new Date(raw);
  if (isNaN(d.getTime())) return raw;
  return d.toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

onMounted(async () => {
  await loadSettings();
  await loadPrimeLogs();
});
</script>

<template>
  <div class="space-y-6">
    <div class="flex items-center justify-between">
      <h2 class="text-lg font-semibold text-[#29261e]">设置</h2>
    </div>

    <!-- 评分权重 -->
    <Card class="bg-white border-[#e8e2d9] rounded-xl overflow-hidden">
      <div class="p-6 space-y-4">
        <div>
          <h3 class="text-sm font-semibold text-[#29261e]">评分权重</h3>
          <p class="text-xs text-[#8c8475] mt-1">
            调度评分公式：score = eff_7d × W1 + eff_5h × W2 + 负载% × W3，分数越低越优先分配。负载% =（活跃+排队）/并发 × 100。时间衰减使用阶梯档位（1.0/0.8/0.6）替代线性比例。
          </p>
        </div>

        <div class="grid grid-cols-3 gap-4">
          <div class="space-y-1.5">
            <Label class="text-[#5c5647] text-sm">7 天窗口 (W1)</Label>
            <Input
              v-model="w7d"
              type="number"
              step="0.05"
              min="0"
              max="1"
              class="border-[#e8e2d9] focus:ring-[#c4704f] text-center"
            />
          </div>
          <div class="space-y-1.5">
            <Label class="text-[#5c5647] text-sm">5 小时窗口 (W2)</Label>
            <Input
              v-model="w5h"
              type="number"
              step="0.05"
              min="0"
              max="1"
              class="border-[#e8e2d9] focus:ring-[#c4704f] text-center"
            />
          </div>
          <div class="space-y-1.5">
            <Label class="text-[#5c5647] text-sm">并发负载 (W3)</Label>
            <Input
              v-model="wconc"
              type="number"
              step="0.05"
              min="0"
              max="1"
              class="border-[#e8e2d9] focus:ring-[#c4704f] text-center"
            />
          </div>
        </div>

        <div class="flex items-center pt-2">
          <p class="text-xs" :class="isValidTotal ? 'text-emerald-600' : 'text-amber-600'">
            权重总和: {{ totalWeight.toFixed(2) }}
            <span v-if="!isValidTotal"> (建议为 1.0)</span>
          </p>
        </div>
      </div>
    </Card>

    <!-- 峰值预热 -->
    <Card class="bg-white border-[#e8e2d9] rounded-xl overflow-hidden">
      <div class="p-6 space-y-4">
        <div>
          <h3 class="text-sm font-semibold text-[#29261e]">峰值预热</h3>
          <p class="text-xs text-[#8c8475] mt-1">
            每天在配置的小时 HH:10（<span class="font-medium">服务器本地时间</span>）对所有活跃账号发送一次小型
            Haiku 请求，主动启动 Anthropic 侧 5h 速率限制窗口，让窗口重置点尽量落在下午高峰前后。失败不重试，所有调用都会记录在下方。
          </p>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div class="space-y-1.5">
            <Label class="text-[#5c5647] text-sm">总开关</Label>
            <label class="flex items-center gap-2 h-9 px-3 rounded-md border border-[#e8e2d9] bg-[#f9f6f1] cursor-pointer select-none">
              <input
                v-model="primeEnabled"
                type="checkbox"
                class="accent-[#c4704f] w-4 h-4"
              />
              <span class="text-sm text-[#29261e]">{{ primeEnabled ? '已启用' : '已关闭' }}</span>
            </label>
          </div>
          <div class="space-y-1.5">
            <Label class="text-[#5c5647] text-sm">预热小时 (逗号分隔)</Label>
            <Input
              v-model="primeHours"
              placeholder="4,5,6"
              class="border-[#e8e2d9] focus:ring-[#c4704f]"
              :class="isValidHours ? '' : 'border-red-400'"
            />
            <p class="text-[11px] text-[#b5b0a6]">触发分钟固定为 :10,例如 4 表示 04:10</p>
          </div>
          <div class="space-y-1.5">
            <Label class="text-[#5c5647] text-sm">预热模型</Label>
            <Input
              v-model="primeModel"
              placeholder="claude-haiku-4-5-20251001"
              class="border-[#e8e2d9] focus:ring-[#c4704f]"
              :class="isValidModel ? '' : 'border-red-400'"
            />
            <p class="text-[11px] text-[#b5b0a6]">建议保留为 Haiku,成本最低</p>
          </div>
        </div>
      </div>
    </Card>

    <!-- 保存按钮 -->
    <div class="flex justify-end">
      <Button
        @click="saveSettings"
        :disabled="saving || !allValid || !isValidHours || !isValidModel"
        class="bg-[#c4704f] hover:bg-[#b5623f] text-white font-medium rounded-xl transition-all duration-200 px-6"
      >
        {{ saving ? '保存中...' : '保存' }}
      </Button>
    </div>

    <!-- 最近预热记录 -->
    <Card class="bg-white border-[#e8e2d9] rounded-xl overflow-hidden">
      <div class="p-6 space-y-4">
        <div class="flex items-center justify-between">
          <div>
            <h3 class="text-sm font-semibold text-[#29261e]">最近预热记录</h3>
            <p class="text-xs text-[#8c8475] mt-1">按触发时间倒序,最多保留 200 条</p>
          </div>
          <Button
            @click="loadPrimeLogs"
            :disabled="logsLoading"
            variant="outline"
            class="border-[#e8e2d9] text-[#5c5647] hover:bg-[#f0ebe4] rounded-lg text-xs h-8"
          >
            {{ logsLoading ? '加载中...' : '刷新' }}
          </Button>
        </div>

        <Table>
          <TableHeader>
            <TableRow>
              <TableHead class="text-[#8c8475]">时间</TableHead>
              <TableHead class="text-[#8c8475]">账号</TableHead>
              <TableHead class="text-[#8c8475]">小时</TableHead>
              <TableHead class="text-[#8c8475]">模型</TableHead>
              <TableHead class="text-[#8c8475]">耗时</TableHead>
              <TableHead class="text-[#8c8475]">结果</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            <TableRow v-for="log in primeLogs" :key="log.id">
              <TableCell class="text-[#29261e] text-xs whitespace-nowrap">{{ formatTime(log.triggered_at) }}</TableCell>
              <TableCell class="text-[#29261e] text-xs">
                <span class="font-medium">{{ log.account_name || `#${log.account_id}` }}</span>
                <span class="text-[#b5b0a6] ml-1">#{{ log.account_id }}</span>
              </TableCell>
              <TableCell class="text-[#5c5647] text-xs">{{ String(log.hour).padStart(2, '0') }}:10</TableCell>
              <TableCell class="text-[#5c5647] text-xs">{{ log.model }}</TableCell>
              <TableCell class="text-[#5c5647] text-xs">{{ log.duration_ms }}ms</TableCell>
              <TableCell>
                <Badge
                  v-if="log.success"
                  class="bg-emerald-50 text-emerald-700 border-emerald-200"
                >
                  成功
                </Badge>
                <Badge
                  v-else
                  class="bg-red-50 text-red-700 border-red-200"
                  :title="log.error_message"
                >
                  失败
                </Badge>
                <span
                  v-if="!log.success && log.error_message"
                  class="ml-2 text-[11px] text-[#b5b0a6] truncate inline-block max-w-[260px] align-middle"
                  :title="log.error_message"
                >
                  {{ log.error_message }}
                </span>
              </TableCell>
            </TableRow>
            <TableEmpty v-if="primeLogs.length === 0" :colspan="6">
              暂无预热记录
            </TableEmpty>
          </TableBody>
        </Table>
      </div>
    </Card>
  </div>
</template>
