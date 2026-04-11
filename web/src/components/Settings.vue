<script setup lang="ts">
import { ref, onMounted, computed } from 'vue';
import { api } from '../api';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { useToast } from '../composables/useToast';

const { show: toast } = useToast();

/** 权重表单 */
const w7d = ref('0.5');
const w5h = ref('0.3');
const wconc = ref('0.2');
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

/** 加载设置 */
async function loadSettings() {
  try {
    const data = await api.getSettings();
    w7d.value = data.score_weight_7d ?? '0.5';
    w5h.value = data.score_weight_5h ?? '0.3';
    wconc.value = data.score_weight_concurrency ?? '0.2';
    loaded.value = true;
  } catch (e) {
    toast((e as Error).message || '加载设置失败');
  }
}

/** 保存设置 */
async function saveSettings() {
  if (!allValid.value) {
    toast('权重必须为非负数');
    return;
  }
  saving.value = true;
  try {
    await api.updateSettings({
      score_weight_7d: w7d.value,
      score_weight_5h: w5h.value,
      score_weight_concurrency: wconc.value,
    });
    toast('保存成功');
  } catch (e) {
    toast((e as Error).message || '保存失败');
  } finally {
    saving.value = false;
  }
}

onMounted(loadSettings);
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
            调度评分公式：score = eff_7d × W1 + eff_5h × W2 + 并发% × W3，分数越低越优先分配。
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

        <div class="flex items-center justify-between pt-2">
          <p class="text-xs" :class="isValidTotal ? 'text-emerald-600' : 'text-amber-600'">
            权重总和: {{ totalWeight.toFixed(2) }}
            <span v-if="!isValidTotal"> (建议为 1.0)</span>
          </p>
          <Button
            @click="saveSettings"
            :disabled="saving || !allValid"
            class="bg-[#c4704f] hover:bg-[#b5623f] text-white font-medium rounded-xl transition-all duration-200 px-6"
          >
            {{ saving ? '保存中...' : '保存' }}
          </Button>
        </div>
      </div>
    </Card>
  </div>
</template>
