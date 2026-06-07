<script setup lang="ts">
import { ref, onMounted, computed } from 'vue';
import { api, type PrimeLogEntry } from '../api';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { Textarea } from '@/components/ui/textarea';
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

/** 允许 messages[].role=system 的模型列表 */
const allowSystemRoleModels = ref('claude-opus-4-8');

/** 客户端访问策略表单 */
const allowedClaudeCodeVersions = ref('2.1.89-2.1.156');
const allowedUserAgents = ref('AI-Hub-Monitor*\npython-httpx*');

/** 系统提示词环境字段「真值透传」开关(工作目录默认透传) */
const passthroughShell = ref(false);
const passthroughOsVersion = ref(false);
const passthroughWorkingDir = ref(true);

/** Anthropic cache_control TTL 改写模式 */
const cacheControlTtlRewrite = ref<'off' | '5m' | '1h'>('off');

/** Claude Code messages 缓存断点改写模式 */
const messageCacheControlRewrite = ref<'off' | 'auto' | 'rolling' | 'stateful'>('off');

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

/** 系统角色模型列表是否合法 */
const isValidSystemRoleModels = computed(() => {
  const raw = allowSystemRoleModels.value.trim();
  if (!raw) return true;
  return raw.split(',').every((s) => {
    const model = s.trim();
    return !model || /^[A-Za-z0-9._:-]+$/.test(model);
  });
});

/** Claude Code 版本范围是否合法 */
const isValidClaudeCodeVersions = computed(() => {
  const raw = allowedClaudeCodeVersions.value.trim();
  if (!raw) return true;
  return raw.split(/[,\n\r]+/).every((s) => {
    const item = s.trim();
    if (!item) return true;
    const version = '\\d+(?:\\.\\d+)*';
    const exact = new RegExp(`^${version}$`);
    const wildcard = new RegExp(`^${version}\\.\\*$`);
    const range = new RegExp(`^${version}-${version}$`);
    return exact.test(item) || wildcard.test(item) || range.test(item);
  });
});

/** UA pattern 列表是否合法 */
const isValidAllowedUserAgents = computed(() => {
  const raw = allowedUserAgents.value.trim();
  if (!raw) return true;
  return raw.split(/[,\n\r]+/).every((s) => {
    const pattern = s.trim();
    return !pattern || /^[\x20-\x7E]+$/.test(pattern);
  });
});

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
    allowSystemRoleModels.value = data.allow_system_role_models ?? 'claude-opus-4-8';
    allowedClaudeCodeVersions.value = data.allowed_claude_code_versions ?? '2.1.89-2.1.156';
    allowedUserAgents.value = data.allowed_user_agents ?? 'AI-Hub-Monitor*\npython-httpx*';
    passthroughShell.value = (data.passthrough_shell ?? 'false') === 'true';
    passthroughOsVersion.value = (data.passthrough_os_version ?? 'false') === 'true';
    passthroughWorkingDir.value = (data.passthrough_working_dir ?? 'true') === 'true';
    const ttlRewrite = data.cache_control_ttl_rewrite ?? 'off';
    cacheControlTtlRewrite.value = ttlRewrite === '5m' || ttlRewrite === '1h' ? ttlRewrite : 'off';
    const messageCacheRewrite = data.message_cache_control_rewrite ?? 'off';
    if (messageCacheRewrite === 'auto' || messageCacheRewrite === 'rolling' || messageCacheRewrite === 'stateful') {
      messageCacheControlRewrite.value = messageCacheRewrite;
    } else if (messageCacheRewrite === 'stable' || messageCacheRewrite === 'anchored') {
      messageCacheControlRewrite.value = 'auto';
    } else {
      messageCacheControlRewrite.value = 'off';
    }
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
  if (!isValidSystemRoleModels.value) {
    toast('系统角色模型列表包含非法字符');
    return;
  }
  if (!isValidClaudeCodeVersions.value) {
    toast('Claude Code 版本范围格式不正确');
    return;
  }
  if (!isValidAllowedUserAgents.value) {
    toast('UA 白名单包含非法字符');
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
      allow_system_role_models: allowSystemRoleModels.value.trim(),
      allowed_claude_code_versions: allowedClaudeCodeVersions.value.trim(),
      allowed_user_agents: allowedUserAgents.value.trim(),
      passthrough_shell: passthroughShell.value ? 'true' : 'false',
      passthrough_os_version: passthroughOsVersion.value ? 'true' : 'false',
      passthrough_working_dir: passthroughWorkingDir.value ? 'true' : 'false',
      cache_control_ttl_rewrite: cacheControlTtlRewrite.value,
      message_cache_control_rewrite: messageCacheControlRewrite.value,
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

/** 判断一条日志是否"跳过"而非真正失败。
 *  后端把冷却期跳过统一用 'skipped: ...' 前缀写入 error_message,
 *  前端依前缀识别,渲染成琥珀色"跳过"徽章,避免与红色"失败"混淆。 */
function isSkipped(log: PrimeLogEntry): boolean {
  return !log.success && log.error_message.startsWith('skipped:');
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

    <!-- 系统角色模型白名单 -->
    <Card class="bg-white border-[#e8e2d9] rounded-xl overflow-hidden">
      <div class="p-6 space-y-4">
        <div>
          <h3 class="text-sm font-semibold text-[#29261e]">系统角色模型</h3>
        </div>

        <div class="space-y-2">
          <Label class="text-[#5c5647] text-sm">允许模型 (逗号分隔)</Label>
          <Input
            v-model="allowSystemRoleModels"
            placeholder="claude-opus-4-8"
            class="border-[#e8e2d9] focus:ring-[#c4704f] font-mono text-sm"
            :class="isValidSystemRoleModels ? '' : 'border-red-400'"
          />
          <div class="flex flex-wrap gap-1.5">
            <span class="text-xs text-[#b5b0a6] self-center">预设:</span>
            <button
              type="button"
              @click="allowSystemRoleModels = 'claude-opus-4-8'"
              class="px-2 py-0.5 text-xs rounded border border-[#e8e2d9] bg-[#f9f6f1] text-[#8c8475] hover:border-emerald-300 hover:bg-emerald-50 hover:text-emerald-600 transition-colors"
            >Opus 4.8</button>
            <button
              type="button"
              @click="allowSystemRoleModels = ''"
              class="px-2 py-0.5 text-xs rounded border border-[#e8e2d9] bg-[#f9f6f1] text-[#8c8475] hover:border-red-300 hover:bg-red-50 hover:text-red-600 transition-colors"
            >全部关闭</button>
          </div>
        </div>
      </div>
    </Card>

    <!-- 系统提示词环境透传 -->
    <Card class="bg-white border-[#e8e2d9] rounded-xl overflow-hidden">
      <div class="p-6 space-y-4">
        <div>
          <h3 class="text-sm font-semibold text-[#29261e]">环境透传(指纹)</h3>
          <p class="text-xs text-[#8c8475] mt-1">
            控制系统提示词 <span class="font-mono">&lt;env&gt;</span> 块中各字段是否使用客户端真实值。开启后该字段不再改写为账号预设,让模型识别真实环境;这三项仅存在于请求体,不影响请求头/遥测。<span class="font-medium">Platform 不在此列(跨通道字段,始终锁定)</span>。
          </p>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div class="space-y-1.5">
            <Label class="text-[#5c5647] text-sm">Shell</Label>
            <label class="flex items-center gap-2 h-9 px-3 rounded-md border border-[#e8e2d9] bg-[#f9f6f1] cursor-pointer select-none">
              <input
                v-model="passthroughShell"
                type="checkbox"
                class="accent-[#c4704f] w-4 h-4"
              />
              <span class="text-sm text-[#29261e]">{{ passthroughShell ? '透传真实值' : '改写为预设' }}</span>
            </label>
          </div>
          <div class="space-y-1.5">
            <Label class="text-[#5c5647] text-sm">OS Version</Label>
            <label class="flex items-center gap-2 h-9 px-3 rounded-md border border-[#e8e2d9] bg-[#f9f6f1] cursor-pointer select-none">
              <input
                v-model="passthroughOsVersion"
                type="checkbox"
                class="accent-[#c4704f] w-4 h-4"
              />
              <span class="text-sm text-[#29261e]">{{ passthroughOsVersion ? '透传真实值' : '改写为预设' }}</span>
            </label>
          </div>
          <div class="space-y-1.5">
            <Label class="text-[#5c5647] text-sm">Working directory</Label>
            <label class="flex items-center gap-2 h-9 px-3 rounded-md border border-[#e8e2d9] bg-[#f9f6f1] cursor-pointer select-none">
              <input
                v-model="passthroughWorkingDir"
                type="checkbox"
                class="accent-[#c4704f] w-4 h-4"
              />
              <span class="text-sm text-[#29261e]">{{ passthroughWorkingDir ? '透传真实值' : '改写为预设' }}</span>
            </label>
          </div>
        </div>
        <p class="text-[11px] text-[#b5b0a6]">
          开启 OS Version / Working directory 透传时,请确保账号预设平台与真机系统一致,否则会出现 Platform 与系统/路径不同系的矛盾。工作目录默认透传以避免误导模型对真实 cwd 的判断。
        </p>
      </div>
    </Card>

    <!-- Anthropic 缓存改写 -->
    <Card class="bg-white border-[#e8e2d9] rounded-xl overflow-hidden">
      <div class="p-6 space-y-6">
        <div>
          <h3 class="text-sm font-semibold text-[#29261e]">Anthropic 缓存改写</h3>
          <p class="text-xs text-[#8c8475] mt-1">
            分别控制 Claude Code messages 缓存断点位置和 ephemeral cache_control.ttl。默认不改写,保持客户端原始缓存策略。
          </p>
        </div>

        <div class="space-y-3">
          <div>
            <Label class="text-[#5c5647] text-sm">messages 缓存断点</Label>
            <p class="text-[11px] text-[#b5b0a6] mt-1">
              会话防污染会记住同一 Claude Code session 的正常主线断点,并忽略并行 tool 或停止恢复触发的异常暴涨请求。
            </p>
          </div>
          <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
            <label class="flex items-center gap-2 h-9 px-3 rounded-md border border-[#e8e2d9] bg-[#f9f6f1] cursor-pointer select-none">
              <input
                v-model="messageCacheControlRewrite"
                type="radio"
                value="off"
                class="accent-[#c4704f] w-4 h-4"
              />
              <span class="text-sm text-[#29261e]">保持原样</span>
            </label>
            <label class="flex items-center gap-2 h-9 px-3 rounded-md border border-[#e8e2d9] bg-[#f9f6f1] cursor-pointer select-none">
              <input
                v-model="messageCacheControlRewrite"
                type="radio"
                value="auto"
                class="accent-[#c4704f] w-4 h-4"
              />
              <span class="text-sm text-[#29261e]">自动修复</span>
            </label>
            <label class="flex items-center gap-2 h-9 px-3 rounded-md border border-[#e8e2d9] bg-[#f9f6f1] cursor-pointer select-none">
              <input
                v-model="messageCacheControlRewrite"
                type="radio"
                value="stateful"
                class="accent-[#c4704f] w-4 h-4"
              />
              <span class="text-sm text-[#29261e]">会话防污染</span>
            </label>
            <label class="flex items-center gap-2 h-9 px-3 rounded-md border border-[#e8e2d9] bg-[#f9f6f1] cursor-pointer select-none">
              <input
                v-model="messageCacheControlRewrite"
                type="radio"
                value="rolling"
                class="accent-[#c4704f] w-4 h-4"
              />
              <span class="text-sm text-[#29261e]">滚动断点</span>
            </label>
          </div>
        </div>

        <div class="space-y-3">
          <div>
            <Label class="text-[#5c5647] text-sm">cache_control.ttl</Label>
            <p class="text-[11px] text-[#b5b0a6] mt-1">
              仅改写请求体里已经存在或由 messages 缓存断点策略创建的 ephemeral cache_control.ttl。
            </p>
          </div>
          <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
            <label class="flex items-center gap-2 h-9 px-3 rounded-md border border-[#e8e2d9] bg-[#f9f6f1] cursor-pointer select-none">
              <input
                v-model="cacheControlTtlRewrite"
                type="radio"
                value="off"
                class="accent-[#c4704f] w-4 h-4"
              />
              <span class="text-sm text-[#29261e]">不改写</span>
            </label>
            <label class="flex items-center gap-2 h-9 px-3 rounded-md border border-[#e8e2d9] bg-[#f9f6f1] cursor-pointer select-none">
              <input
                v-model="cacheControlTtlRewrite"
                type="radio"
                value="5m"
                class="accent-[#c4704f] w-4 h-4"
              />
              <span class="text-sm text-[#29261e]">强制 5m</span>
            </label>
            <label class="flex items-center gap-2 h-9 px-3 rounded-md border border-[#e8e2d9] bg-[#f9f6f1] cursor-pointer select-none">
              <input
                v-model="cacheControlTtlRewrite"
                type="radio"
                value="1h"
                class="accent-[#c4704f] w-4 h-4"
              />
              <span class="text-sm text-[#29261e]">强制 1h</span>
            </label>
          </div>
        </div>
        <p class="text-[11px] text-[#b5b0a6]">
          这些设置只影响 Anthropic /v1/messages 转发,发生在 CCH attestation 重新计算之前。off 可作为回滚开关。
        </p>
      </div>
    </Card>

    <!-- 客户端访问策略 -->
    <Card class="bg-white border-[#e8e2d9] rounded-xl overflow-hidden">
      <div class="p-6 space-y-4">
        <div>
          <h3 class="text-sm font-semibold text-[#29261e]">客户端访问策略</h3>
          <p class="text-xs text-[#8c8475] mt-1">
            Claude Code / CLI 按版本范围校验；其他客户端按 UA 白名单校验。空值表示对应限制关闭。
          </p>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div class="space-y-2">
            <Label class="text-[#5c5647] text-sm">Claude Code 版本范围</Label>
            <Textarea
              v-model="allowedClaudeCodeVersions"
              rows="4"
              placeholder="2.1.89-2.1.156"
              class="border-[#e8e2d9] focus:ring-[#c4704f] font-mono text-sm"
              :class="isValidClaudeCodeVersions ? '' : 'border-red-400'"
            />
            <p class="text-[11px] text-[#b5b0a6]">支持精确版本、2.1.*、2.1.89-2.1.156；逗号或换行分隔</p>
            <div class="flex flex-wrap gap-1.5">
              <span class="text-xs text-[#b5b0a6] self-center">预设:</span>
              <button
                type="button"
                @click="allowedClaudeCodeVersions = '2.1.89-2.1.156'"
                class="px-2 py-0.5 text-xs rounded border border-[#e8e2d9] bg-[#f9f6f1] text-[#8c8475] hover:border-emerald-300 hover:bg-emerald-50 hover:text-emerald-600 transition-colors"
              >2.1.89-2.1.156</button>
              <button
                type="button"
                @click="allowedClaudeCodeVersions = ''"
                class="px-2 py-0.5 text-xs rounded border border-[#e8e2d9] bg-[#f9f6f1] text-[#8c8475] hover:border-red-300 hover:bg-red-50 hover:text-red-600 transition-colors"
              >关闭版本限制</button>
            </div>
          </div>

          <div class="space-y-2">
            <Label class="text-[#5c5647] text-sm">其他允许 UA</Label>
            <Textarea
              v-model="allowedUserAgents"
              rows="4"
              placeholder="AI-Hub-Monitor*&#10;python-httpx*"
              class="border-[#e8e2d9] focus:ring-[#c4704f] font-mono text-sm"
              :class="isValidAllowedUserAgents ? '' : 'border-red-400'"
            />
            <p class="text-[11px] text-[#b5b0a6]">支持 * 通配；只用于非 claude-code/claude-cli 客户端</p>
            <div class="flex flex-wrap gap-1.5">
              <span class="text-xs text-[#b5b0a6] self-center">预设:</span>
              <button
                type="button"
                @click="allowedUserAgents = 'AI-Hub-Monitor*\npython-httpx*'"
                class="px-2 py-0.5 text-xs rounded border border-[#e8e2d9] bg-[#f9f6f1] text-[#8c8475] hover:border-emerald-300 hover:bg-emerald-50 hover:text-emerald-600 transition-colors"
              >默认 UA</button>
              <button
                type="button"
                @click="allowedUserAgents = ''"
                class="px-2 py-0.5 text-xs rounded border border-[#e8e2d9] bg-[#f9f6f1] text-[#8c8475] hover:border-red-300 hover:bg-red-50 hover:text-red-600 transition-colors"
              >关闭 UA 限制</button>
            </div>
          </div>
        </div>
      </div>
    </Card>

    <!-- 保存按钮 -->
    <div class="flex justify-end">
      <Button
        @click="saveSettings"
        :disabled="saving || !allValid || !isValidHours || !isValidModel || !isValidSystemRoleModels || !isValidClaudeCodeVersions || !isValidAllowedUserAgents"
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
                  v-else-if="isSkipped(log)"
                  class="bg-amber-50 text-amber-700 border-amber-200"
                  :title="log.error_message"
                >
                  跳过
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
