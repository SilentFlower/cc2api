<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue';
import { ApiError, api, type SystemUpdateCatalog, type SystemUpdateRequest, type SystemUpdateStatus, type SystemUpdateRelease } from '../api';
import { Download, ChevronDown, ChevronRight, RefreshCw, Box, Layers, Cpu, Check } from 'lucide-vue-next';
import { formatLocalTime } from '../lib/time';
import { platformVersion, releaseNotes, releaseVersion } from '../lib/platformVersion';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { useToast } from '../composables/useToast';

const toast = useToast();
const historyOpen = ref(false);
const expanded = ref('');
const status = ref<SystemUpdateStatus | null>(null);
const catalog = ref<SystemUpdateCatalog | null>(null);
const error = ref('');
const loading = ref(true);
const writing = ref(false);
const checking = ref(false);
const conflicted = ref(false);
const sent = ref(false);
const dialog = ref<HTMLDialogElement | null>(null);
const minutes = ref(15);
const frozen = ref<{ action: 'install' | 'rollback' | 'recover'; body: SystemUpdateRequest; accountChanged: boolean; label: string } | null>(null);
let alive = true;
let timer: ReturnType<typeof setTimeout> | undefined;
let controller: AbortController | undefined;
let failures = 0;

const phases: Record<string, string> = {
  snapshotting: '保存数据库快照', migrating: '执行数据库迁移', restoring_data: '正在恢复数据库',
  preparing: '下载与校验', prepared: '候选已准备', draining: '等待在途完成', switching: '切换平台服务',
  verifying: '验证新版本', committing: '保存安装结果', cleaning: '回收旧制品', succeeded: '更新完成',
  succeeded_cleanup_pending: '更新完成，清理待重试', restoring: '正在恢复原版本', restored: '已恢复原版本',
  failed: '本次更新失败', cancelled: '本次更新已取消', reconcile: '等待恢复与对账',
};
const progress = computed(() => status.value?.operation);
const cancelAllowed = computed(() => status.value?.busy && ['preparing', 'draining'].includes(progress.value?.phase ?? ''));
const recoveryAllowed = computed(() => ['reconcile', 'succeeded_cleanup_pending'].includes(progress.value?.phase ?? ''));
const runningEnvironments = computed(() => status.value?.environments?.filter(item => item.running).length ?? 0);
const environmentSources = computed(() => {
  const counts = new Map<string, number>();
  for (const item of status.value?.environments ?? []) {
    const source = item.verified ? item.release ?? '已停止，启动时使用所选镜像' : '来源待对账';
    counts.set(source, (counts.get(source) ?? 0) + 1);
  }
  return Array.from(counts, ([release, count]) => ({ release, count }));
});

const prominent = computed(() => (catalog.value?.releases ?? []).filter((release, index) => index === 0 || release.release === status.value?.current_release));
const older = computed(() => (catalog.value?.releases ?? []).filter(release => !prominent.value.includes(release)));
const displayed = computed(() => historyOpen.value ? [...prominent.value, ...older.value] : prominent.value);
const currentVersion = computed(() => platformVersion ?? (catalog.value?.releases.find(item => item.release === status.value?.current_release)
  ? releaseVersion(catalog.value.releases.find(item => item.release === status.value?.current_release)!.changes) : null) ?? '历史构建');

/** 使用受信版本标题；旧来源保留历史构建语义。@param id 原发行身份。@return 人类可读版本。 */
function releaseLabel(id: string): string {
  const release = catalog.value?.releases.find(item => item.release === id);
  if (release) return releaseVersion(release.changes) ?? `历史构建 · ${formatLocalTime(release.published_at)}`;
  return id === status.value?.current_release ? currentVersion.value : '历史构建';
}

/** 版本列表只保留一个详情展开。@param id 当前条目标识。@return 无返回值。 */
function toggleDetails(id: string): void { expanded.value = expanded.value === id ? '' : id; }

/** 显示受信发布说明的首段摘要。@param release 完整发行摘要。@return 短说明。 */
function summary(release: SystemUpdateRelease): string { return releaseNotes(release.changes).split('\n')[0] ?? ''; }

/** 为安全和普通 HTTP 环境生成 UUID。@return 规范 UUID。 */
function operationId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6]! & 15) | 64;
  bytes[8] = (bytes[8]! & 63) | 128;
  const text = Array.from(bytes, value => value.toString(16).padStart(2, '0')).join('');
  return `${text.slice(0, 8)}-${text.slice(8, 12)}-${text.slice(12, 16)}-${text.slice(16, 20)}-${text.slice(20)}`;
}

/** 读取缓存状态并串行安排下一次轮询。@return 本轮读取完成。 */
async function refresh(): Promise<void> {
  if (!alive) return;
  clearTimeout(timer);
  if (controller) return;
  controller = new AbortController();
  const signal = controller.signal;
  try {
    const current = await api.systemUpdateStatus(signal);
    const available = current.installed ? await api.systemUpdateReleases(signal) : null;
    if (!alive) return;
    status.value = current;
    catalog.value = available;
    error.value = '';
    failures = 0;
  } catch (cause) {
    if (!alive || signal.aborted) return;
    failures += 1;
    error.value = status.value?.busy ? '平台正在切换，正在重新连接…'
      : cause instanceof Error ? cause.message : '系统更新状态暂不可用';
  } finally {
    controller = undefined;
    if (alive) {
      loading.value = false;
      timer = setTimeout(refresh, failures ? Math.min(30000, 2000 * 2 ** Math.min(failures, 4)) : status.value?.busy || catalog.value?.checking ? 2000 : 10000);
    }
  }
}

/** 手动检查只更新提示。@return 检查入队完成。 */
async function check(): Promise<void> {
  if (checking.value || writing.value) return;
  checking.value = true;
  try {
    await api.systemUpdateCheck();
    toast.show('正在检查受信版本', 'success');
    await refresh();
  } catch (cause) {
    toast.show(cause instanceof Error ? cause.message : '检查版本失败');
  } finally {
    checking.value = false;
  }
}

/** 冻结现场及目标后显示确认，轮询保持原参数。@param action 动作。@param release 版本。@param digest 摘要。@param accountChanged 环境影响。@return 弹窗就绪。 */
async function confirm(action: 'install' | 'rollback' | 'recover', release: string, digest: string, accountChanged = true): Promise<void> {
  if (!status.value?.revision || writing.value || (status.value.busy && action !== 'recover')) return;
  minutes.value = 15;
  conflicted.value = false;
  sent.value = false;
  frozen.value = { action, accountChanged, label: releaseLabel(release), body: { operation_id: action === 'recover' ? progress.value!.operation_id : operationId(),
    expected_revision: status.value.revision, target_release: release, target_manifest_sha256: digest, drain_timeout_seconds: 900 } };
  await nextTick();
  dialog.value?.showModal();
}

/** 关闭确认框。@return 无业务写入。 */
function closeConfirm(): void {
  if (writing.value) return;
  dialog.value?.close();
  frozen.value = null;
}

/** 提交冻结请求，网络不确定时保留同一个操作 ID。@return 本次提交完成。 */
async function execute(): Promise<void> {
  const selected = frozen.value;
  if (!selected || writing.value || conflicted.value) return;
  const wait = Number(minutes.value);
  if (!Number.isInteger(wait) || wait < 1 || wait > 120) {
    toast.show('等待期限应为 1～120 分钟');
    return;
  }
  if (!sent.value && selected.body.drain_timeout_seconds !== wait * 60) {
    // 参数变化使用新身份；首次发送后锁定期限，重试保持整份冻结参数。
    selected.body.drain_timeout_seconds = wait * 60;
  }
  writing.value = true;
  sent.value = true;
  try {
    if (selected.action === 'recover') {
      await api.systemUpdateControl(selected.body.operation_id, 'recover', selected.body.expected_revision);
    } else {
      await api.systemUpdateInstall(selected.action, selected.body);
    }
    if (alive) {
      dialog.value?.close();
      frozen.value = null;
      toast.show('操作已登记，进度会持续保存', 'success');
      await refresh();
    }
  } catch (cause) {
    if (alive) {
      conflicted.value = cause instanceof ApiError && cause.status === 409;
      toast.show(cause instanceof Error ? cause.message : '提交结果待核对，请重试同一操作');
    }
  } finally {
    if (alive) writing.value = false;
  }
}

/** 取消安全阶段的等待。@return 原操作收口请求完成。 */
async function cancel(): Promise<void> {
  if (!cancelAllowed.value || !status.value?.revision || !progress.value || writing.value) return;
  writing.value = true;
  try {
    await api.systemUpdateControl(progress.value.operation_id, 'cancel', status.value.revision);
    if (!alive) return;
    toast.show('已请求取消，正在核对原版本', 'success');
    await refresh();
  } catch (cause) {
    if (alive) toast.show(cause instanceof Error ? cause.message : '取消结果待核对');
  } finally {
    writing.value = false;
  }
}

/** 恢复连接并读取当前现场。@return 初始化完成。 */
async function connect(): Promise<void> {
  try {
    await api.openRuntime();
    await refresh();
  } catch (cause) {
    if (alive) {
      error.value = cause instanceof Error ? cause.message : '连接更新服务失败';
      loading.value = false;
    }
  }
}

onMounted(connect);
onUnmounted(() => { alive = false; clearTimeout(timer); controller?.abort(); dialog.value?.close(); });
</script>

<template>
  <section class="space-y-6 text-foreground" aria-label="系统更新">
    <header class="flex flex-wrap items-start justify-between gap-4"><div><h1 class="text-2xl font-bold tracking-tight">系统更新</h1><p class="mt-1 text-sm text-muted-foreground">查看当前版本与变更，选择合适的时间安装</p></div><Button variant="outline" class="h-9 shadow-none" :disabled="checking || catalog?.checking || !status?.installed" @click="check"><RefreshCw class="size-4" :class="checking || catalog?.checking ? 'animate-spin motion-reduce:animate-none' : ''" />{{ checking || catalog?.checking ? '正在检查…' : '检查更新' }}</Button></header>
    <p v-if="error" role="alert" class="rounded-xl border border-amber-200 bg-amber-50 p-4 text-sm text-amber-900">{{ error }}</p>
    <p v-if="loading" class="py-10 text-center text-muted-foreground">正在读取系统版本…</p>
    <Card v-else-if="status && !status.installed" class="gap-3 p-5 shadow-none"><h2 class="font-semibold">首次接入系统更新</h2><p class="text-sm text-muted-foreground">{{ status.message }}</p><p class="text-sm text-muted-foreground">接入独立更新服务及维护底座后，即可安装后续版本。</p></Card>
    <template v-if="status?.installed">
      <div class="grid gap-4 md:grid-cols-3">
        <Card class="min-w-0 gap-0 p-5 shadow-none"><div class="flex items-center gap-2 text-xs text-muted-foreground"><Box class="size-4" /><h2>平台版本</h2><span class="ml-auto rounded-md bg-primary/10 px-2 py-1 text-primary">当前运行</span></div><p class="mt-4 text-3xl font-semibold tracking-tight">{{ currentVersion }}</p><button type="button" class="mt-3 w-fit text-xs text-muted-foreground hover:text-foreground" :aria-expanded="expanded === 'current'" @click="toggleDetails('current')">{{ expanded === 'current' ? '收起' : '查看' }}构建详情</button><dl v-if="expanded === 'current'" class="mt-3 space-y-2 border-t pt-3 text-xs"><dt class="text-muted-foreground">发行标识</dt><dd class="break-all font-mono">{{ status.current_release }}</dd><dt class="text-muted-foreground">控制组件</dt><dd class="break-all font-mono">{{ status.components?.control.release }}</dd></dl></Card>
        <Card class="min-w-0 gap-0 p-5 shadow-none"><div class="flex items-center gap-2 text-xs text-muted-foreground"><Layers class="size-4" /><h2>账号运行环境</h2></div><p class="mt-4 text-3xl font-semibold tabular-nums">{{ runningEnvironments }}<span class="ml-2 text-sm font-normal text-muted-foreground">个运行中</span></p><router-link :to="{name: 'environments'}" class="mt-3 w-fit text-xs text-primary">管理环境 →</router-link><details class="mt-3 text-xs text-muted-foreground"><summary class="cursor-pointer">镜像来源</summary><p class="mt-2 break-all font-mono">所选：{{ status.components?.account.release }}</p><p v-for="source in environmentSources" :key="source.release" class="mt-2 break-all">{{ source.release }} · {{ source.count }} 个</p></details></Card>
        <Card class="min-w-0 gap-0 p-5 shadow-none"><div class="flex items-center gap-2 text-xs text-muted-foreground"><Cpu class="size-4" /><h2>CLI 与请求内核</h2></div><p class="mt-4 text-xl font-semibold">按账号独立管理</p><p class="mt-2 text-xs text-muted-foreground">各账号显示实际 CLI 版本和内核选择</p><router-link :to="{name: 'environments'}" class="mt-3 w-fit text-xs text-primary">查看版本与切换入口 →</router-link></Card>
      </div>
      <section v-if="progress" class="rounded-xl border bg-card" aria-live="polite">
        <div class="flex flex-wrap items-center justify-between gap-3 px-5 py-4"><div class="flex min-w-0 items-center gap-3"><span class="flex size-8 shrink-0 items-center justify-center rounded-full bg-primary/10 text-primary"><Check v-if="progress.phase === 'succeeded'" class="size-4" /><RefreshCw v-else class="size-4" :class="status.busy ? 'animate-spin motion-reduce:animate-none' : ''" /></span><div><h2 class="text-sm font-semibold">{{ phases[progress.phase] ?? progress.phase }}</h2><p class="mt-1 text-xs text-muted-foreground">{{ releaseLabel(progress.target_release) }}</p></div></div><div class="flex gap-2"><Button v-if="cancelAllowed" variant="outline" size="sm" :disabled="writing || progress.cancel_requested" @click="cancel">{{ progress.cancel_requested ? '正在取消…' : '取消等待' }}</Button><Button v-if="recoveryAllowed" variant="outline" size="sm" :disabled="writing" @click="confirm('recover', progress.target_release, '', true)">恢复操作</Button></div></div>
        <div v-if="status.busy || progress.error || progress.blockers?.length" class="space-y-3 border-t px-5 py-4 text-sm"><p v-if="status.busy" class="text-muted-foreground">维护期间新工作暂缓，在途工作自然结束后继续。</p><ul v-if="progress.blockers?.length" class="space-y-1 rounded-lg bg-muted p-3"><li v-for="blocker in progress.blockers" :key="blocker" class="break-words">{{ blocker }}</li></ul><p v-if="progress.error" role="alert" class="text-amber-800">{{ progress.error }}</p></div>
        <details class="border-t px-5 py-3 text-xs text-muted-foreground"><summary class="cursor-pointer">操作详情与数据库快照</summary><div class="mt-3 space-y-2"><p class="break-all">目标发行：<code>{{ progress.target_release }}</code></p><p class="break-all">操作 ID：<code>{{ progress.operation_id }}</code></p><p :title="progress.updated_at">更新时间：{{ formatLocalTime(progress.updated_at) }}</p><p v-if="progress.database_snapshot">数据库快照{{ progress.database_snapshot.state === 'verified' ? '已校验' : '状态：' + progress.database_snapshot.state }} · {{ (progress.database_snapshot.bytes / 1024 / 1024).toFixed(1) }} MiB</p><p v-for="(migration, database) in progress.database_migrations ?? {}" :key="database">{{ database === 'bench' ? '平台数据库' : '网关数据库' }} · 目标版本 {{ migration.target_version }} · {{ migration.state === 'completed' ? '迁移完成' : migration.state === 'running' ? '正在迁移' : migration.state }}</p></div></details>
      </section>
      <section class="space-y-3" aria-label="可用版本">
        <div class="flex flex-wrap items-center justify-between gap-2"><h2 class="text-sm font-semibold">可用版本</h2><time class="text-xs text-muted-foreground" :title="catalog?.checked_at ?? undefined">最近检查：{{ catalog?.checked_at ? formatLocalTime(catalog.checked_at) : '等待首次检查' }}</time></div>
        <p v-if="catalog?.check_error" role="status" class="text-sm text-amber-800">{{ catalog.check_error }}</p>
        <article v-for="(release, index) in displayed" :key="release.release" class="overflow-hidden rounded-xl border bg-card" data-release-card>
          <div class="flex flex-wrap items-center gap-3 px-5 py-4"><button type="button" class="flex min-w-0 flex-1 items-start gap-3 text-left" :aria-expanded="expanded === release.release" :aria-controls="`release-detail-${release.release}`" @click="toggleDetails(release.release)"><component :is="expanded === release.release ? ChevronDown : ChevronRight" class="mt-1 size-4 shrink-0 text-muted-foreground" /><span class="min-w-0"><span class="flex flex-wrap items-center gap-2"><strong class="break-words text-sm font-semibold">{{ releaseLabel(release.release) }}</strong><span v-if="release.release === status.current_release" class="rounded-md bg-primary/10 px-2 py-0.5 text-[11px] text-primary">当前版本</span><span v-else-if="index === 0" class="rounded-md bg-muted px-2 py-0.5 text-[11px] text-muted-foreground">最新可用</span></span><span class="mt-1 block text-xs text-muted-foreground" :title="release.published_at">{{ formatLocalTime(release.published_at) }}</span><span class="mt-2 block truncate text-xs text-muted-foreground">{{ summary(release) }}</span></span></button><Button v-if="release.release !== status.current_release" size="sm" :disabled="status.busy || writing || catalog?.fresh === false || !release.data_compatible" @click="confirm('install', release.release, release.manifest_sha256, release.account_changed)"><Download class="size-3.5" />{{ catalog?.fresh === false ? '先检查更新' : release.data_compatible ? '安装更新' : '待验证兼容' }}</Button></div>
          <div v-if="expanded === release.release" :id="`release-detail-${release.release}`" class="space-y-3 border-t bg-muted/20 px-5 py-4 text-sm" data-release-details><p class="whitespace-pre-wrap break-words">{{ releaseNotes(release.changes) }}</p><p v-if="release.compatibility_reason" class="text-amber-800">{{ release.compatibility_reason === 'migration_evidence_missing' ? '此来源版本的升级与回退演练证据待发布端补齐。' : '此版本的数据兼容声明待发布端验证。' }}</p><p v-if="release.migrations?.length" class="text-xs text-muted-foreground">包含已验证的数据库迁移，安装前自动保存两库快照。</p><p v-for="migration in release.migrations ?? []" :key="migration.database" class="text-xs text-muted-foreground">{{ migration.database === 'bench' ? '平台数据库' : '网关数据库' }} → 版本 {{ migration.target_version }} · {{ migration.steps }} 项登记</p><p class="text-xs text-muted-foreground">{{ release.account_changed ? `预计重建 ${runningEnvironments} 个运行环境，已停止环境保留原状态。` : '兼容的账号运行环境继续复用。' }}</p><dl class="grid gap-1 border-t pt-3 text-xs"><dt class="text-muted-foreground">发行标识</dt><dd class="break-all font-mono">{{ release.release }}</dd><dt class="mt-2 text-muted-foreground">清单 SHA256</dt><dd class="break-all font-mono">{{ release.manifest_sha256 }}</dd></dl></div>
        </article>
        <p v-if="!catalog?.releases.length" class="rounded-xl border border-dashed p-8 text-center text-sm text-muted-foreground">{{ catalog?.checking ? '正在检查受信版本…' : '等待检查可用版本' }}</p>
        <button v-if="older.length || status.history?.length" type="button" class="flex items-center gap-2 py-2 text-xs text-muted-foreground hover:text-foreground" :aria-expanded="historyOpen" aria-controls="update-history" @click="historyOpen = !historyOpen; expanded = ''"><component :is="historyOpen ? ChevronDown : ChevronRight" class="size-4" />{{ historyOpen ? '收起历史版本' : `查看历史版本与回退（${older.length} 个其它发行）` }}</button>
        <section v-if="historyOpen" id="update-history" class="space-y-3 rounded-xl border bg-card p-5"><h3 class="text-sm font-medium">已安装版本与回退</h3><div v-for="item in status.history ?? []" :key="item.release" class="flex flex-wrap items-center justify-between gap-3 border-t pt-3"><div class="min-w-0"><p class="text-sm font-medium">{{ releaseLabel(item.release) }}</p><p class="mt-1 text-xs text-muted-foreground">{{ formatLocalTime(item.installed_at) }} · {{ item.rollback_available ? '满足当前数据回退条件' : '当前数据合同阻止回退' }}</p><code class="mt-1 block break-all text-[11px] text-muted-foreground">{{ item.release }}</code></div><Button variant="outline" size="sm" :disabled="status.busy || writing || !item.rollback_available" @click="confirm('rollback', item.release, item.manifest_sha256)">回退到此版本</Button></div><p v-if="!status.history?.length" class="text-xs text-muted-foreground">首次接入后，成功更新会保存可回退的历史组合。</p></section>
      </section>
    </template>
    <dialog ref="dialog" class="w-[calc(100%_-_2rem)] max-w-lg rounded-xl border bg-card p-0 text-foreground shadow-xl backdrop:bg-black/40" @cancel.prevent="closeConfirm" @click="event => { if (event.target === dialog) closeConfirm(); }"><div v-if="frozen" class="space-y-5 p-6"><h2 class="text-lg font-semibold">{{ frozen.action === 'recover' ? '恢复本次操作' : frozen.action === 'rollback' ? '确认版本回退' : '确认安装更新' }}</h2><p class="text-xl font-semibold">{{ frozen.label }}</p><code class="block break-all text-xs text-muted-foreground">{{ frozen.body.target_release }}</code><p class="text-sm text-muted-foreground">系统会暂停新接单，等待在途工作结束，再执行维护切换。</p><p v-if="frozen.action !== 'recover'" class="text-sm text-muted-foreground">{{ frozen.accountChanged ? '将按计划核对并更新受影响的账号环境。' : '兼容的账号容器继续复用。' }}</p><label v-if="frozen.action !== 'recover'" class="flex flex-wrap items-center gap-3 text-sm">等待在途完成<select v-model="minutes" :disabled="writing || sent" class="rounded-lg border bg-card px-3 py-2"><option v-for="value in [1, 5, 15, 30, 60, 120]" :key="value" :value="value">{{ value }} 分钟</option></select></label><p v-if="conflicted" role="alert" class="text-sm text-amber-800">现场版本已经变化，请关闭窗口并刷新后重新选择。</p><div class="flex justify-end gap-3"><Button variant="outline" :disabled="writing" @click="closeConfirm">取消</Button><Button :disabled="writing || conflicted" @click="execute">{{ writing ? '正在登记…' : frozen.action === 'recover' ? '开始恢复' : '确认执行' }}</Button></div></div></dialog>
  </section>
</template>
