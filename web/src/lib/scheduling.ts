/**
 * 将后端共享容量裁决转换为中文，不自行计算另一套准入规则。
 * @param reason 后端原因枚举。
 * @return 可展示标签，未知枚举不猜测为可用。
 */
export function schedulingReasonLabel(reason: string): string {
  const labels: Record<string, string> = {
    ready: '容量就绪',
    concurrency: '等待并发容量',
    pacing: '等待发送间隔',
    queue_full: '等待队列已满',
    wait_budget: '预计等待超过准入期限',
    pending_submissions: '前方请求准备或等待提交',
    account_unavailable: '状态或额度不可用',
  }
  return labels[reason] ?? '状态待更新'
}
