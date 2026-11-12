/**
 * 按浏览者时区展示持久化时间，缺失或非法值保持可见。
 * @param value 接口保存的带时区时间戳。
 * @return 本地日期时间及 IANA 时区，或未观测。
 */
export function formatLocalTime(value?: string | null): string {
  if (!value) return '未观测';
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return '未观测';
  const formatter = new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
  });
  return `${formatter.format(date)} · ${formatter.resolvedOptions().timeZone}`;
}
