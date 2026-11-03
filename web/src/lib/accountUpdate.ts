/**
 * 仅提交本次表单改动，避免未改动的凭据/代理触发维护锁或覆盖其他编辑。
 * @param current 当前账号表单，字段均为标量。
 * @param baseline 打开编辑弹窗时的表单快照。
 * @return 沿用管理接口空值语义的增量更新。
 */
export function accountUpdateDelta(current: Record<string, unknown>, baseline: Record<string, unknown>): Record<string, unknown> {
  const updates: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(current)) {
    if (value === baseline[key]) continue;
    if (['setup_token', 'access_token', 'refresh_token', 'name', 'email'].includes(key) && !String(value ?? '').trim()) continue;
    if (key === 'expires_at') {
      const text = String(value ?? '').trim();
      const previous = String(baseline[key] ?? '').trim();
      if (text === previous || (text && previous && Number(text) === Number(previous))) continue;
      updates[key] = text ? Number(text) : null;
    } else if (['account_uuid', 'organization_uuid', 'subscription_type', 'rate_limit_tier'].includes(key)) {
      updates[key] = value || null;
    } else {
      updates[key] = value;
    }
  }
  return updates;
}
